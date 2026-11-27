"""Pushdown rule computing comparisons of a channel with a scalar natively in Spark."""

from __future__ import annotations

import math
import operator
from collections.abc import Mapping
from typing import TYPE_CHECKING

import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import Column, Window

from impulse_query_engine.analyze.metadata.time_series_expression import (
    SeriesType,
    TimeSeriesExpression,
    TimeSeriesOp,
    TimeSeriesSelector,
)
from impulse_query_engine.model.series.intervals import Intervals

from .context import RowsContext, SeriesContext, nan_to_null
from .pushdown import (
    VALUE_TYPES,
    Operand,
    Pushdown,
    PushdownContext,
    PushdownRule,
    real_scalar,
)
from .resample_rule import ResamplePushdownRule, kept_bucket

if TYPE_CHECKING:
    from impulse_query_engine.analyze.query.solvers.default_solver import TimeSeriesCache

    from .plan import PushedNode

_OP, _THRESHOLD, _ALIAS = "op", "threshold", "alias"
#: The comparisons by parameter name; ``ne`` treats NaN differently (see ``_holds``).
_OPS = {
    "gt": operator.gt,
    "ge": operator.ge,
    "lt": operator.lt,
    "le": operator.le,
    "eq": operator.eq,
    "ne": operator.ne,
}
_OP_NAMES = {fn: name for name, fn in _OPS.items()}
#: ``scalar op x`` as the same comparison with the series first.
_FLIPPED = {"gt": "lt", "ge": "le", "lt": "gt", "le": "ge", "eq": "eq", "ne": "ne"}


class ConditionPushdownRule(PushdownRule):
    """Pushes ``channel op scalar`` (``>``, ``>=``, ``<``, ``<=``, ``==``, ``!=``) down to Spark.

    The channel is a continuous physical or aliased selection, or its resample
    (``rpm.resample(1e6) > 4000``), which the rule then absorbs
    (:attr:`~.pushdown.Pushdown.absorbed`) and computes in the same pass.  The scalar
    may come first (``500 < rpm``).  The result is what pandas computes,
    ``Intervals(tstarts[holds], tends[holds], merge_overlaps=True)``: the samples
    where the comparison holds, merged where one starts at or before the previous
    one's ``tend``.  Comparisons follow numpy: NaN and null values satisfy only
    ``!=``.  Aliased selections compare unit-converted values.

    Spark computes the merged intervals in two passes:

    1. **Pieces**, in the ordered pass, chunk by chunk: runs of consecutive selected
       samples that pandas merges for certain.  A sample only joins the previous one
       of its chunk if both are selected, it has a positive duration and starts at or
       before the previous ``tend``, and the previous ``tstart`` is not null (null
       times sort first in Spark but last in the UDF's cache).  A piece is shipped as
       ``(tstart of its first sample, tend of its last sample)``.
    2. **Merge**, in the finishing pass over the pieces of a series: pandas merges
       consecutive samples pairwise, so merging consecutive pieces makes exactly the
       comparisons pandas makes between their samples, including its rules for a
       trailing zero-duration interval and for dropping empty intervals.

    On a resample, the samples are the resampled grid rows.  The resample keeps the
    last sample of each grid bucket, whose grid rows all hold its value and follow
    each other without a gap, so a piece is a run of consecutive selected buckets of
    a block within a chunk; a trailing zero-duration grid point is a piece of its
    own.  The comparison computes the resample's columns for its own variants, even
    when the channel also ships the resample itself.

    Exact unless two selected samples share a ``tstart``: Spark orders them by
    ``tend`` and value, the UDF's cache in their arbitrary arrival order, and pandas'
    merge depends on the order.  On a resample, the limits of
    :class:`~.resample_rule.ResamplePushdownRule` apply as well.
    """

    name = "condition"
    may_emit_nothing = True
    finishes = True

    def match(self, node: TimeSeriesExpression, operand: Operand) -> Pushdown | None:
        if (
            type(node) is not TimeSeriesOp
            or node.optype != "builtin"
            or node.kwargs
            or len(node.args) != 2
            or not callable(node.operation)
        ):
            return None
        op = _OP_NAMES.get(node.operation)
        if op is None:
            return None
        first, second = node.args
        if isinstance(first, TimeSeriesExpression) and not isinstance(
            second, TimeSeriesExpression
        ):
            series, other = first, second
        elif isinstance(second, TimeSeriesExpression) and not isinstance(
            first, TimeSeriesExpression
        ):
            series, other, op = second, first, _FLIPPED[op]
        else:
            return None
        threshold = real_scalar(other)
        if threshold is None:
            return None
        classified = operand(series)
        absorbed: tuple[Pushdown, ...] = ()
        if isinstance(classified, TimeSeriesSelector):
            selector = classified
        elif _is_absorbable(classified):
            selector, absorbed = classified.selector, (classified,)
        else:
            return None
        if selector.series_type is not SeriesType.CONTINUOUS:
            return None
        # One NaN object, so equal comparisons have equal variant keys.
        if math.isnan(threshold):
            threshold = math.nan
        return Pushdown(
            rule=self,
            selector=selector,
            # The alias flag is part of the variant key: the selector id ignores it.
            params=((_OP, op), (_THRESHOLD, threshold), (_ALIAS, bool(selector.uses_alias))),
            consumed=(selector,),
            absorbed=absorbed,
        )

    def supports(self, pushdown: Pushdown, context: PushdownContext) -> bool:
        return (
            context.numeric_times
            and isinstance(context.value_type, VALUE_TYPES)
            and all(inner.rule.supports(inner, context) for inner in pushdown.absorbed)
        )

    def result_type(self, pushdown: Pushdown) -> type:
        return Intervals

    def needs_factor(self, pushdown: Pushdown) -> bool:
        return dict(pushdown.params)[_ALIAS]

    # --- pass 1: pieces --------------------------------------------------------------

    def row_columns(self, ctx: SeriesContext) -> Mapping[str, Column]:
        plain, resampled = _inputs(ctx)
        value = ctx.converted_value
        # Shared: a resample bucket's grid rows hold the value of the sample it keeps.
        columns = {"selected": ctx.per_variant(lambda p: _holds(p, value))}
        if plain is not None:
            columns.update(_only(plain, resampled, _sample_pieces(plain)))
        if resampled is not None:
            resample = _resample_rule(resampled)
            columns.update(resampled.columns(resample.row_columns(resampled)))
            # The samples the resample keeps: its buckets (other kept rows are block
            # ends, which it keeps too, and the channel's presence row).
            columns.update(resampled.columns({"bucket": resample.keep(resampled)}))
        return columns

    def keep(self, ctx: SeriesContext) -> Column:
        plain, resampled = _inputs(ctx)
        # The last sample of each piece.
        sample_keep = ctx.col("selected") & ~ctx.col("next_joins")
        if resampled is None:
            return sample_keep
        # Every bucket, selected or not: the resample's grid checks run on all of them.
        bucket_keep = F.coalesce(resampled.col("bucket"), F.lit(False))
        if plain is None:
            return bucket_keep
        return F.when(plain.active, sample_keep).when(resampled.active, bucket_keep)

    def kept_columns(self, ctx: SeriesContext) -> Mapping[str, Column]:
        plain, resampled = _inputs(ctx)
        if resampled is None:
            return {}
        columns = resampled.columns(_resample_rule(resampled).kept_columns(resampled))
        bucket = kept_bucket(resampled)
        # lead is the previous kept sample: the order the pipeline already sorts them in.
        reverse = ctx.kept_in_chunk_reversed
        running = reverse.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        run, joins, next_joins = ctx.col("run"), ctx.col("joins_run"), ctx.col("next_joins_run")
        prev = F.lead(
            F.struct(run.alias("run"), ctx.block.id.alias("block"), bucket.stop.alias("stop"))
        ).over(reverse)
        columns.update(
            _only(
                resampled,
                plain,
                {
                    # A selected bucket with grid rows; the grid check comes first, so a
                    # block the resample cannot grid fails like pandas.
                    "run": F.coalesce(bucket.emits, F.lit(False))
                    & F.coalesce(resampled.col("bucket"), F.lit(False))
                    & ctx.col("selected"),
                    # It continues the previous bucket's grid rows, unless it is the
                    # trailing point, which stays a piece of its own.
                    "joins_run": run
                    & ~F.coalesce(bucket.only_point, F.lit(False))
                    & F.coalesce(
                        prev.getField("run")
                        & (prev.getField("block") == ctx.block.id)
                        & (prev.getField("stop") == bucket.first),
                        F.lit(False),
                    ),
                    "next_joins_run": F.coalesce(F.lag(joins).over(reverse), F.lit(False)),
                    # The end of the piece, from its last bucket.
                    "run_end": F.last(
                        F.when(
                            run & ~next_joins,
                            F.struct(
                                bucket.end.alias("end"),
                                F.coalesce(bucket.has_point, F.lit(False)).alias("has_point"),
                                bucket.point.alias("point"),
                            ),
                        ),
                        ignorenulls=True,
                    ).over(running),
                },
            )
        )
        return columns

    def emit(self, ctx: SeriesContext) -> Column:
        plain, resampled = _inputs(ctx)
        # The piece ends at its last sample's own tend.
        sample_piece = F.array(
            ctx.element(tstart=ctx.col("start").getField("t"), value=F.lit(None))
        )
        if resampled is None:
            return sample_piece
        bucket = kept_bucket(resampled)
        end = ctx.col("run_end")
        grid_piece = F.filter(
            F.array(
                F.when(
                    ~F.coalesce(bucket.only_point, F.lit(False)),
                    ctx.element(tstart=bucket.start, tend=end.getField("end"), value=F.lit(None)),
                ),
                F.when(
                    end.getField("has_point"),
                    ctx.element(
                        tstart=end.getField("point"), tend=end.getField("point"), value=F.lit(None)
                    ),
                ),
            ),
            lambda e: e.isNotNull(),
        )
        bucket_piece = (
            # A series of one sample is not resampled: a piece of that sample.
            F.when(
                ~bucket.resampled,
                F.when(ctx.col("selected"), F.array(ctx.element(value=F.lit(None)))),
            ).when(ctx.col("run") & ~ctx.col("joins_run"), grid_piece)
        )
        if plain is None:
            return bucket_piece
        return F.when(plain.active, sample_piece).when(resampled.active, bucket_piece)

    # --- pass 2: merge ---------------------------------------------------------------

    def finish_columns(self, ctx: RowsContext) -> Mapping[str, Column]:
        starts = ctx.col("starts")
        trailing_point = (
            ctx.is_last & _equal(ctx.tstart, ctx.tend) & _equal(ctx.prev.tend, ctx.tend)
        )
        return {
            # The piece starts a merged interval (pandas: tstarts[i] > tends[i - 1]); a
            # trailing zero-duration piece ending with the previous one stays apart.
            "starts": ctx.is_first | ~_at_most(ctx.tstart, ctx.prev.tend) | trailing_point,
            "start": F.last(
                F.when(starts, F.struct(ctx.tstart.alias("t"))), ignorenulls=True
            ).over(ctx.running),
            "ends": F.coalesce(F.lead(starts).over(ctx.series), F.lit(True)),
        }

    def finish_keep(self, ctx: RowsContext) -> Column:
        start = ctx.col("start").getField("t")
        # pandas drops empty intervals, the last one only if it ends before it starts.
        empty = F.when(ctx.is_last, _greater(start, ctx.tend)).otherwise(
            _greater_equal(start, ctx.tend)
        )
        return ctx.col("ends") & ~empty

    def finish_row(self, ctx: RowsContext) -> Column:
        return ctx.element(tstart=ctx.col("start").getField("t"), value=F.lit(None))

    # --- results ---------------------------------------------------------------------

    def decode(self, pushed: PushedNode, cache: TimeSeriesCache) -> Intervals:
        # The derived rows are the merged intervals.
        (selector,) = pushed.selectors
        rows = selector.build(cache, variant=pushed.variant)
        order = np.lexsort((rows.tends, rows.tstarts))
        return Intervals(rows.tstarts[order], rows.tends[order])

    def emits_result_rows(self, pushdown: Pushdown) -> bool:
        # Intervals, not samples.
        return False

    def has_native_value(self, pushdown: Pushdown) -> bool:
        return True

    def native_value(self, pushdown: Pushdown, rows: Column) -> Column:
        # Intervals.get_data(), with NaN as the UDF's Arrow output encodes it: null.
        return F.transform(
            rows, lambda r: F.array(nan_to_null(r["tstart"]), nan_to_null(r["tend"]))
        )


def _holds(pushdown: Pushdown, value: Column) -> Column:
    """Whether ``value op threshold`` holds as numpy evaluates it; never null."""
    params = dict(pushdown.params)
    op, threshold = params[_OP], params[_THRESHOLD]
    missing = value.isNull() | F.isnan(value)
    if op == "ne":
        return F.lit(True) if math.isnan(threshold) else missing | (value != F.lit(threshold))
    if math.isnan(threshold):
        return F.lit(False)
    compared = _OPS[op](value, F.lit(threshold))
    return ~missing & F.coalesce(compared, F.lit(False))


def _numpy(compared: Column, a: Column, b: Column) -> Column:
    """*compared* as numpy evaluates it on *a* and *b*: false with NaN or null; never null."""
    return F.coalesce(~F.isnan(a) & ~F.isnan(b) & compared, F.lit(False))


def _at_most(a: Column, b: Column) -> Column:
    return _numpy(a <= b, a, b)


def _greater(a: Column, b: Column) -> Column:
    return _numpy(a > b, a, b)


def _greater_equal(a: Column, b: Column) -> Column:
    return _numpy(a >= b, a, b)


def _equal(a: Column, b: Column) -> Column:
    return _numpy(a == b, a, b)


#: Scope of an absorbed resample's columns (see :meth:`SeriesContext.scoped`).
_RESAMPLE_SCOPE = "resample"


def _is_absorbable(classified) -> bool:
    """Whether the operand's pushdown is a resample the condition can compute in its pass."""
    return (
        isinstance(classified, Pushdown)
        and type(classified.rule) is ResamplePushdownRule
        and classified.source is None
        and not classified.absorbed
    )


def _inputs(ctx: SeriesContext) -> tuple[SeriesContext | None, SeriesContext | None]:
    """The rule's variants on samples, and those on an absorbed resample, as contexts.

    The first restricts *ctx* to the comparisons of samples; the second is scoped to
    the absorbed resamples of the others.  ``None`` when there are no such variants.
    """
    plain = [vid for vid, p in ctx.variants.items() if not p.absorbed]
    absorbed = {vid: p.absorbed[0] for vid, p in ctx.variants.items() if p.absorbed}
    return (
        ctx.restricted(plain) if plain else None,
        ctx.scoped(_RESAMPLE_SCOPE, _resample_rule_of(absorbed), absorbed) if absorbed else None,
    )


def _resample_rule_of(absorbed: Mapping[int, Pushdown]) -> ResamplePushdownRule:
    return next(iter(absorbed.values())).rule


def _resample_rule(scope: SeriesContext) -> ResamplePushdownRule:
    return _resample_rule_of(scope.variants)


def _only(ctx: SeriesContext, other: SeriesContext | None, columns: Mapping[str, Column]):
    """*columns* defined for *ctx*'s variants only, if the rule has *other* variants too."""
    if other is None:
        return dict(columns)
    return {name: F.when(ctx.active, column) for name, column in columns.items()}


def _sample_pieces(ctx: SeriesContext) -> dict[str, Column]:
    """The piece columns of comparisons of the channel's samples."""
    selected, joins = ctx.col("selected"), ctx.col("joins")
    running = ctx.in_chunk.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return {
        # The sample continues the piece of the previous sample in its chunk.
        "joins": F.coalesce(
            selected
            & ~ctx.first_in_chunk
            & _greater(ctx.tend, ctx.tstart)
            & F.coalesce(F.lag(selected).over(ctx.in_chunk), F.lit(False))
            & ctx.prev.tstart.isNotNull()
            & _at_most(ctx.tstart, ctx.prev.tend),
            F.lit(False),
        ),
        # The piece's first tstart, wrapped so a null tstart is not skipped.
        "start": F.when(~joins, F.struct(ctx.tstart.alias("t"))).otherwise(
            F.last(
                F.when(selected & ~joins, F.struct(ctx.tstart.alias("t"))), ignorenulls=True
            ).over(running)
        ),
        "next_joins": F.coalesce(F.lead(joins).over(ctx.in_chunk), F.lit(False)),
    }
