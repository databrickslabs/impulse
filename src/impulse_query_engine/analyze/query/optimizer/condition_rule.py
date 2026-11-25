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

    The channel is a continuous physical or aliased selection; the scalar may come
    first (``500 < rpm``).  The result is what pandas computes,
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

    Exact unless two samples of different pieces share a ``tstart``: the UDF's
    cache orders those arbitrarily, and the pandas result depends on the order.
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
        selector = operand(series)
        if threshold is None or not isinstance(selector, TimeSeriesSelector):
            return None
        if selector.series_type is not SeriesType.CONTINUOUS:
            return None
        # One NaN object, so equal comparisons have equal variant keys.
        if threshold is not None and math.isnan(threshold):
            threshold = math.nan
        return Pushdown(
            rule=self,
            selector=selector,
            # The alias flag is part of the variant key: the selector id ignores it.
            params=((_OP, op), (_THRESHOLD, threshold), (_ALIAS, bool(selector.uses_alias))),
            consumed=(selector,),
        )

    def supports(self, pushdown: Pushdown, context: PushdownContext) -> bool:
        return context.numeric_times and isinstance(context.value_type, VALUE_TYPES)

    def result_type(self, pushdown: Pushdown) -> type:
        return Intervals

    def needs_factor(self, pushdown: Pushdown) -> bool:
        return dict(pushdown.params)[_ALIAS]

    # --- pass 1: pieces --------------------------------------------------------------

    def row_columns(self, ctx: SeriesContext) -> Mapping[str, Column]:
        selected, joins = ctx.col("selected"), ctx.col("joins")
        value = ctx.converted_value
        running = ctx.in_chunk.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        return {
            "selected": ctx.per_variant(lambda p: _holds(p, value)),
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

    def keep(self, ctx: SeriesContext) -> Column:
        # The last sample of each piece.
        return ctx.col("selected") & ~ctx.col("next_joins")

    def emit(self, ctx: SeriesContext) -> Column:
        # The piece ends at its last sample's own tend.
        return F.array(ctx.element(tstart=ctx.col("start").getField("t"), value=F.lit(None)))

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
