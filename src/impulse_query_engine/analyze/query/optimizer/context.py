"""What a pushdown rule sees of the samples it computes on.

A rule never handles DataFrames.  The pipeline (:mod:`.pipeline`) evaluates the
rule's hooks with a :class:`SeriesContext`, whose properties are Spark columns
describing the current sample of one of the rule's series: its times and value,
its neighbours, the run (e.g. continuous block) it belongs to, and facts about
the whole series.  Columns returned by a hook become part of the shared pass
over the channel data.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, Window, WindowSpec

from .pushdown import FACTOR_COL, VARIANT_COL

if TYPE_CHECKING:
    from .pushdown import Pushdown, PushdownRule

#: Element type of the arrays returned by :meth:`PushdownRule.emit`.
ELEMENT_TYPE = T.StructType(
    [
        T.StructField("tstart", T.DoubleType()),
        T.StructField("tend", T.DoubleType()),
        T.StructField("value", T.DoubleType()),
    ]
)

#: Name of the built-in run of continuous samples.
BLOCK = "block"

# Columns the pipeline provides, shared by all rules.
TS, TE = "_pd_ts", "_pd_te"
CHUNK = "_pd_chunk"
FIRST_IN_CHUNK, LAST_IN_CHUNK = "_pd_first_in_chunk", "_pd_last_in_chunk"
PREV_TS, PREV_TE, PREV_VALUE = "_pd_prev_ts", "_pd_prev_te", "_pd_prev_value"
NEXT_TS, NEXT_TE, NEXT_VALUE = "_pd_next_ts", "_pd_next_te", "_pd_next_value"
COUNT = "_pd_n"
LAST = "_pd_last"
BLOCK_END = "_pd_block_end"


def nan_to_null(col: Column) -> Column:
    """*col* with NaN as null, as ``numpy.nanmin`` / ``nanmax`` skip it and Arrow encodes it."""
    return F.when(~F.isnan(col), col)


def converted(value: Column, factor: Column, applies: Column) -> Column:
    """*value* times the unit-conversion *factor* where *applies*, as the UDF's cache converts.

    Like ``TimeSeriesCache.to_sample_series``: a null or NaN factor leaves the value as it is.
    """
    applies = applies & factor.isNotNull() & ~F.isnan(factor)
    return F.when(applies, value * factor).otherwise(value)


def element(
    tstart: Column | None = None, tend: Column | None = None, value: Column | None = None
) -> Column:
    """One emitted row; ``tstart`` / ``tend`` left out keep the row's own times."""

    def field(col: Column | None, name: str) -> Column:
        return (F.lit(None) if col is None else col).cast("double").alias(name)

    return F.struct(field(tstart, "tstart"), field(tend, "tend"), field(value, "value"))


def _per_variant(variants: Mapping[int, Pushdown], column: Callable[[Pushdown], Column]) -> Column:
    """``column(pushdown)`` for the row's variant: a CASE over *variants*."""
    result = None
    for vid, pushdown in sorted(variants.items()):
        branch = F.col(VARIANT_COL) == F.lit(vid)
        result = (
            F.when(branch, column(pushdown))
            if result is None
            else result.when(branch, column(pushdown))
        )
    return F.lit(None) if result is None else result


def run_col(run: str, name: str) -> str:
    """Name of the pipeline column *name* of the run *run*."""
    return f"_pd_run__{run}__{name}"


@dataclass(frozen=True)
class Sample:
    """One sample as columns: ``tstart`` and ``tend`` as double, and its value as double."""

    tstart: Column
    tend: Column
    value: Column


#: A run definition: whether the sample *cur* starts a new run after *prev*.  It
#: is called with ``prev`` columns that are null before the series' first
#: sample; a null result counts as a break.
RunBreak = Callable[[Sample, Sample], Column]


@dataclass(frozen=True)
class Run:
    """Where the current sample sits in its run, a maximal stretch without a break.

    Attributes
    ----------
    starts : Column
        The sample starts its run.
    id : Column
        Number of the run within the series, from 1.
    start : Column
        ``tstart`` of the run's first sample.
    next_starts : Column
        The next sample starts a new run; null for the series' last sample.
    is_last : Column
        The run is the last of its series.
    """

    starts: Column
    id: Column
    start: Column
    next_starts: Column
    is_last: Column


def block_break(prev: Sample, cur: Sample) -> Column:
    """Continuous blocks: a sample continues its block when it starts where the previous ended."""
    return prev.tend.isNull() | ((cur.tstart - prev.tend) != 0)


class SeriesContext:
    """Columns a rule's hooks build on, for the current sample of one of its series.

    Every series is one channel's samples of one variant, ordered by
    ``(tstart, tend, value)``; all variants of a channel have the same samples.
    Rules running in series order (``ordered``) see neighbours, runs and series
    facts; per-sample rules see only the sample.

    Parameters
    ----------
    rule : PushdownRule
        The rule the hooks belong to.
    variants : Mapping[int, Pushdown]
        The rule's variants in this solve.
    key : Sequence[str]
        Columns identifying a series.
    order : Sequence[str]
        The channel-data ``tstart``, ``tend`` and value columns: the series order.
    ordered : bool
        Whether the pipeline runs the rule in series order.
    runs : Mapping[str, str]
        The rule's run names to pipeline run names.
    kept : bool
        Whether the hook runs on the kept samples (``kept_columns``, ``emit``).
    has_factor : bool
        Whether the rows carry the channel's conversion factor (:data:`FACTOR_COL`).
    """

    def __init__(
        self,
        rule: PushdownRule,
        variants: Mapping[int, Pushdown],
        key: Sequence[str],
        order: Sequence[str],
        ordered: bool,
        runs: Mapping[str, str],
        kept: bool = False,
        has_factor: bool = False,
    ):
        self._rule = rule
        self._variants = variants
        self._key = tuple(key)
        self._order = tuple(order)
        self._ordered = ordered
        self._runs = dict(runs)
        self._kept = kept
        self._has_factor = has_factor

    def kept_stage(self) -> SeriesContext:
        """This context, for the hooks that run on the kept samples."""
        return SeriesContext(
            self._rule,
            self._variants,
            self._key,
            self._order,
            self._ordered,
            self._runs,
            kept=True,
            has_factor=self._has_factor,
        )

    # --- always available -------------------------------------------------------

    @property
    def active(self) -> Column:
        """The sample belongs to one of the rule's variants."""
        return F.col(VARIANT_COL).isin(sorted(self._variants))

    def name(self, column: str) -> str:
        """Pipeline name of the rule's column *column*."""
        return f"_pd_rule__{self._rule.name}__{column}"

    def col(self, column: str) -> Column:
        """The rule's column *column*, defined by an earlier hook entry."""
        return F.col(self.name(column))

    def param(self, name: str) -> Column:
        """The pushdown parameter *name* of the sample's variant."""
        return F.try_element_at(
            F.create_map(
                *[
                    item
                    for vid, pushdown in sorted(self._variants.items())
                    for item in (F.lit(vid), F.lit(dict(pushdown.params)[name]))
                ]
            ),
            F.col(VARIANT_COL),
        )

    def per_variant(self, column: Callable[[Pushdown], Column]) -> Column:
        """``column(pushdown)`` for the sample's variant: a CASE over the rule's variants."""
        return _per_variant(self._variants, column)

    @property
    def tstart(self) -> Column:
        """The sample's ``tstart`` as double."""
        return F.col(TS)

    @property
    def tend(self) -> Column:
        """The sample's ``tend`` as double."""
        return F.col(TE)

    @property
    def value(self) -> Column:
        """The sample's value as double."""
        return F.col(self._order[2]).cast("double")

    @property
    def converted_value(self) -> Column:
        """The sample's value as the UDF's cache loads it for the variant's selector.

        Multiplied by the channel's conversion factor for variants whose rule
        :meth:`~.pushdown.PushdownRule.needs_factor`, unless the factor is null or
        NaN; otherwise :attr:`value`.
        """
        value = self.value
        converting = sorted(vid for vid, p in self._variants.items() if self._rule.needs_factor(p))
        if not self._has_factor or not converting:
            return value
        return converted(
            value, F.col(FACTOR_COL).cast("double"), F.col(VARIANT_COL).isin(converting)
        )

    @property
    def sample(self) -> Sample:
        """The current sample."""
        return Sample(self.tstart, self.tend, self.value)

    element = staticmethod(element)

    # --- rules running in series order --------------------------------------------

    def _require_ordered(self, what: str) -> None:
        if not self._ordered:
            raise TypeError(f"{self._rule.name}: {what} needs an ordered rule")

    @property
    def prev(self) -> Sample:
        """The previous sample of the series; null columns for its first sample."""
        self._require_ordered("prev")
        return Sample(F.col(PREV_TS), F.col(PREV_TE), F.col(PREV_VALUE))

    @property
    def next(self) -> Sample:
        """The next sample of the series; null columns for its last sample."""
        self._require_ordered("next")
        return Sample(F.col(NEXT_TS), F.col(NEXT_TE), F.col(NEXT_VALUE))

    @property
    def n(self) -> Column:
        """Number of samples in the series."""
        self._require_ordered("n")
        return F.col(COUNT)

    @property
    def last(self) -> Sample:
        """The series' last sample."""
        self._require_ordered("last")
        last = F.col(LAST)
        return Sample(last.getField("tstart"), last.getField("tend"), last.getField("value"))

    @property
    def first_in_chunk(self) -> Column:
        """The sample is the first of its chunk."""
        self._require_ordered("first_in_chunk")
        return F.col(FIRST_IN_CHUNK)

    @property
    def in_chunk(self) -> WindowSpec:
        """Window over the sample's chunk in series order, for local neighbour access.

        Chunks are consecutive slices of a channel, the same for all its series;
        use :attr:`prev`, :attr:`next` and runs for anything that must see across
        chunk boundaries.
        """
        self._require_ordered("in_chunk")
        return Window.partitionBy(*self._key, CHUNK).orderBy(*self._order)

    @property
    def block(self) -> Run:
        """The continuous block the sample belongs to (gaps end blocks)."""
        self._require_ordered("block")
        return _run(BLOCK)

    def run(self, name: str) -> Run:
        """The sample's run of the rule's run definition *name* (see :meth:`PushdownRule.runs`)."""
        self._require_ordered("run")
        return _run(self._runs[name])

    @property
    def block_end(self) -> Column:
        """``nanmax`` of the ``tend`` values of the sample's block (kept samples only)."""
        self._require_ordered("block_end")
        if not self._kept:
            raise TypeError(f"{self._rule.name}: block_end is available to kept samples only")
        return F.col(BLOCK_END)


#: Columns of the finishing pass, shared by all finishing rules.
ROW_TS, ROW_TE, ROW_VALUE = "_pd_row_ts", "_pd_row_te", "_pd_row_value"
ROW_PREV_TS, ROW_PREV_TE, ROW_PREV_VALUE = (
    "_pd_row_prev_ts",
    "_pd_row_prev_te",
    "_pd_row_prev_value",
)
ROW_NEXT_TS, ROW_NEXT_TE, ROW_NEXT_VALUE = (
    "_pd_row_next_ts",
    "_pd_row_next_te",
    "_pd_row_next_value",
)
ROW_FIRST, ROW_LAST = "_pd_row_first", "_pd_row_last"


class RowsContext:
    """Columns a rule's finishing hooks build on, for one derived row of one of its series.

    The finishing pass (:func:`~.pipeline.finish_rows`) sees all derived rows of a
    series (one channel's rows of one variant in a container), ordered like the
    UDF's cache orders them: by ``tstart``, then ``tend``, NaN and null last.

    Parameters
    ----------
    rule : PushdownRule
        The rule the hooks belong to.
    variants : Mapping[int, Pushdown]
        The rule's variants in this solve.
    series : WindowSpec
        Window over the row's series in order.
    """

    def __init__(self, rule: PushdownRule, variants: Mapping[int, Pushdown], series: WindowSpec):
        self._rule = rule
        self._variants = variants
        self._series = series

    @property
    def active(self) -> Column:
        """The row belongs to one of the rule's variants."""
        return F.col(VARIANT_COL).isin(sorted(self._variants))

    def name(self, column: str) -> str:
        """Pipeline name of the rule's finishing column *column*."""
        return f"_pd_finish__{self._rule.name}__{column}"

    def col(self, column: str) -> Column:
        """The rule's finishing column *column*, defined by an earlier hook entry."""
        return F.col(self.name(column))

    def per_variant(self, column: Callable[[Pushdown], Column]) -> Column:
        """``column(pushdown)`` for the row's variant: a CASE over the rule's variants."""
        return _per_variant(self._variants, column)

    element = staticmethod(element)

    @property
    def tstart(self) -> Column:
        """The row's ``tstart`` as double."""
        return F.col(ROW_TS)

    @property
    def tend(self) -> Column:
        """The row's ``tend`` as double."""
        return F.col(ROW_TE)

    @property
    def value(self) -> Column:
        """The row's value as double."""
        return F.col(ROW_VALUE)

    @property
    def prev(self) -> Sample:
        """The previous row of the series; null columns for its first row."""
        return Sample(F.col(ROW_PREV_TS), F.col(ROW_PREV_TE), F.col(ROW_PREV_VALUE))

    @property
    def next(self) -> Sample:
        """The next row of the series; null columns for its last row."""
        return Sample(F.col(ROW_NEXT_TS), F.col(ROW_NEXT_TE), F.col(ROW_NEXT_VALUE))

    @property
    def is_first(self) -> Column:
        """The row is the first of its series."""
        return F.col(ROW_FIRST)

    @property
    def is_last(self) -> Column:
        """The row is the last of its series."""
        return F.col(ROW_LAST)

    @property
    def running(self) -> WindowSpec:
        """Window from the series' first row up to the current row."""
        return self._series.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    @property
    def series(self) -> WindowSpec:
        """Window over the row's series in order, for ``lag`` / ``lead``."""
        return self._series


def _run(run: str) -> Run:
    return Run(
        starts=F.col(run_col(run, "starts")),
        id=F.col(run_col(run, "id")),
        start=F.col(run_col(run, "start")),
        next_starts=F.col(run_col(run, "next_starts")),
        is_last=F.col(run_col(run, "is_last")),
    )
