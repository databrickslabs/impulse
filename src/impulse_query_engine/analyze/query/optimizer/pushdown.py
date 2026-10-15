"""Building blocks of Spark pushdown rules."""

from __future__ import annotations

import abc
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from impulse_query_engine.model.series.sample_series import SampleSeries

if TYPE_CHECKING:
    import pandas as pd

    from impulse_query_engine.analyze.query.solvers.default_solver import TimeSeriesCache
    from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig

    from .context import RunBreak, SeriesContext

#: Column tagging each channel-data row with the series variant it belongs to.
VARIANT_COL = "__impulse_variant"
#: Variant id of the unmodified (raw) channel data.
RAW_VARIANT = 0


@dataclass(frozen=True)
class PushdownContext:
    """Plan-time facts about the channel data a pushdown would run on.

    Parameters
    ----------
    tstart_type, tend_type : pyspark.sql.types.DataType
        Spark types of the channel-data ``tstart`` / ``tend`` columns.
    """

    tstart_type: T.DataType
    tend_type: T.DataType

    @staticmethod
    def from_schema(schema: T.StructType, config: SolverConfig) -> PushdownContext:
        """Build the context from the column-mapped channel-data schema."""
        return PushdownContext(
            tstart_type=schema[config.tstart_col].dataType,
            tend_type=schema[config.tend_col].dataType,
        )


@dataclass(frozen=True, eq=False)
class Pushdown:
    """A subtree that a :class:`PushdownRule` computes natively in Spark.

    Expression nodes overload ``==``, so pushdowns compare by identity; use
    :attr:`variant_key` to identify equivalent pushdowns.

    Parameters
    ----------
    rule : PushdownRule
        The rule that matched the subtree.
    selector : TimeSeriesSelector
        The physical channel selector the subtree reads.
    params : tuple of (str, Any)
        Canonical, hashable operation parameters (e.g. ``(("sample_rate", 1e6),)``).
    consumed : tuple of TimeSeriesSelector
        Every selector occurrence inside the subtree (its ``get_selectors()``).
    """

    rule: PushdownRule
    selector: TimeSeriesSelector
    params: tuple[tuple[str, Any], ...]
    consumed: tuple[TimeSeriesSelector, ...]

    @property
    def variant_key(self) -> tuple:
        """Identity of the derived series, independent of the input channel."""
        return (self.rule.name, self.params)

    def describe(self) -> str:
        """Human-readable ``rule(param=value, ...)`` label."""
        params = ", ".join(f"{k}={v}" for k, v in self.params)
        return f"{self.rule.name}({params})"


class PushdownRule(abc.ABC):
    """Recognizes a subtree and computes it in Spark ahead of the pandas UDF.

    Planning and decoding run in Python: :meth:`match` recognizes the subtree,
    :meth:`decode` turns one container/channel/variant slice of the derived rows
    back into the object the subtree evaluates to.  The result must equal
    evaluating the subtree in pandas on the same channel data.

    The Spark side is a set of hooks returning columns, evaluated by the shared
    pushdown pipeline (:mod:`.pipeline`) for the samples of the rule's variants,
    one series (channel and variant) at a time, with a :class:`SeriesContext`:

    1. :meth:`row_columns` per sample;
    2. :meth:`keep` selects the samples that go on;
    3. :meth:`kept_columns` per kept sample;
    4. :meth:`emit` the derived rows of a kept sample.

    Rules never handle DataFrames, so every rule shares the pipeline's reads and
    shuffles.  Derived rows have the channel-data shape
    ``(container_id, channel_id, variant, tstart, tend, value)``.
    """

    #: Unique rule name, part of every :attr:`Pushdown.variant_key`.
    name: ClassVar[str]
    #: Whether the rule needs its series in order (neighbours, runs, series
    #: facts).  Per-sample rules set ``False`` and skip the series scan.
    ordered: ClassVar[bool] = True

    @abc.abstractmethod
    def match(self, node: TimeSeriesExpression) -> Pushdown | None:
        """Return the pushdown for *node*, or ``None`` if the rule does not apply.

        Purely syntactic: it runs on the driver while planning and again inside
        the UDF to recognize the node, so it must not depend on data or schemas.
        """

    def supports(self, pushdown: Pushdown, context: PushdownContext) -> bool:
        """Whether *pushdown* can run on channel data described by *context*."""
        return True

    def result_type(self, pushdown: Pushdown) -> type:
        """Core-model class :meth:`decode` returns for *pushdown*."""
        return SampleSeries

    def runs(self) -> Mapping[str, RunBreak]:
        """Run definitions beyond the built-in continuous blocks, by name.

        Each maps ``(prev, cur)`` samples to whether ``cur`` starts a new run;
        :meth:`SeriesContext.run` then describes the sample's run.
        """
        return {}

    def row_columns(self, ctx: SeriesContext) -> Mapping[str, Column]:
        """Columns computed for every sample, in order; refer to them via ``ctx.col``."""
        return {}

    def keep(self, ctx: SeriesContext) -> Column:
        """Whether a sample can emit rows; the others are dropped early."""
        return F.lit(True)

    def kept_columns(self, ctx: SeriesContext) -> Mapping[str, Column]:
        """Columns computed for the kept samples, in order (``ctx.block_end`` available)."""
        return {}

    @abc.abstractmethod
    def emit(self, ctx: SeriesContext) -> Column:
        """The derived rows of a kept sample.

        Returns
        -------
        Column
            An array of :data:`~.context.ELEMENT_TYPE` structs built with
            :meth:`SeriesContext.element`; empty or null emits nothing.  Times
            left out keep the sample's own ``tstart`` / ``tend``.
        """

    def decode(self, rows: pd.DataFrame, pushdown: Pushdown, cache: TimeSeriesCache) -> Any:
        """Build the subtree's result from one container/channel/variant slice."""
        return cache.to_sample_series(rows, uses_alias=pushdown.selector.uses_alias)

    def emits_result_rows(self, pushdown: Pushdown) -> bool:
        """Whether *pushdown*'s derived rows are its result's samples as they are.

        Holds when the result is a :class:`SampleSeries` built by the default
        :meth:`decode`: the variant's rows, with values multiplied by the
        conversion factor for aliased selectors, are then exactly the result, so
        it can be emitted without the pandas evaluation.
        """
        return (
            type(self).decode is PushdownRule.decode and self.result_type(pushdown) is SampleSeries
        )
