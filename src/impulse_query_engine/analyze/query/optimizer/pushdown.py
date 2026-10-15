"""Building blocks of Spark pushdown rules."""

from __future__ import annotations

import abc
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

import pyspark.sql.types as T
from pyspark.sql import DataFrame

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from impulse_query_engine.model.series.sample_series import SampleSeries

if TYPE_CHECKING:
    import pandas as pd

    from impulse_query_engine.analyze.query.solvers.default_solver import TimeSeriesCache
    from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig

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

    A rule's derived rows use the channel-data shape
    ``(container_id, channel_id, variant, tstart, tend, value)``; :meth:`decode`
    turns one container/channel/variant slice of them back into the object the
    original subtree evaluates to.  The result must equal evaluating the subtree
    in pandas on the same channel data.
    """

    #: Unique rule name, part of every :attr:`Pushdown.variant_key`.
    name: ClassVar[str]

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

    @abc.abstractmethod
    def to_spark(
        self, rows: DataFrame, variants: Mapping[int, Pushdown], config: SolverConfig
    ) -> DataFrame:
        """Replace the rows of this rule's *variants* with their derived rows.

        *rows* is shared by all rules and partitioned by container; the result
        must keep that partitioning (windows partitioned by container id,
        channel id and :data:`VARIANT_COL` reuse it).  Implementations must not
        union *rows* or join it to a non-broadcast frame.  Small summaries
        derived from *rows* (per series or per chunk) may be broadcast-joined
        back; Spark reads the channel data again for them, so summaries should
        read only the key and time columns to share one narrow read.

        Parameters
        ----------
        rows : pyspark.sql.DataFrame
            All channel-data rows of the solve, tagged with :data:`VARIANT_COL`
            (one copy per variant a channel ships) and partitioned by container.
        variants : Mapping[int, Pushdown]
            This rule's variant ids, each with a representative pushdown carrying
            its parameters.
        config : SolverConfig
            Provides the internal column names.

        Returns
        -------
        pyspark.sql.DataFrame
            *rows* with the same columns and types, the rows of *variants*
            replaced by derived rows and all other rows unchanged.
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
