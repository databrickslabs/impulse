"""Building blocks of Spark pushdown rules."""

from __future__ import annotations

import abc
import re
from collections.abc import Callable, Mapping
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

_NAME = re.compile(r"[a-z][a-z0-9]*(_[a-z0-9]+)*")


def check_name(kind: str, name: str) -> str:
    """Return *name* if it is a valid rule, rule column or run name, else raise.

    Names are lowercase letters, digits and single underscores, starting with a
    letter.  Pipeline columns join them with ``__``, which a name never contains,
    so names of different rules cannot collide.
    """
    if not isinstance(name, str) or not _NAME.fullmatch(name):
        raise ValueError(
            f"{kind} name {name!r} must consist of lowercase letters, digits and single "
            "underscores, starting with a letter"
        )
    return name


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

    A pushdown reads the samples of its channel, possibly absorbing pushed
    operands into that pass, or computes on the result of another pushdown, its
    :attr:`source`.  The pipeline computes the former only.

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
    source : Pushdown, optional
        The pushdown whose result this one computes on; ``None`` when it reads
        the channel's samples.
    """

    rule: PushdownRule
    selector: TimeSeriesSelector
    params: tuple[tuple[str, Any], ...]
    consumed: tuple[TimeSeriesSelector, ...]
    source: Pushdown | None = None

    @property
    def variant_key(self) -> tuple:
        """Identity of the derived series, independent of the input channel."""
        key = (self.rule.name, self.params)
        return key if self.source is None else (*key, self.source.variant_key)

    def describe(self) -> str:
        """Human-readable ``rule(param=value, ...)`` label."""
        params = ", ".join(f"{k}={v}" for k, v in self.params)
        label = f"{self.rule.name}({params})"
        return label if self.source is None else f"{label} on {self.source.describe()}"


#: Classification of an operand of the node a rule matches: the selector of a
#: channel, the :class:`Pushdown` computing the operand, or ``None`` when the
#: operand is evaluated in pandas or is not an expression.
Operand = Callable[[Any], "TimeSeriesSelector | Pushdown | None"]


class PushdownRule(abc.ABC):
    """Recognizes a subtree and computes it in Spark ahead of the pandas UDF.

    Planning and decoding run in Python: :meth:`match` recognizes a node from
    its operands' classification, :meth:`decode` turns one
    container/channel/variant slice of the derived rows back into the object the
    subtree evaluates to.  The result must equal evaluating the subtree in pandas
    on the same channel data.

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

    #: Unique rule name, part of every :attr:`Pushdown.variant_key`; lowercase
    #: letters, digits and single underscores (see :func:`check_name`).
    name: ClassVar[str]
    #: Whether the rule needs its series in order (neighbours, runs, series
    #: facts).  Per-sample rules set ``False`` and skip the series scan.
    ordered: ClassVar[bool] = True

    @abc.abstractmethod
    def match(self, node: TimeSeriesExpression, operand: Operand) -> Pushdown | None:
        """Return the pushdown for *node*, or ``None`` if the rule does not apply.

        Nodes are classified bottom-up: *operand* returns the classification of
        any operand of *node*.  A pushdown reading the channel's samples sets no
        ``source``; when it absorbs a pushed operand into that pass, its
        ``consumed`` includes the operand's and :meth:`supports` covers both.  A
        pushdown computing on a pushed operand's result sets ``source`` to it.

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
        :meth:`SeriesContext.run` then describes the sample's run.  Names follow
        :func:`check_name` and only need to be unique within the rule.
        """
        return {}

    def row_columns(self, ctx: SeriesContext) -> Mapping[str, Column]:
        """Columns computed for every sample, in order; refer to them via ``ctx.col``.

        Column names follow :func:`check_name` and only need to be unique within
        the rule.
        """
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
