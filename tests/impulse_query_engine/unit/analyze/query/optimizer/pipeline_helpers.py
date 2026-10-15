"""Helpers for pushdown pipeline tests: a runner and test-only rules."""

import contextlib
from collections.abc import Callable
from unittest import mock

import numpy as np
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Column, DataFrame

from impulse_query_engine.analyze.metadata.time_series_expression import (
    SeriesType,
    TimeSeriesOp,
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.optimizer import (
    VARIANT_COL,
    Pushdown,
    PushdownPlan,
    PushdownRule,
    SeriesContext,
    apply_pushdowns,
    pipeline,
)
from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig
from impulse_query_engine.model.series.sample_series import SampleSeries

CONFIG = SolverConfig()


@contextlib.contextmanager
def chunk_rows(n: int | None):
    """Run the pipeline with *n* samples per chunk (the default when ``None``)."""
    if n is None:
        yield
        return
    with mock.patch.object(pipeline, "CHUNK_ROWS", n):
        yield


def channel_rows(spark, series_by_key, time_type=T.LongType()) -> DataFrame:
    """Tagged channel data for ``{(cid, ch, variant): (tstarts, tends, values)}``."""
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField("channel_id", T.LongType()),
            T.StructField("tstart", time_type),
            T.StructField("tend", time_type),
            T.StructField("value", T.DoubleType()),
            T.StructField(VARIANT_COL, T.IntegerType()),
        ]
    )
    to_time = int if isinstance(time_type, T.IntegralType) else float
    rows = [
        (cid, ch, to_time(ts), to_time(te), None if v is None else float(v), vid)
        for (cid, ch, vid), (tstarts, tends, values) in series_by_key.items()
        for ts, te, v in zip(tstarts, tends, values, strict=True)
    ]
    return spark.createDataFrame(rows, schema=schema)


def plan_for(pushdowns: dict[int, Pushdown]) -> PushdownPlan:
    """A plan computing *pushdowns* as the given variants."""
    rules = tuple(dict.fromkeys(p.rule for p in pushdowns.values()))
    return PushdownPlan(
        rules=rules,
        variant_ids={p.variant_key: vid for vid, p in pushdowns.items()},
        pushdowns_by_variant=dict(pushdowns),
    )


def run(
    spark,
    series_by_key,
    pushdowns: dict[int, Pushdown],
    time_type=T.LongType(),
    chunk_size: int | None = None,
    include_raw: bool = True,
) -> dict[tuple, SampleSeries]:
    """Run *pushdowns* on the series; ``{(cid, ch, variant): SampleSeries}`` like the cache."""
    df = channel_rows(spark, series_by_key, time_type)
    with chunk_rows(chunk_size):
        result = apply_pushdowns(df, plan_for(pushdowns), CONFIG, include_raw=include_raw)
        assert result.schema == df.schema
        pdf = result.toPandas()
    out = {}
    for key, group in pdf.groupby(["container_id", "channel_id", VARIANT_COL]):
        group = group.sort_values(["tstart", "tend"], kind="stable")
        out[tuple(int(k) for k in key)] = SampleSeries(
            group["tstart"], group["tend"], group["value"].astype(np.float64)
        )
    return out


def random_series(rng, n, nan_tend=False):
    """Samples with gaps, zero-duration samples and NaN values, in series order.

    With *nan_tend*, some samples have a NaN ``tend`` (use a double time column).
    """
    tstarts, tends, values = [], [], []
    t, after_point = 0, False
    for _ in range(n):
        if rng.random() < 0.15:
            t += int(rng.integers(1, 6))
        duration = 0 if not after_point and rng.random() < 0.1 else int(rng.integers(1, 5))
        tstarts.append(t)
        nan = nan_tend and duration > 0 and rng.random() < 0.1
        tends.append(np.nan if nan else t + duration)
        values.append(np.nan if rng.random() < 0.1 else float(rng.integers(0, 4)))
        t, after_point = t + duration, duration == 0
    return tstarts, tends, values


def _selector_op(node, operation: str) -> TimeSeriesSelector | None:
    """The continuous selector of ``selector.<operation>()``, or ``None``."""
    if (
        type(node) is not TimeSeriesOp
        or node.optype != "cls"
        or node.operation != operation
        or len(node.args) != 1
        or node.kwargs
        or not isinstance(node.args[0], TimeSeriesSelector)
        or node.args[0].series_type is not SeriesType.CONTINUOUS
    ):
        return None
    return node.args[0]


class DiffRule(PushdownRule):
    """Test-only ``channel.diff()``: the value change within a block, 0 at block starts."""

    name = "diff"

    def match(self, node):
        selector = _selector_op(node, "diff")
        if selector is None:
            return None
        return Pushdown(rule=self, selector=selector, params=(), consumed=(selector,))

    def row_columns(self, ctx: SeriesContext):
        return {"diff": F.when(ctx.block.starts, F.lit(0.0)).otherwise(ctx.value - ctx.prev.value)}

    def emit(self, ctx: SeriesContext) -> Column:
        return F.array(ctx.element(value=ctx.col("diff")))


class OffsetRule(PushdownRule):
    """Test-only per-sample rule adding its ``offset`` parameter to every value."""

    name = "offset"
    ordered = False

    def match(self, node):
        return None

    def emit(self, ctx: SeriesContext) -> Column:
        return F.array(ctx.element(value=ctx.value + ctx.param("offset")))


class ProbeRule(PushdownRule):
    """Test-only rule emitting every sample with ``probe(ctx)`` as its value."""

    match = OffsetRule.match

    def __init__(
        self,
        name: str,
        probe: Callable[[SeriesContext], Column],
        runs: dict | None = None,
    ):
        self.name = name
        self.probe = probe
        self._runs = runs or {}

    def runs(self):
        return self._runs

    def emit(self, ctx: SeriesContext) -> Column:
        return F.array(ctx.element(value=self.probe(ctx)))

    def pushdown(self, selector: TimeSeriesSelector) -> Pushdown:
        return Pushdown(rule=self, selector=selector, params=(), consumed=(selector,))
