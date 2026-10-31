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
    VARIANTS_COL,
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
    """Run the pipeline with *n* rows per chunk (the default when ``None``)."""
    if n is None:
        yield
        return
    with mock.patch.object(pipeline, "CHUNK_ROWS", n):
        yield


def channel_rows(spark, series_by_key, time_type=T.LongType()) -> tuple[DataFrame, DataFrame]:
    """Channel data and the variants of each channel, as ``apply_pushdowns`` takes them.

    *series_by_key* maps ``(cid, ch, variant)`` to ``(tstarts, tends, values)``; all
    variants of a channel have the same samples, as in a solve.
    """
    by_channel: dict[tuple, tuple] = {}
    for (cid, ch, vid), samples in series_by_key.items():
        known, vids = by_channel.setdefault((cid, ch), (samples, []))
        assert known is samples or known == samples, f"variants of {(cid, ch)} differ"
        vids.append(vid)
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField("channel_id", T.LongType()),
            T.StructField("tstart", time_type),
            T.StructField("tend", time_type),
            T.StructField("value", T.DoubleType()),
        ]
    )
    cast = int if isinstance(time_type, T.IntegralType) else float

    def to_time(t):
        return None if t is None else cast(t)

    rows = [
        (cid, ch, to_time(ts), to_time(te), None if v is None else float(v))
        for (cid, ch), ((tstarts, tends, values), _) in by_channel.items()
        for ts, te, v in zip(tstarts, tends, values, strict=True)
    ]
    variants = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField("channel_id", T.LongType()),
            T.StructField(VARIANTS_COL, T.ArrayType(T.IntegerType())),
        ]
    )
    return spark.createDataFrame(rows, schema=schema), spark.createDataFrame(
        [(cid, ch, sorted(vids)) for (cid, ch), (_, vids) in by_channel.items()], variants
    )


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
    df, variants = channel_rows(spark, series_by_key, time_type)
    with chunk_rows(chunk_size):
        result = apply_pushdowns(
            df, variants, plan_for(pushdowns), CONFIG, include_raw=include_raw
        )
        expected = [*df.schema.fields, T.StructField(VARIANT_COL, T.IntegerType())]
        assert [(f.name, f.dataType) for f in result.schema] == [
            (f.name, f.dataType) for f in expected
        ]
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


def _selector_op(node, operand, operation: str) -> TimeSeriesSelector | None:
    """The continuous selector of ``selector.<operation>()``, or ``None``."""
    if (
        type(node) is not TimeSeriesOp
        or node.optype != "cls"
        or node.operation != operation
        or len(node.args) != 1
        or node.kwargs
    ):
        return None
    selector = operand(node.args[0])
    if not isinstance(selector, TimeSeriesSelector):
        return None
    return selector if selector.series_type is SeriesType.CONTINUOUS else None


class DiffRule(PushdownRule):
    """Test-only ``channel.diff()``: the value change within a block, 0 at block starts."""

    name = "diff"

    def match(self, node, operand):
        selector = _selector_op(node, operand, "diff")
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

    def match(self, node, operand):
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


def top_level_items(text: str) -> list[str]:
    """The comma-separated items of *text*, not splitting inside parentheses."""
    items, depth, start = [], 0, 0
    for i, char in enumerate(text):
        depth += {"(": 1, ")": -1}.get(char, 0)
        if char == "," and depth == 0:
            items.append(text[start:i].strip())
            start = i + 1
    return [*items, text[start:].strip()]
