# pylint: disable=missing-function-docstring
"""The pushdown pipeline's services match a straightforward per-series reference.

Probe rules emit one service column per sample; the series are cut into chunks of
a few samples, so every service has to carry information across chunk boundaries.
"""

import re

import numpy as np
import numpy.testing as nptest
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from impulse_query_engine.analyze.query.optimizer import (
    VARIANT_COL,
    VARIANTS_COL,
    Classifier,
    Pushdown,
    SeriesPipeline,
    apply_pushdowns,
)
from impulse_query_engine.model.series.sample_series import SampleSeries
from tests.conftest import spark
from tests.impulse_query_engine.unit.analyze.query.optimizer.pipeline_helpers import (
    CONFIG,
    DiffRule,
    OffsetRule,
    ProbeRule,
    channel_rows,
    chunk_rows,
    plan_for,
    random_series,
    run,
)

_X = TimeSeriesSelector(TagSelector("channel_name") == "x")


def _same_value(prev, cur) -> bool:
    # Spark compares NaN as equal to NaN.
    return prev == cur or (np.isnan(prev) and np.isnan(cur))


def _value_change(prev, cur):
    return prev.value.isNull() | (prev.value != cur.value)


def _runs(breaks: list[bool], tstarts: list) -> dict[str, list]:
    ids = np.cumsum(breaks).tolist()
    starts, start = [], None
    for brk, ts in zip(breaks, tstarts, strict=True):
        start = ts if brk else start
        starts.append(start)
    return {
        "starts": [float(b) for b in breaks],
        "id": ids,
        "start": starts,
        "next_starts": [float(b) for b in breaks[1:]] + [None],
        "is_last": [float(i == ids[-1]) for i in ids],
    }


def _reference(tstarts, tends, values) -> dict[str, list]:
    """Every probed service for one series, computed sample by sample."""
    n = len(tstarts)
    block_breaks = [i == 0 or tstarts[i] - tends[i - 1] != 0 for i in range(n)]
    level_breaks = [i == 0 or not _same_value(values[i - 1], values[i]) for i in range(n)]
    block = _runs(block_breaks, tstarts)
    block_end = []
    for i in range(n):
        members = [tends[j] for j in range(n) if block["id"][j] == block["id"][i]]
        finite = [t for t in members if not np.isnan(t)]
        block_end.append(max(finite) if finite else None)
    return {
        "prev_tstart": [None, *tstarts[:-1]],
        "prev_tend": [None, *tends[:-1]],
        "prev_value": [None, *values[:-1]],
        "next_tstart": [*tstarts[1:], None],
        "next_value": [*values[1:], None],
        "n": [n] * n,
        "last_tstart": [tstarts[-1]] * n,
        "last_tend": [tends[-1]] * n,
        **{f"block_{name}": column for name, column in block.items()},
        "block_end": block_end,
        **{f"level_{name}": column for name, column in _runs(level_breaks, tstarts).items()},
    }


_PROBES = {
    "prev_tstart": lambda ctx: ctx.prev.tstart,
    "prev_tend": lambda ctx: ctx.prev.tend,
    "prev_value": lambda ctx: ctx.prev.value,
    "next_tstart": lambda ctx: ctx.next.tstart,
    "next_value": lambda ctx: ctx.next.value,
    "n": lambda ctx: ctx.n,
    "last_tstart": lambda ctx: ctx.last.tstart,
    "last_tend": lambda ctx: ctx.last.tend,
    "block_starts": lambda ctx: ctx.block.starts,
    "block_id": lambda ctx: ctx.block.id,
    "block_start": lambda ctx: ctx.block.start,
    "block_next_starts": lambda ctx: ctx.block.next_starts,
    "block_is_last": lambda ctx: ctx.block.is_last,
    "block_end": lambda ctx: ctx.block_end,
    "level_starts": lambda ctx: ctx.run("level").starts,
    "level_id": lambda ctx: ctx.run("level").id,
    "level_start": lambda ctx: ctx.run("level").start,
    "level_next_starts": lambda ctx: ctx.run("level").next_starts,
    "level_is_last": lambda ctx: ctx.run("level").is_last,
}


def _probe_pushdowns() -> dict[int, Pushdown]:
    probes = {}
    for vid, (name, probe) in enumerate(_PROBES.items(), start=1):
        runs = {"level": _value_change} if name.startswith("level_") else None
        probes[vid] = ProbeRule(f"probe_{name}", probe, runs).pushdown(_X)
    return probes


def _as_float(column: list) -> np.ndarray:
    return np.array([np.nan if v is None else float(v) for v in column])


@pytest.mark.parametrize(
    ("time_type", "nan_tend"), [(T.LongType(), False), (T.DoubleType(), True)]
)
@pytest.mark.parametrize("chunk_size", [1, 2, 3, 7, None])
def test_services_across_chunk_boundaries(spark, chunk_size, time_type, nan_tend):
    rng = np.random.default_rng(17)
    pushdowns = _probe_pushdowns()
    series, expected = {}, {}
    for cid, n in enumerate([1, 2, 5, 40, 90]):
        samples = random_series(rng, n, nan_tend=nan_tend)
        reference = _reference(*samples)
        for vid, pushdown in pushdowns.items():
            series[(cid, 1, vid)] = samples
            expected[(cid, 1, vid)] = (
                samples,
                reference[pushdown.rule.name.removeprefix("probe_")],
            )
    actual = run(spark, series, pushdowns, time_type=time_type, chunk_size=chunk_size)
    assert actual.keys() == expected.keys()
    for key, ((tstarts, tends, _), column) in expected.items():
        name = pushdowns[key[2]].rule.name
        nptest.assert_array_equal(actual[key].tstarts, tstarts, err_msg=name)
        nptest.assert_array_equal(actual[key].tends, tends, err_msg=name)
        nptest.assert_array_equal(actual[key].values, _as_float(column), err_msg=name)


def test_diff_matches_sample_series(spark):
    rng = np.random.default_rng(23)
    series, expected = {}, {}
    for cid, n in enumerate([1, 2, 30, 70]):
        samples = random_series(rng, n)
        series[(cid, 1, 1)] = samples
        expected[(cid, 1, 1)] = SampleSeries(*samples).diff()
    pushdowns = {1: Classifier([DiffRule()])(_X.diff())}
    for chunk_size in (1, 3, None):
        actual = run(spark, series, pushdowns, chunk_size=chunk_size)
        for key, exp in expected.items():
            nptest.assert_array_equal(actual[key].tstarts, exp.tstarts)
            nptest.assert_array_equal(actual[key].tends, exp.tends)
            nptest.assert_array_equal(actual[key].values, exp.values)


_OFFSET = OffsetRule()


def _offset(value: float) -> Pushdown:
    return Pushdown(rule=_OFFSET, selector=_X, params=(("offset", value),), consumed=(_X,))


def test_per_sample_rules_share_the_row_branch_with_raw_rows(spark):
    samples = ([0, 1, 5], [1, 2, 6], [1.0, 2.0, 4.0])
    series = {(1, 1, 0): samples, (1, 1, 1): samples, (1, 1, 2): samples, (2, 1, 3): samples}
    pushdowns = {1: _offset(10.0), 2: _offset(0.5), 3: Classifier([DiffRule()])(_X.diff())}
    actual = run(spark, series, pushdowns, chunk_size=2)
    nptest.assert_array_equal(actual[(1, 1, 0)].values, [1.0, 2.0, 4.0])
    nptest.assert_array_equal(actual[(1, 1, 1)].values, [11.0, 12.0, 14.0])
    nptest.assert_array_equal(actual[(1, 1, 2)].values, [1.5, 2.5, 4.5])
    nptest.assert_array_equal(actual[(2, 1, 3)].values, [0.0, 1.0, 0.0])
    nptest.assert_array_equal(actual[(1, 1, 1)].tstarts, [0, 1, 5])


def test_per_sample_rules_skip_the_series_scan(spark):
    df, variants = channel_rows(spark, {(1, 1, 1): ([0, 1], [1, 2], [1.0, 2.0])})
    result = apply_pushdowns(df, variants, plan_for({1: _offset(1.0)}), CONFIG, include_raw=False)
    plan = result._jdf.queryExecution().optimizedPlan().toString()
    assert "_pd_chunk" not in plan
    assert "Window" not in plan
    assert "Aggregate" not in plan


def test_raw_rows_only_when_asked(spark):
    samples = ([0, 1], [1, 2], [1.0, 2.0])
    series = {(1, 1, 0): samples, (1, 1, 1): samples}
    pushdowns = {1: Classifier([DiffRule()])(_X.diff())}
    assert set(run(spark, series, pushdowns)) == {(1, 1, 0), (1, 1, 1)}
    assert set(run(spark, series, pushdowns, include_raw=False)) == {(1, 1, 1)}


def test_ordered_services_are_refused_to_per_sample_rules(spark):
    class _Neighbourly(OffsetRule):
        name = "neighbourly"

        def emit(self, ctx):
            return F.array(ctx.element(value=ctx.prev.value))

    df, variants = channel_rows(spark, {(1, 1, 1): ([0], [1], [1.0])})
    pushdown = Pushdown(rule=_Neighbourly(), selector=_X, params=(), consumed=(_X,))
    with pytest.raises(TypeError, match="needs an ordered rule"):
        apply_pushdowns(df, variants, plan_for({1: pushdown}), CONFIG)


def test_block_end_is_only_available_to_kept_samples(spark):
    rule = ProbeRule("early_block_end", lambda ctx: ctx.value)
    rule.row_columns = lambda ctx: {"end": ctx.block_end}
    df, variants = channel_rows(spark, {(1, 1, 1): ([0], [1], [1.0])})
    with pytest.raises(TypeError, match="kept samples"):
        apply_pushdowns(df, variants, plan_for({1: rule.pushdown(_X)}), CONFIG)


def test_one_shuffle_of_the_samples(spark):
    rng = np.random.default_rng(3)
    series = {(cid, 1, 1): random_series(rng, 30) for cid in range(3)}
    df, variants = channel_rows(spark, series)
    plan = plan_for({1: Classifier([DiffRule()])(_X.diff())})
    result = apply_pushdowns(df, variants, plan, CONFIG, include_raw=False)
    result.collect()
    final = _final_plan(result)
    exchanges = _shuffles(final)
    key = "container_id, channel_id"
    samples = [e for e in exchanges if "_pd_chunk" in e]
    assert len(samples) == 1 and samples[0].startswith(f"hashpartitioning({key}, _pd_chunk")
    # The others are the per-channel boundaries and chunk summaries.
    assert all(e.startswith(f"hashpartitioning({key}") for e in exchanges)
    # One full read, shared by the chunk pass and its summaries, and the boundaries.
    assert len(_channel_data_scans(final)) == 2


def test_chunks_scale_with_the_variants_of_a_channel(spark):
    # Both variants of channel (1, 1) fan out per chunk, so it gets twice as many chunks
    # to keep the rows per chunk; each of its samples enters the chunk shuffle once.
    samples = (list(range(12)), list(range(1, 13)), [1.0] * 12)
    df, variants = channel_rows(
        spark, {(1, 1, 1): samples, (1, 1, 2): samples, (2, 1, 1): samples}
    )
    diff, probe = DiffRule(), ProbeRule("probe_value", lambda ctx: ctx.value)
    rules = [(diff, {1: Classifier([diff])(_X.diff())}), (probe, {2: probe.pushdown(_X)})]
    with chunk_rows(4):
        chunks = SeriesPipeline(rules, CONFIG)._chunked(df, variants)
        per_channel = chunks.groupBy("container_id", "channel_id").agg(
            F.countDistinct("_pd_chunk").alias("chunks"), F.count(F.lit(1)).alias("samples")
        )
        counts = {(r[0], r[1]): (r["chunks"], r["samples"]) for r in per_channel.collect()}
    assert counts == {(1, 1): (6, 12), (2, 1): (3, 12)}


def test_samples_are_shuffled_once_per_channel(spark):
    samples = random_series(np.random.default_rng(11), 40)
    # Channel (1, 1) ships two ordered variants, channel (2, 1) its raw rows.
    series = {(1, 1, 1): samples, (1, 1, 2): samples, (2, 1, 0): samples}
    df, variants = channel_rows(spark, series)
    probe = ProbeRule("probe_value", lambda ctx: ctx.value)
    plan = plan_for({1: Classifier([DiffRule()])(_X.diff()), 2: probe.pushdown(_X)})
    result = apply_pushdowns(df, variants, plan, CONFIG)
    result.collect()
    final = _final_plan(result)
    # The variants fan out after the chunk shuffle, which no further shuffle follows.
    assert all(VARIANT_COL not in e for e in _shuffles(final))
    lines = final.splitlines()
    shuffle = next(
        i for i, line in enumerate(lines) if re.search(r"- Exchange \w+\(.*_pd_chunk", line)
    )
    assert not any("Generate" in line for line in _subtree(lines, shuffle))
    # One broadcast of the variants for the boundaries and the fan-out, one for the raw rows.
    assert len([s for s in _scans(final) if VARIANTS_COL in s]) == 2


def test_raw_rows_are_joined_for_raw_channels_only(spark):
    # The row branch broadcasts only the channels shipping raw rows, so the pushed
    # channel's samples are dropped at the join instead of being exploded and filtered.
    samples = random_series(np.random.default_rng(13), 20)
    series = {(1, 1, 1): samples, (1, 1, 2): samples, (2, 1, 0): samples}
    df, variants = channel_rows(spark, series)
    plan = plan_for({1: Classifier([DiffRule()])(_X.diff()), 2: _offset(1.0)})
    result = apply_pushdowns(df, variants, plan, CONFIG)
    optimized = re.sub(r"#\d+L?", "", result._jdf.queryExecution().optimizedPlan().toString())
    shipping = r"size\(filter\(__impulse_variants, lambdafunction\(lambda \w+ IN \(0,2\).*\) > 0"
    assert re.search(shipping, optimized)


def _final_plan(df) -> str:
    """The final physical plan of *df*, which must have been collected."""
    return df._jdf.queryExecution().executedPlan().toString().split("== Initial Plan ==")[0]


def _shuffles(final: str) -> list[str]:
    """Partitionings of the shuffles (not broadcasts), each listed once even when reused."""
    return [re.sub(r"#\d+L?", "", e) for e in re.findall(r"(?<=- )Exchange (\w+\([^)]*\))", final)]


def _scans(final: str) -> list[str]:
    """Output columns of each in-memory scan in the plan."""
    return re.findall(r"Scan ExistingRDD\[([^\]]*)\]", final)


def _channel_data_scans(final: str) -> list[str]:
    """Scans of the channel data, not of the variants of each channel."""
    return [s for s in _scans(final) if VARIANTS_COL not in s]


def _subtree(lines: list[str], i: int) -> list[str]:
    """The plan lines below the node on line *i*."""

    def depth(line: str) -> int:
        return len(line) - len(line.lstrip(" :+-"))

    below = []
    for line in lines[i + 1 :]:
        if depth(line) <= depth(lines[i]):
            break
        below.append(line)
    return below


def _values_by_tend(df) -> dict:
    return {row["tend"]: row["value"] for row in df.collect()}


@pytest.mark.parametrize("chunk_size", [1, 2, None])
def test_null_and_nan_tstarts_keep_their_place_in_the_series(spark, chunk_size):
    # Spark orders a null tstart first and a NaN tstart last; their chunks must too.
    nan = float("nan")
    samples = ([None, 0.0, 1.0, 2.0, 3.0, 4.0, nan], [0.5, 1, 2, 3, 4, 5, 5.5], [1.0] * 7)
    df, variants = channel_rows(
        spark, {(1, 1, 1): samples, (1, 1, 2): samples}, time_type=T.DoubleType()
    )
    prev = ProbeRule("prev_start", lambda ctx: ctx.prev.tstart).pushdown(_X)
    nxt = ProbeRule("next_start", lambda ctx: ctx.next.tstart).pushdown(_X)
    with chunk_rows(chunk_size):
        result = apply_pushdowns(
            df, variants, plan_for({1: prev, 2: nxt}), CONFIG, include_raw=False
        )
        by_variant = {v: _values_by_tend(result.where(F.col(VARIANT_COL) == v)) for v in (1, 2)}
    expected_prev = {0.5: None, 1.0: None, 2.0: 0.0, 3.0: 1.0, 4.0: 2.0, 5.0: 3.0, 5.5: 4.0}
    expected_next = {0.5: 0.0, 1.0: 1.0, 2.0: 2.0, 3.0: 3.0, 4.0: 4.0, 5.0: nan, 5.5: None}
    for actual, expected in ((by_variant[1], expected_prev), (by_variant[2], expected_next)):
        assert actual.keys() == expected.keys()
        nptest.assert_array_equal(_as_float(actual.values()), _as_float(expected.values()))


def test_series_without_a_finite_tend_is_still_chunked(spark):
    nan = float("nan")
    samples = ([0.0, 1.0, 2.0, 3.0, 4.0, 5.0], [nan] * 6, [1.0] * 6)
    df, variants = channel_rows(spark, {(1, 1, 1): samples}, time_type=T.DoubleType())
    rule = DiffRule()
    with chunk_rows(2):
        pipeline = SeriesPipeline([(rule, {1: Classifier([rule])(_X.diff())})], CONFIG)
        chunks = pipeline._chunked(df, variants)
        assert chunks.select("_pd_chunk").distinct().count() == 3


def test_rule_names_cannot_collide_with_pipeline_columns(spark):
    # The rule column "ts" of a rule named "prev" once overwrote the pipeline's prev tstart.
    rule = ProbeRule("prev", lambda ctx: ctx.prev.tstart)
    rule.row_columns = lambda ctx: {"ts": ctx.value * 100}
    samples = ([0, 1, 2], [1, 2, 3], [1.0, 2.0, 3.0])
    actual = run(spark, {(1, 1, 1): samples}, {1: rule.pushdown(_X)}, chunk_size=1)
    nptest.assert_array_equal(actual[(1, 1, 1)].values, [np.nan, 0.0, 1.0])


@pytest.mark.parametrize("hook", ["row_columns", "runs"])
def test_invalid_rule_column_and_run_names_are_rejected(spark, hook):
    rule = ProbeRule("bad_names", lambda ctx: ctx.value)
    if hook == "row_columns":
        rule.row_columns = lambda ctx: {"Bad__name": ctx.value}
    else:
        rule._runs = {"Bad__name": _value_change}
    df, variants = channel_rows(spark, {(1, 1, 1): ([0], [1], [1.0])})
    with pytest.raises(ValueError, match="name 'Bad__name'"):
        apply_pushdowns(df, variants, plan_for({1: rule.pushdown(_X)}), CONFIG)
