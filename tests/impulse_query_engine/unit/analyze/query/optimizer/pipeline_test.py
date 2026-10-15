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
from impulse_query_engine.analyze.query.optimizer import VARIANT_COL, Pushdown, apply_pushdowns
from impulse_query_engine.model.series.sample_series import SampleSeries
from tests.conftest import spark
from tests.impulse_query_engine.unit.analyze.query.optimizer.pipeline_helpers import (
    CONFIG,
    DiffRule,
    OffsetRule,
    ProbeRule,
    channel_rows,
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
    pushdowns = {1: DiffRule().match(_X.diff())}
    for chunk_size in (1, 3, None):
        actual = run(spark, series, pushdowns, chunk_size=chunk_size)
        for key, exp in expected.items():
            nptest.assert_array_equal(actual[key].tstarts, exp.tstarts)
            nptest.assert_array_equal(actual[key].tends, exp.tends)
            nptest.assert_array_equal(actual[key].values, exp.values)


def _offset(value: float) -> Pushdown:
    rule = OffsetRule()
    return Pushdown(rule=rule, selector=_X, params=(("offset", value),), consumed=(_X,))


def test_per_sample_rules_share_the_row_branch_with_raw_rows(spark):
    samples = ([0, 1, 5], [1, 2, 6], [1.0, 2.0, 4.0])
    series = {(1, 1, 0): samples, (1, 1, 1): samples, (1, 1, 2): samples, (2, 1, 3): samples}
    pushdowns = {1: _offset(10.0), 2: _offset(0.5), 3: DiffRule().match(_X.diff())}
    actual = run(spark, series, pushdowns, chunk_size=2)
    nptest.assert_array_equal(actual[(1, 1, 0)].values, [1.0, 2.0, 4.0])
    nptest.assert_array_equal(actual[(1, 1, 1)].values, [11.0, 12.0, 14.0])
    nptest.assert_array_equal(actual[(1, 1, 2)].values, [1.5, 2.5, 4.5])
    nptest.assert_array_equal(actual[(2, 1, 3)].values, [0.0, 1.0, 0.0])
    nptest.assert_array_equal(actual[(1, 1, 1)].tstarts, [0, 1, 5])


def test_per_sample_rules_skip_the_series_scan(spark):
    df = channel_rows(spark, {(1, 1, 1): ([0, 1], [1, 2], [1.0, 2.0])})
    result = apply_pushdowns(df, plan_for({1: _offset(1.0)}), CONFIG, include_raw=False)
    plan = result._jdf.queryExecution().optimizedPlan().toString()
    assert "_pd_chunk" not in plan
    assert "Window" not in plan
    assert "Aggregate" not in plan


def test_raw_rows_only_when_asked(spark):
    samples = ([0, 1], [1, 2], [1.0, 2.0])
    series = {(1, 1, 0): samples, (1, 1, 1): samples}
    pushdowns = {1: DiffRule().match(_X.diff())}
    assert set(run(spark, series, pushdowns)) == {(1, 1, 0), (1, 1, 1)}
    assert set(run(spark, series, pushdowns, include_raw=False)) == {(1, 1, 1)}


def test_ordered_services_are_refused_to_per_sample_rules(spark):
    class _Neighbourly(OffsetRule):
        name = "neighbourly"

        def emit(self, ctx):
            return F.array(ctx.element(value=ctx.prev.value))

    df = channel_rows(spark, {(1, 1, 1): ([0], [1], [1.0])})
    pushdown = Pushdown(rule=_Neighbourly(), selector=_X, params=(), consumed=(_X,))
    with pytest.raises(TypeError, match="needs an ordered rule"):
        apply_pushdowns(df, plan_for({1: pushdown}), CONFIG)


def test_block_end_is_only_available_to_kept_samples(spark):
    rule = ProbeRule("early_block_end", lambda ctx: ctx.value)
    rule.row_columns = lambda ctx: {"end": ctx.block_end}
    df = channel_rows(spark, {(1, 1, 1): ([0], [1], [1.0])})
    with pytest.raises(TypeError, match="kept samples"):
        apply_pushdowns(df, plan_for({1: rule.pushdown(_X)}), CONFIG)


def test_one_shuffle_of_the_samples(spark):
    rng = np.random.default_rng(3)
    series = {(cid, 1, 1): random_series(rng, 30) for cid in range(3)}
    df = channel_rows(spark, series)
    plan = plan_for({1: DiffRule().match(_X.diff())})
    result = apply_pushdowns(df, plan, CONFIG, include_raw=False)
    result.collect()
    final = result._jdf.queryExecution().executedPlan().toString().split("== Initial Plan ==")[0]
    # Shuffles (not broadcasts), each listed once even when reused.
    exchanges = [
        re.sub(r"#\d+L?", "", e) for e in re.findall(r"(?<=- )Exchange (\w+\([^)]*\))", final)
    ]
    key = f"container_id, channel_id, {VARIANT_COL}"
    samples = [e for e in exchanges if "_pd_chunk" in e]
    assert len(samples) == 1 and samples[0].startswith(f"hashpartitioning({key}, _pd_chunk")
    # The others are the per-series boundaries and chunk summaries.
    assert all(e.startswith(f"hashpartitioning({key}") for e in exchanges)
    # One full read, shared by the chunk pass and its summaries, and the boundaries.
    assert len(re.findall(r"Scan ExistingRDD", final)) == 2
