# pylint: disable=missing-function-docstring
"""Parity of the Spark ``resample`` pushdown with ``SampleSeries.resample``."""

import re

import numpy as np
import numpy.testing as nptest
import pyspark.sql.types as T
import pytest

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from impulse_query_engine.analyze.query.optimizer import VARIANT_COL, ResamplePushdownRule
from impulse_query_engine.model.series.sample_series import SampleSeries
from tests.conftest import spark
from tests.impulse_query_engine.unit.analyze.query.optimizer.pipeline_helpers import (
    CONFIG,
    DiffRule,
    channel_rows,
    plan_for,
    random_series,
    run,
)
from impulse_query_engine.analyze.query.optimizer import apply_pushdowns

_RULE = ResamplePushdownRule()
_X = TimeSeriesSelector(TagSelector("channel_name") == "x")


def _pushdown(rate):
    return _RULE.match(_X.resample(rate))


def _resample_in_spark(spark, series_by_key, rates, time_type=T.LongType(), chunk_rows=None):
    """Resample ``{(cid, ch, variant): (tstarts, tends, values)}`` at ``{variant: rate}``.

    Variant 0 rows are raw rows.  Returns ``{(cid, ch, variant): SampleSeries}``
    built like the pandas cache does.
    """
    pushdowns = {vid: _pushdown(rate) for vid, rate in rates.items()}
    return run(spark, series_by_key, pushdowns, time_type=time_type, chunk_size=chunk_rows)


def _expected(series, rate):
    tstarts, tends, values = series
    return SampleSeries(tstarts, tends, [np.nan if v is None else v for v in values]).resample(
        rate
    )


def _assert_same(actual, expected):
    nptest.assert_array_equal(actual.tstarts, expected.tstarts)
    nptest.assert_array_equal(actual.tends, expected.tends)
    nptest.assert_array_equal(actual.values, expected.values)


def _where_lt(tstarts, tends, values, threshold):
    s = SampleSeries(tstarts, tends, values)
    w = s.where(s < threshold)
    return list(w.tstarts), list(w.tends), list(w.values)


# Inputs of the SampleSeries.resample unit tests plus further edge cases, as
# (tstarts, tends, values, rate). Integral, so exact for LongType columns.
_CASES = {
    "two_samples": ([0, 1], [1, 3], [1, 2], 1),
    "long_first_sample": ([0, 4, 5], [4, 5, 6], [1, 3, 2], 1),
    "uneven": ([0, 4, 6], [4, 6, 7], [1, 3, 2], 1),
    "gap": ([0, 1, 6], [1, 3, 7], [1, 2, 4], 1),
    "where_gap": (*_where_lt([0, 1, 2, 3, 4, 5], [1, 2, 3, 4, 5, 10], [5, 5, 5, 4, 5, 1], 5), 1),
    "where_gap_rate_2": (
        *_where_lt([0, 1, 2, 3, 4, 5, 10], [1, 2, 3, 4, 5, 10, 11], [5, 5, 5, 4, 5, 1, 2], 5),
        2,
    ),
    "trailing_point": ([0, 4, 5, 6], [4, 5, 6, 6], [1, 3, 2, 3], 1),
    "where_trailing_point": (
        *_where_lt([0, 1, 2, 3, 4, 5, 10], [1, 2, 3, 4, 5, 10, 10], [5, 5, 5, 4, 5, 1, 2], 6),
        1,
    ),
    "interior_point": ([0, 2, 3], [1, 2, 4], [10, 20, 30], 1),
    "interior_and_trailing_point": ([0, 2, 3], [1, 2, 3], [10, 20, 30], 1),
    "isolated_trailing_point": ([0, 1, 5], [1, 2, 5], [1, 2, 3], 1),
    "single_sample": ([5], [7], [1.0], 1),
    "single_point": ([5], [5], [1.0], 1),
    "nan_and_null_values": ([0, 1, 2, 3], [1, 2, 3, 4], [1.0, np.nan, None, 4.0], 1),
    "grid_off_sample_boundaries": ([0, 3, 7, 12], [3, 7, 12, 20], [1, 2, 3, 4], 4),
    "rate_beyond_block": ([0, 1, 2], [1, 2, 3], [1, 2, 3], 10),
    "grid_on_sample_starts": (list(range(10)), list(range(1, 11)), list(range(10)), 3),
}


@pytest.mark.parametrize("name", list(_CASES))
def test_resample_matches_sample_series(spark, name):
    tstarts, tends, values, rate = _CASES[name]
    actual = _resample_in_spark(spark, {(1, 1, 1): (tstarts, tends, values)}, {1: rate})
    _assert_same(actual[(1, 1, 1)], _expected((tstarts, tends, values), rate))


# Few samples per chunk put chunk boundaries at gaps, inside buckets and next to
# zero-duration samples, with empty chunks in between.
@pytest.mark.parametrize("chunk_rows", [None, 1, 2, 3])
def test_all_cases_in_one_pass_and_raw_rows_unchanged(spark, chunk_rows):
    series = {}
    expected = {}
    for i, (tstarts, tends, values, _rate) in enumerate(_CASES.values()):
        # rate 1 -> variant 1, rate 2 -> variant 2; containers keep channels apart.
        for vid, r in ((1, 1), (2, 2)):
            series[(i, 7, vid)] = (tstarts, tends, values)
            expected[(i, 7, vid)] = _expected((tstarts, tends, values), r)
        # Raw rows (variant 0) pass through untouched.
        series[(i, 7, 0)] = (tstarts, tends, values)
        expected[(i, 7, 0)] = SampleSeries(
            tstarts, tends, [np.nan if v is None else v for v in values]
        )
    actual = _resample_in_spark(spark, series, {1: 1, 2: 2}, chunk_rows=chunk_rows)
    assert actual.keys() == expected.keys()
    for key, exp in expected.items():
        _assert_same(actual[key], exp)


@pytest.mark.parametrize("chunk_rows", [None, 7])
def test_ns_epoch_longs_above_2_pow_53(spark, chunk_rows):
    rng = np.random.default_rng(7)
    base = 1_700_000_000_123_456_789
    steps = rng.integers(9_000_000, 11_000_000, size=500)  # ~10 ms, jittered
    times = base + np.concatenate([[0], np.cumsum(steps)])
    tstarts, tends = times[:-1].tolist(), times[1:].tolist()
    values = rng.normal(size=500).tolist()
    rate = 100_000_000  # 100 ms
    actual = _resample_in_spark(
        spark, {(1, 1, 1): (tstarts, tends, values)}, {1: rate}, chunk_rows=chunk_rows
    )
    expected = _expected((tstarts, tends, values), rate)
    assert len(expected) < len(tstarts) / 5
    _assert_same(actual[(1, 1, 1)], expected)


def test_only_linear_window_frames(spark):
    # Spark recomputes a ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING frame for every
    # row (quadratic in the series length), so the rule must not use one.
    plan = _optimized_plan(spark)
    assert "specifiedwindowframe(RowFrame, currentrow$(), unboundedfollowing$())" not in plan
    assert "specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())" in plan


def test_random_series_across_chunk_sizes(spark):
    rng = np.random.default_rng(3)
    rates = {1: 1, 2: 3, 3: 7}
    series, expected = {}, {}
    for cid, n in enumerate([1, 2, 40, 60, 60, 80]):
        tstarts, tends, values = random_series(rng, n)
        for vid, rate in rates.items():
            series[(cid, 1, vid)] = (tstarts, tends, values)
            expected[(cid, 1, vid)] = _expected((tstarts, tends, values), rate)
        series[(cid, 1, 0)] = (tstarts, tends, values)
        expected[(cid, 1, 0)] = SampleSeries(tstarts, tends, values)
    assert sum(len(e) for e in expected.values()) > 0
    for chunk_rows in (1, 2, 3, 5, 8, 13):
        actual = _resample_in_spark(spark, series, rates, chunk_rows=chunk_rows)
        assert actual.keys() == expected.keys()
        for key, exp in expected.items():
            _assert_same(actual[key], exp)


def _top_level_items(text):
    items, depth, start = [], 0, 0
    for i, char in enumerate(text):
        depth += {"(": 1, ")": -1}.get(char, 0)
        if char == "," and depth == 0:
            items.append(text[start:i].strip())
            start = i + 1
    return [*items, text[start:].strip()]


def _optimized_plan(spark) -> str:
    df = channel_rows(spark, {(1, 1, 1): ([0, 1], [1, 2], [1.0, 2.0])})
    result = apply_pushdowns(df, plan_for({1: _pushdown(1)}), CONFIG, include_raw=False)
    return result._jdf.queryExecution().optimizedPlan().toString()


def test_windows_over_samples_are_chunked(spark):
    # Spark buffers a whole window partition, so no window over the samples may be
    # partitioned by the series alone; only the chunk summaries are.
    plan = _optimized_plan(spark)
    specs = re.findall(r"windowspecdefinition\((.*?), specifiedwindowframe", plan)
    series_key = ["container_id", "channel_id", VARIANT_COL]
    seen = set()
    for spec in specs:
        items = _top_level_items(re.sub(r"#\d+L?", "", spec))
        partition = [item for item in items if not item.endswith(("NULLS FIRST", "NULLS LAST"))]
        order = [item.split(" ")[0] for item in items[len(partition) :]]
        seen.add((tuple(partition), tuple(order)))
        if partition == series_key:
            assert order == ["_pd_chunk"]
        else:
            assert partition == [*series_key, "_pd_chunk"]
    assert any(order == ("tstart", "tend", "value") for _, order in seen)
    assert any(partition == tuple(series_key) for partition, _ in seen)
    # Photon runs window aggregates over structs row-based.
    assert not re.search(r"(max|min)\(struct\(", plan)


def test_resample_and_diff_in_one_pass(spark):
    rng = np.random.default_rng(5)
    series, expected = {}, {}
    for cid in range(3):
        samples = random_series(rng, 50)
        series[(cid, 1, 1)] = series[(cid, 1, 2)] = samples
        expected[(cid, 1, 1)] = _expected(samples, 3)
        expected[(cid, 1, 2)] = SampleSeries(*samples).diff()
    pushdowns = {1: _pushdown(3), 2: DiffRule().match(_X.diff())}
    for chunk_size in (2, 7):
        actual = run(spark, series, pushdowns, chunk_size=chunk_size)
        assert actual.keys() == expected.keys()
        for key, exp in expected.items():
            _assert_same(actual[key], exp)


@pytest.mark.parametrize("chunk_rows", [None, 7])
def test_ns_epoch_grid_on_sample_starts(spark, chunk_rows):
    # Every fifth sample starts exactly on a grid point, the case where the bucket
    # index estimate needs its correction step.
    base = 1_700_000_000_123_456_789
    times = [base + i * 10_000_000 for i in range(301)]
    tstarts, tends = times[:-1], times[1:]
    values = [float(i) for i in range(300)]
    rate = 50_000_000
    actual = _resample_in_spark(
        spark, {(1, 1, 1): (tstarts, tends, values)}, {1: rate}, chunk_rows=chunk_rows
    )
    _assert_same(actual[(1, 1, 1)], _expected((tstarts, tends, values), rate))


@pytest.mark.parametrize("chunk_rows", [None, 2])
def test_integer_time_columns(spark, chunk_rows):
    tstarts, tends, values, rate = _CASES["where_gap_rate_2"]
    actual = _resample_in_spark(
        spark,
        {(1, 1, 1): (tstarts, tends, values)},
        {1: rate},
        time_type=T.IntegerType(),
        chunk_rows=chunk_rows,
    )
    _assert_same(actual[(1, 1, 1)], _expected((tstarts, tends, values), rate))


@pytest.mark.parametrize("chunk_rows", [None, 2])
def test_seconds_as_double_with_binary_exact_rate(spark, chunk_rows):
    tstarts = [10.0, 10.25, 11.0, 12.5, 14.0]
    tends = [10.25, 11.0, 12.5, 14.0, 14.0]
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    actual = _resample_in_spark(
        spark,
        {(1, 1, 1): (tstarts, tends, values)},
        {1: 0.75},
        time_type=T.DoubleType(),
        chunk_rows=chunk_rows,
    )
    _assert_same(actual[(1, 1, 1)], _expected((tstarts, tends, values), 0.75))


@pytest.mark.parametrize("chunk_rows", [None, 7])
def test_seconds_as_double_with_fractional_rate(spark, chunk_rows):
    # 0.1 is not binary-exact: grid points may differ from numpy by one ulp where
    # numpy fuses the multiply-add. Sample boundaries stay clear of the grid, so the
    # values must match exactly.
    rng = np.random.default_rng(11)
    times = 1234.5678 + np.concatenate([[0.0], np.cumsum(rng.uniform(0.031, 0.037, 400))])
    tstarts, tends = times[:-1].tolist(), times[1:].tolist()
    values = rng.normal(size=400).tolist()
    actual = _resample_in_spark(
        spark,
        {(1, 1, 1): (tstarts, tends, values)},
        {1: 0.1},
        time_type=T.DoubleType(),
        chunk_rows=chunk_rows,
    )[(1, 1, 1)]
    expected = _expected((tstarts, tends, values), 0.1)
    assert len(actual) == len(expected)
    nptest.assert_array_max_ulp(actual.tstarts, expected.tstarts, maxulp=1)
    nptest.assert_array_max_ulp(actual.tends, expected.tends, maxulp=1)
    nptest.assert_array_equal(actual.values, expected.values)
