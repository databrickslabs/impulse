from __future__ import annotations

import random
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pytest

from impulse_query_engine.analyze.query.aggregations.stats_aggregator import StatsAggregator
from impulse_query_engine.analyze.query.events import TimeWindowExpression
from impulse_query_engine.analyze.query.events.time_window_expression import (
    MAX_WINDOWS_PER_CONTAINER,
    window_intervals_col,
)
from impulse_query_engine.analyze.query.solvers.empty_cache import EmptyTimeSeriesCache
from impulse_query_engine.model.series.intervals import Intervals
from impulse_query_engine.model.series.sample_series import SampleSeries
from tests.conftest import spark  # noqa: F401  (pytest fixture)


class _FakeCache:
    """Minimal SeriesCache stand-in exposing container metrics for ``build``."""

    def __init__(self, container_metrics: dict):
        self._container_metrics = container_metrics

    @property
    def container_metrics(self) -> dict:
        return self._container_metrics

    @property
    def container_tags(self) -> dict:
        return {}


def _build(start_ts, stop_ts, window_length, **kwargs) -> Intervals:
    expr = TimeWindowExpression(window_length, **kwargs)
    return expr.build(_FakeCache({"start_ts": start_ts, "stop_ts": stop_ts}))


def test_exact_multiple_windows_last_ends_at_stop():
    """D=100, W=10 -> 10 contiguous windows, final window ends exactly at stop_ts."""
    iv = _build(0, 100, 10)
    assert len(iv) == 10
    assert iv.tstarts.tolist() == [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
    assert iv.tends.tolist() == [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    # Contiguity: each window's end equals the next window's start.
    assert iv.tstarts[1:].tolist() == iv.tends[:-1].tolist()
    assert iv.tends[-1] == 100


def test_non_multiple_has_short_final_window_clamped_to_stop():
    """D=105, W=10 -> 11 windows; the last is a short slice clamped to stop_ts."""
    iv = _build(0, 105, 10)
    assert len(iv) == 11
    assert iv.tstarts[-1] == 100
    assert iv.tends[-1] == 105  # clamped, not 110
    # The final window is shorter than the fixed length.
    assert (iv.tends[-1] - iv.tstarts[-1]) < 10
    assert iv.tstarts[1:].tolist() == iv.tends[:-1].tolist()


def test_span_equal_to_window_yields_single_window():
    iv = _build(1000, 1010, 10)
    assert len(iv) == 1
    assert iv.tstarts.tolist() == [1000]
    assert iv.tends.tolist() == [1010]


def test_span_smaller_than_window_yields_single_clamped_window():
    iv = _build(1000, 1001, 10)
    assert len(iv) == 1
    assert iv.tstarts.tolist() == [1000]
    assert iv.tends.tolist() == [1001]


def test_epoch_millisecond_boundaries():
    """Realistic epoch-ms container with a 10s (10000 ms) window."""
    start, stop = 1751528502708, 1751528610253  # ~107.545 s span
    iv = _build(start, stop, 10000)
    assert len(iv) == int(np.ceil((stop - start) / 10000))  # 11
    assert iv.tstarts[0] == start
    assert iv.tends[-1] == stop
    assert iv.tstarts[1:].tolist() == iv.tends[:-1].tolist()
    assert bool(np.all(iv.tstarts < iv.tends))


def test_degenerate_container_yields_no_windows():
    assert len(_build(50, 50, 10)) == 0  # stop == start
    assert len(_build(60, 50, 10)) == 0  # stop < start


def test_missing_container_metrics_yields_empty():
    expr = TimeWindowExpression(10)
    assert len(expr.build(_FakeCache({}))) == 0
    # The empty cache used by evaluation_type() has no container metrics.
    assert len(expr.build(EmptyTimeSeriesCache())) == 0


def test_evaluation_type_is_intervals():
    assert TimeWindowExpression(10).evaluation_type() is Intervals


def test_no_selectors_and_requests_container_metrics():
    expr = TimeWindowExpression(10)
    assert expr.get_selectors() == []
    assert expr.get_selector_expr() is None
    assert expr.required_container_metrics() == {"start_ts", "stop_ts"}
    assert expr.required_tags() == set()


def test_str_includes_window_length():
    assert "window_length=10" in str(TimeWindowExpression(10))


def test_str_stable_across_int_and_float_window_length():
    # int 10 and float 10.0 are the same window; the string form (which feeds the event
    # definition hash) must not differ between them.
    assert str(TimeWindowExpression(10)) == str(TimeWindowExpression(10.0))


def test_str_includes_epoch_unit_only_when_set():
    # The string feeds the definition hashes of the event and its scoped aggregations, so
    # the unit must move them, while an unset unit keeps the unit-less form.
    expr = TimeWindowExpression(10)
    assert str(expr) == "TimeWindowExpression<window_length=10.0>"
    expr.epoch_unit = "ms"
    assert str(expr) == "TimeWindowExpression<window_length=10.0, epoch_unit=ms>"


def test_max_windows_not_part_of_str():
    # The cap only decides between an error and a result, so it must not force a recompute.
    assert MAX_WINDOWS_PER_CONTAINER == 1_000_000
    assert TimeWindowExpression(10).max_windows == MAX_WINDOWS_PER_CONTAINER
    assert str(TimeWindowExpression(10, max_windows=5)) == str(TimeWindowExpression(10))


@pytest.mark.parametrize("bad", [0, -1, 1.5, True, None, "10"])
def test_invalid_max_windows_raises(bad):
    with pytest.raises(ValueError, match="max_windows must be a positive integer"):
        TimeWindowExpression(10, max_windows=bad)


def test_build_raises_beyond_max_windows():
    # 10 windows are fine at max_windows=10, not at 9.
    assert len(_build(0, 100, 10, max_windows=10)) == 10
    with pytest.raises(ValueError, match="10 windows of length 10.0 .* exceed max_windows=9"):
        _build(0, 100, 10, max_windows=9)


def test_build_unit_mismatch_hits_default_cap():
    # window_length=60 meant as seconds over a 1 h ns-epoch span: 6e10 windows.
    start = 1_700_000_000_000_000_000
    with pytest.raises(ValueError, match="epoch unit of the container boundaries"):
        _build(np.int64(start), np.int64(start + 3_600_000_000_000), 60)


@pytest.mark.parametrize("bad", [0, -1, -10.5, None, float("inf"), float("-inf"), float("nan")])
def test_non_positive_window_length_raises(bad):
    with pytest.raises(ValueError, match="strictly positive"):
        TimeWindowExpression(bad)


def test_int64_and_float64_inputs_build_identical_windows():
    # A long container metric reaches pandas as int64 or float64 depending on the group
    # (nulls force float64); both must produce the same windows.
    start, stop = 1_700_000_000_000_000_123, 1_700_000_007_000_000_049
    a = _build(np.int64(start), np.int64(stop), 1_000_000_007)
    b = _build(np.float64(start), np.float64(stop), 1_000_000_007)
    assert a.get_data() == b.get_data()


def test_datetime_container_metrics_raise_clear_error():
    # TIMESTAMP boundaries reach pandas as pd.Timestamp unless epoch_unit converts them.
    start, stop = pd.Timestamp("2025-07-03 07:41:41"), pd.Timestamp("2025-07-03 07:43:30")
    with pytest.raises(TypeError, match="epoch_unit"):
        _build(start, stop, 10)


def test_nan_container_metrics_yield_empty():
    # A null start/stop arrives as NaN in a float64 column.
    assert len(_build(np.nan, 100.0, 10)) == 0
    assert len(_build(0.0, np.nan, 10)) == 0


def test_infinite_container_metrics_yield_empty():
    # Used to overflow in int(np.ceil(inf)).
    assert len(_build(0.0, np.inf, 10)) == 0
    assert len(_build(-np.inf, 100.0, 10)) == 0


# ---------------------------------------------------------------------------
# window_intervals_col: the native-Spark mirror used by the reporting event fact
# ---------------------------------------------------------------------------
def _spark_windows(spark, rows, window_length, ts_type="long"):  # noqa: F811
    df = spark.createDataFrame(rows, f"k int, start_ts {ts_type}, stop_ts {ts_type}")
    out = df.select(
        "k", window_intervals_col(F.col("start_ts"), F.col("stop_ts"), window_length).alias("w")
    )
    return out, {r.k: [list(p) for p in r.w] for r in out.collect()}


def test_window_intervals_col_edge_cases(spark):  # noqa: F811
    rows = [
        (0, 0, 100),  # exact multiple -> 10 windows
        (1, 0, 105),  # short final window clamped to stop
        (2, 1000, 1010),  # span == W -> 1 window
        (3, 1000, 1001),  # span < W -> 1 clamped window
        (4, 50, 50),  # stop == start -> none (sequence(0, -1) is [0, -1], not [])
        (5, 60, 50),  # stop < start -> none
        (6, None, 50),  # null bound -> none
        (7, 0, None),
    ]
    out, w = _spark_windows(spark, rows, 10)

    assert out.schema["w"].dataType.simpleString() == "array<array<double>>"
    assert w[0] == [[float(s), float(s + 10)] for s in range(0, 100, 10)]
    assert w[1][-1] == [100.0, 105.0] and len(w[1]) == 11
    assert w[2] == [[1000.0, 1010.0]]
    assert w[3] == [[1000.0, 1001.0]]
    assert w[4] == w[5] == w[6] == w[7] == []
    assert all(s < e for windows in w.values() for s, e in windows)


def test_window_intervals_col_non_finite_bounds_yield_no_windows(spark):  # noqa: F811
    """NaN / infinite boundaries give no windows on both sides. Spark orders NaN above
    every number, so a NaN stop_ts used to pass ``stop > start`` and emit [[0, 4], [-4, 0]]."""
    nan, inf = float("nan"), float("inf")
    rows = [(0, 0.0, nan), (1, nan, 10.0), (2, nan, nan), (3, 0.0, inf), (4, -inf, 10.0)]
    _, w = _spark_windows(spark, rows, 4, ts_type="double")

    for k, start, stop in rows:
        assert w[k] == [], f"row {k} ({start}, {stop}) produced {w[k]}"
        assert _build(start, stop, 4).get_data() == []


def test_window_intervals_col_raises_beyond_max_windows(spark):  # noqa: F811
    df = spark.createDataFrame([(0, 0, 100)], "k int, start_ts long, stop_ts long")
    ok = df.select(window_intervals_col(F.col("start_ts"), F.col("stop_ts"), 10, max_windows=10))
    assert len(ok.collect()[0][0]) == 10

    too_many = df.select(
        window_intervals_col(F.col("start_ts"), F.col("stop_ts"), 10, max_windows=9)
    )
    with pytest.raises(Exception, match="10 windows of length 10.0 .* exceed max_windows=9"):
        too_many.collect()


def test_window_intervals_col_invalid_max_windows_raises():
    with pytest.raises(ValueError, match="max_windows must be a positive integer"):
        window_intervals_col(F.col("start_ts"), F.col("stop_ts"), 10, max_windows=0)


def _as_list(windows) -> list[tuple[float, float]]:
    """Windows as ordered (start, end) pairs: the order is the window index the ids hash."""
    return [(float(s), float(e)) for s, e in windows]


def _count_mismatch_case(window_length: float) -> tuple[int, int]:
    """Find ns-epoch (start, stop) whose int64 and double spans yield different counts.

    This is exactly the case where numpy without the float() conversion (exact int64
    subtraction) and Spark (double subtraction) disagree on the number of windows.
    """
    base = 1_700_000_000_000_000_000
    for start in range(base, base + 512):
        for k in (1, 3, 5, 7):
            for d in range(-300, 1):
                stop = start + int(k * window_length) + d
                exact = int(np.ceil((stop - start) / window_length))
                rounded = int(np.ceil((float(stop) - float(start)) / window_length))
                if exact != rounded:
                    return start, stop
    raise AssertionError("no int64/double count-mismatch case found")


def test_window_intervals_col_bit_identical_to_build(spark):  # noqa: F811
    """The event fact (Spark) and scoped aggregations (numpy ``build``) produce the same
    windows in the same order: event_instance_id hashes the window's position, and the
    stored boundaries must describe the window the statistics were computed over."""
    rnd = random.Random(7)

    # Long timestamps: ns epochs (~1.7e18, beyond 2^53) and µs epochs, with spans hugging
    # multiples of W. Includes W values that are not multiples of the 256 ns double spacing.
    long_cases = []
    for w in (1e9, 6e10, 333_333_333.0, 1_000_000_007.0):
        for _ in range(60):
            start = rnd.randint(1_600_000_000_000_000_000, 1_800_000_000_000_000_000)
            stop = start + int(rnd.randint(1, 50) * w) + rnd.randint(-600, 600)
            long_cases.append((start, stop, w))
    for w in (1e6, 10_000_000.0):
        for _ in range(30):
            start = rnd.randint(1_600_000_000_000_000, 1_800_000_000_000_000)
            stop = start + int(rnd.randint(1, 50) * w) + rnd.randint(-5, 5)
            long_cases.append((start, stop, w))
    edge_start, edge_stop = _count_mismatch_case(1_000_000_007.0)
    long_cases.append((edge_start, edge_stop, 1_000_000_007.0))

    # Seconds as doubles with fractional window lengths.
    double_cases = []
    for w in (0.1, 0.25, 0.3, 1.7, 60.0):
        for _ in range(60):
            start = rnd.uniform(1.6e9, 1.8e9)
            stop = start + rnd.randint(1, 50) * w + rnd.uniform(-1e-6, 1e-6)
            double_cases.append((start, stop, w))

    for cases, ts_type, np_types in (
        (long_cases, "long", (np.int64, np.float64)),
        (double_cases, "double", (np.float64,)),
    ):
        lengths = sorted({w for _, _, w in cases})
        df = spark.createDataFrame(
            [(k, s, e, w) for k, (s, e, w) in enumerate(cases)],
            f"k int, start_ts {ts_type}, stop_ts {ts_type}, w double",
        )
        # A single CASE WHEN column computes each row's windows for its own length only,
        # so all cases run in one Spark job.
        windows_col = None
        for w in lengths:
            branch = window_intervals_col(F.col("start_ts"), F.col("stop_ts"), w)
            condition = F.col("w") == F.lit(w)
            windows_col = (
                F.when(condition, branch)
                if windows_col is None
                else windows_col.when(condition, branch)
            )
        spark_windows = {
            r.k: r.windows for r in df.select("k", windows_col.alias("windows")).collect()
        }

        mismatches = []
        for k, (start, stop, w) in enumerate(cases):
            expected = _as_list(spark_windows[k])
            assert expected, f"case {k} produced no windows"
            for np_type in np_types:
                built = _as_list(_build(np_type(start), np_type(stop), w).get_data())
                if built != expected:
                    mismatches.append((k, np_type.__name__, start, stop, w))
        assert not mismatches, f"Spark/numpy window mismatch: {mismatches[:5]}"


def test_stats_aggregator_windows_equal_helper_windows(spark):  # noqa: F811
    """A StatsAggregator scoped to a TimeWindowExpression emits exactly the helper's windows
    (no merging of touching windows, no extra drops)."""
    start, stop, w = 1_700_000_000_000_000_123, 1_700_000_007_000_000_049, 1_000_000_007.0
    _, spark_windows = _spark_windows(spark, [(0, start, stop)], w)

    # One channel sampled across the whole container, so every window has data.
    ts = np.linspace(float(start), float(stop), 50)
    series = SampleSeries(tstarts=ts[:-1], tends=ts[1:], values=np.arange(49, dtype=float))

    channel = MagicMock()
    channel.build.return_value = series

    agg = StatsAggregator(
        input_expressions=[channel],
        event_expression=TimeWindowExpression(w),
        statistics=["mean"],
    )
    cache = _FakeCache({"start_ts": np.float64(start), "stop_ts": np.float64(stop)})
    event_timestamps, numeric_values, _, _ = agg.build(cache)

    assert len(spark_windows[0]) == 7
    # Same windows in the same order: event_timestamps' position is the window index.
    assert _as_list(event_timestamps) == _as_list(spark_windows[0])
    assert len(numeric_values[0]) == len(event_timestamps)
