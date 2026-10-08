from __future__ import annotations

import random
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from impulse_query_engine.analyze.query.aggregations.stats_aggregator import StatsAggregator
from impulse_query_engine.analyze.query.events import TimeWindowExpression
from impulse_query_engine.analyze.query.events.time_window_expression import (
    MAX_WINDOWS_PER_CONTAINER,
    _window_batches,
    explode_windows,
    tile_windows,
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


# The container bounds in the channel time frame, as SolverConfig.with_window_bounds adds them.
_WINDOW_START, _WINDOW_STOP = "__window_start", "__window_stop"


def _build(start_ts, stop_ts, window_length, **kwargs) -> Intervals:
    expr = TimeWindowExpression(window_length, **kwargs)
    return expr.build(_FakeCache({_WINDOW_START: start_ts, _WINDOW_STOP: stop_ts}))


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
    # The bounds in the channel time frame, not the raw start_ts / stop_ts (which UDFs
    # keep reading unconverted).
    assert expr.required_container_metrics() == {_WINDOW_START, _WINDOW_STOP}
    assert expr.required_tags() == set()


def test_str_includes_window_length():
    assert "window_length=10" in str(TimeWindowExpression(10))


def test_str_stable_across_int_and_float_window_length():
    # int 10 and float 10.0 are the same window; the string form (which feeds the event
    # definition hash) must not differ between them.
    assert str(TimeWindowExpression(10)) == str(TimeWindowExpression(10.0))


def test_str_includes_channel_time_frame_only_when_set():
    # The string feeds the definition hashes of the event and its scoped aggregations, so
    # the channel time frame must move them, while the defaults keep the plain form.
    expr = TimeWindowExpression(10)
    assert str(expr) == "TimeWindowExpression<window_length=10.0>"
    expr.channel_time_unit = "ms"
    assert str(expr) == "TimeWindowExpression<window_length=10.0, channel_time_unit=ms>"
    expr.channel_time_origin = "container_start"
    assert str(expr) == (
        "TimeWindowExpression<window_length=10.0, channel_time_unit=ms, "
        "channel_time_origin=container_start>"
    )
    expr.container_time_unit = "s"
    assert str(expr).endswith(", container_time_unit=s>")


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
    with pytest.raises(ValueError, match="unit of the channel timestamps"):
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


def test_nan_container_metrics_yield_empty():
    # A null start/stop arrives as NaN in a float64 column.
    assert len(_build(np.nan, 100.0, 10)) == 0
    assert len(_build(0.0, np.nan, 10)) == 0


def test_infinite_container_metrics_yield_empty():
    # Used to overflow in int(np.ceil(inf)).
    assert len(_build(0.0, np.inf, 10)) == 0
    assert len(_build(-np.inf, 100.0, 10)) == 0


def test_tile_windows_none_and_na_yield_empty():
    for start, stop in ((None, 100), (0, None), (pd.NA, 100), (0, np.nan)):
        starts, ends = tile_windows(start, stop, 10)
        assert len(starts) == len(ends) == 0


# ---------------------------------------------------------------------------
# explode_windows: tile_windows on the event fact side (one row per window)
# ---------------------------------------------------------------------------
def _exploded(spark, rows, windows, ts_type="long", id_type="int"):  # noqa: F811
    """explode_windows over (k, start_ts, stop_ts) rows, as {(k, event_name): windows}."""
    # One partition, so all rows reach the function in one Arrow batch.
    df = spark.createDataFrame(
        rows, f"k {id_type}, start_ts {ts_type}, stop_ts {ts_type}"
    ).coalesce(1)
    out = explode_windows(
        df, id_col="k", start_col="start_ts", stop_col="stop_ts", windows=windows
    )
    grouped = {}
    for r in out.collect():
        grouped.setdefault((r.k, r.event_name), []).append([r.start_ts, r.end_ts])
    return out, grouped


def test_explode_windows_edge_cases(spark):  # noqa: F811
    rows = [
        (0, 0, 100),  # exact multiple -> 10 windows
        (1, 0, 105),  # short final window clamped to stop
        (2, 1000, 1010),  # span == W -> 1 window
        (3, 1000, 1001),  # span < W -> 1 clamped window
        (4, 50, 50),  # stop == start -> none
        (5, 60, 50),  # stop < start -> none
        (6, None, 50),  # null bound -> none
        (7, 0, None),
    ]
    out, w = _exploded(spark, rows, [("tw", 10, MAX_WINDOWS_PER_CONTAINER)])

    assert out.schema.simpleString() == (
        "struct<k:int,event_name:string,start_ts:double,end_ts:double>"
    )
    assert w[(0, "tw")] == [[float(s), float(s + 10)] for s in range(0, 100, 10)]
    assert w[(1, "tw")][-1] == [100.0, 105.0] and len(w[(1, "tw")]) == 11
    assert w[(2, "tw")] == [[1000.0, 1010.0]]
    assert w[(3, "tw")] == [[1000.0, 1001.0]]
    assert set(w) == {(k, "tw") for k in range(4)}


def test_explode_windows_non_finite_bounds_yield_no_windows(spark):  # noqa: F811
    nan, inf = float("nan"), float("inf")
    rows = [(0, 0.0, nan), (1, nan, 10.0), (2, nan, nan), (3, 0.0, inf), (4, -inf, 10.0)]
    out, _ = _exploded(spark, rows, [("tw", 4, MAX_WINDOWS_PER_CONTAINER)], ts_type="double")
    assert out.count() == 0


def test_explode_windows_without_any_window_yields_no_rows(spark):  # noqa: F811
    rows = [(0, None, None), (1, 50, 50)]
    out, _ = _exploded(spark, rows, [("tw", 10, MAX_WINDOWS_PER_CONTAINER)])
    assert out.count() == 0


def test_explode_windows_raises_beyond_max_windows(spark):  # noqa: F811
    _, ok = _exploded(spark, [(0, 0, 100)], [("tw", 10, 10)])
    assert len(ok[(0, "tw")]) == 10
    with pytest.raises(Exception, match="10 windows of length 10.0 .* exceed max_windows=9"):
        _exploded(spark, [(0, 0, 100)], [("tw", 10, 9)])


def test_explode_windows_invalid_max_windows_raises(spark):  # noqa: F811
    df = spark.createDataFrame([(0, 0, 100)], "k int, start_ts long, stop_ts long")
    with pytest.raises(ValueError, match="max_windows must be a positive integer"):
        explode_windows(
            df, id_col="k", start_col="start_ts", stop_col="stop_ts", windows=[("tw", 10, 0)]
        )


def test_explode_windows_several_events_in_one_pass(spark):  # noqa: F811
    rows = [(0, 0, 105), (1, 1000, 1030)]
    events = [("ten", 10, 100), ("seven", 7.5, 100)]
    _, w = _exploded(spark, rows, events)

    assert set(w) == {(k, name) for k, _, _ in rows for name, _, _ in events}
    for k, start, stop in rows:
        for name, length, _ in events:
            expected = [[s, e] for s, e in zip(*tile_windows(start, stop, length), strict=True)]
            assert w[(k, name)] == expected


@pytest.mark.parametrize(
    "id_type, ids", [("int", [7, 8]), ("bigint", [2**40, 2**40 + 1]), ("string", ["c-a", "c-b"])]
)
def test_explode_windows_keeps_container_id_type(spark, id_type, ids):  # noqa: F811
    rows = [(ids[0], 0, 30), (ids[1], 100, 120)]
    out, w = _exploded(spark, rows, [("tw", 10, 100)], id_type=id_type)

    assert out.schema["k"].dataType.simpleString() == id_type
    assert w == {
        (ids[0], "tw"): [[0.0, 10.0], [10.0, 20.0], [20.0, 30.0]],
        (ids[1], "tw"): [[100.0, 110.0], [110.0, 120.0]],
    }


def _bounds_batch(ids, starts, stops) -> pa.RecordBatch:
    return pa.RecordBatch.from_arrays(
        [pa.array(ids, pa.string()), pa.array(starts, pa.int64()), pa.array(stops, pa.int64())],
        names=["k", "start_ts", "stop_ts"],
    )


def test_window_batches_flush_between_events_and_per_input_batch():
    # Each container gives 4 "a" windows and 2 "b" windows.
    events = [("a", 10.0, 100), ("b", 20.0, 100)]
    first = _bounds_batch(["c0", "c1", "c2"], [0, 100, 200], [40, 140, 240])
    second = _bounds_batch(["c3"], [300], [340])

    def run(batch_windows):
        return list(
            _window_batches(
                iter([first, second]), "k", "start_ts", "stop_ts", events, batch_windows
            )
        )

    batches = run(batch_windows=9)
    # 6 windows after c0, 10 after c1's "a" -> flush, before c1's "b"; c1's "b" and c2
    # flushed at the end of the first input batch; c3 alone from the second one.
    assert [b.num_rows for b in batches] == [10, 8, 6]
    pairs = [
        set(zip(b.column("k").to_pylist(), b.column("event_name").to_pylist(), strict=True))
        for b in batches
    ]
    assert pairs == [
        {("c0", "a"), ("c0", "b"), ("c1", "a")},
        {("c1", "b"), ("c2", "a"), ("c2", "b")},
        {("c3", "a"), ("c3", "b")},
    ]
    assert batches[0].schema.names == ["k", "event_name", "start_ts", "end_ts"]
    assert batches[0].schema.field("k").type == pa.string()
    assert batches[0].column("event_name").to_pylist()[:6] == ["a"] * 4 + ["b"] * 2

    # Many events per container stay bounded: each flush adds at most one event's windows.
    many = [(f"e{i}", 10.0, 100) for i in range(5)]
    sizes = [
        b.num_rows
        for b in _window_batches(iter([first]), "k", "start_ts", "stop_ts", many, batch_windows=6)
    ]
    assert sizes == [8] * 7 + [4]

    # Batching only splits the stream; the rows are those of a single batch per input batch.
    unbatched = pa.Table.from_batches(run(batch_windows=10**9))
    assert pa.Table.from_batches(batches).equals(unbatched)


def _as_list(windows) -> list[tuple[float, float]]:
    """Windows as ordered (start, end) pairs."""
    return [(float(s), float(e)) for s, e in windows]


def _count_mismatch_case(window_length: float) -> tuple[int, int]:
    """Find ns-epoch (start, stop) whose int64 and double spans yield different counts.

    This is exactly the case where exact int64 subtraction and double subtraction disagree
    on the number of windows, so tile_windows must convert to float first on every path.
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


def test_event_fact_and_solve_windows_identical_across_input_dtypes(spark):  # noqa: F811
    """Both sides call tile_windows, but receive the bounds differently: the event fact as
    Python ints from the Arrow long columns, the solve as float64 (its container metrics are
    nulled on most rows). For ns epochs beyond 2^53, including a span where int64 and double
    subtraction disagree on the count, both must produce the same windows in the same order,
    also with a null bound in the same Arrow batch."""
    rnd = random.Random(7)
    window_length = 1_000_000_007.0
    cases = []
    for _ in range(60):
        start = rnd.randint(1_600_000_000_000_000_000, 1_800_000_000_000_000_000)
        cases.append(
            (start, start + int(rnd.randint(1, 50) * window_length) + rnd.randint(-600, 600))
        )
    cases.append(_count_mismatch_case(window_length))
    rows = [(k, s, e) for k, (s, e) in enumerate(cases)]
    windows = [("tw", window_length, MAX_WINDOWS_PER_CONTAINER)]

    _, without_nulls = _exploded(spark, rows, windows)
    _, with_null = _exploded(spark, [*rows, (-1, None, None)], windows)

    mismatches = []
    for k, (start, stop) in enumerate(cases):
        solve = _as_list(_build(np.float64(start), np.float64(stop), window_length).get_data())
        assert solve, f"case {k} produced no windows"
        if not (_as_list(without_nulls[(k, "tw")]) == _as_list(with_null[(k, "tw")]) == solve):
            mismatches.append((k, start, stop))
    assert not mismatches, f"event fact / solve window mismatch: {mismatches[:5]}"


def test_stats_aggregator_windows_equal_helper_windows(spark):  # noqa: F811
    """A StatsAggregator scoped to a TimeWindowExpression emits exactly the helper's windows
    (no merging of touching windows, no extra drops)."""
    start, stop, w = 1_700_000_000_000_000_123, 1_700_000_007_000_000_049, 1_000_000_007.0
    expected = list(zip(*tile_windows(start, stop, w), strict=True))

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
    cache = _FakeCache({_WINDOW_START: np.float64(start), _WINDOW_STOP: np.float64(stop)})
    event_timestamps, numeric_values, _, _ = agg.build(cache)

    assert len(expected) == 7
    # The same windows, in the same order.
    assert _as_list(event_timestamps) == _as_list(expected)
    assert len(numeric_values[0]) == len(event_timestamps)
