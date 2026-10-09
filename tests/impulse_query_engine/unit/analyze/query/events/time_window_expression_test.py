from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from impulse_query_engine.analyze.query.aggregations.stats_aggregator import StatsAggregator
from impulse_query_engine.analyze.query.events import TimeWindowExpression
from impulse_query_engine.analyze.query.events.time_window_expression import (
    MAX_WINDOWS_PER_CONTAINER,
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


def _as_list(windows) -> list[tuple[float, float]]:
    """Windows as ordered (start, end) pairs."""
    return [(float(s), float(e)) for s, e in windows]


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
