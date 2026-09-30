from __future__ import annotations

import numpy as np
import pytest

from impulse_query_engine.analyze.query.events import TimeWindowExpression
from impulse_query_engine.analyze.query.solvers.empty_cache import EmptyTimeSeriesCache
from impulse_query_engine.model.series.intervals import Intervals


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


def _build(start_ts, stop_ts, window_length) -> Intervals:
    expr = TimeWindowExpression(window_length)
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


@pytest.mark.parametrize("bad", [0, -1, -10.5, None])
def test_non_positive_window_length_raises(bad):
    with pytest.raises(ValueError, match="strictly positive"):
        TimeWindowExpression(bad)
