"""Tests for Intervals, including merge_intervals/merge_overlaps and sustaining
debounce/filter TimeSeriesOp expressions."""

# pylint: disable=missing-function-docstring
import numpy as np
import numpy.testing as nptest
import pandas as pd
import pytest

from mda_query_engine.analyze.metadata.tag_expression import TagSelector
from mda_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesSelector,
)
from mda_query_engine.model.series.intervals import Intervals
from mda_query_engine.model.series.points_in_time import PointsInTime
from mda_query_engine.model.series.sample_series import SampleSeries
from mda_reporting.events.basic_event import BasicEvent

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cache(mocker, series: SampleSeries):
    """Return a mock SeriesCache that always yields *series*."""
    cache = mocker.MagicMock()
    # resolve() must return a DataFrame with container_id / channel_id columns
    cache.resolve.return_value = pd.DataFrame({"container_id": [0], "channel_id": [0]})
    cache.load_blob.return_value = series
    return cache


def _eng_spd_series() -> SampleSeries:
    """
    Simulated engine-speed signal (1 s samples, seconds as timestamps):

    t=0..1   rpm=1000  (below threshold)
    t=1..2   rpm=2500  (event A start)
    t=2..3   rpm=2500
    t=3..4   rpm=1000  (gap of 2 s)
    t=5..6   rpm=2500  (event B start)
    t=6..7   rpm=2500
    t=7..8   rpm=500   (below threshold)
    t=8..9   rpm=2500  (event C – only 1 s, short)
    t=9..10  rpm=500
    """
    tstarts = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    tends = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    values = [1000, 2500, 2500, 1000, 1000, 2500, 2500, 500, 2500, 500]
    return SampleSeries(tstarts, tends, values)


def test_merge_overlaps1():
    intvls = Intervals([0, 1, 2, 4], [1, 2, 3, 5], merge_overlaps=False)
    result = intvls.merge_overlaps()
    nptest.assert_array_equal([0, 4], result.tstarts)
    nptest.assert_array_equal([3, 5], result.tends)


def test_merge_overlaps2():
    intvls = Intervals([0, 0.8, 2, 4], [1, 2, 3, 5], merge_overlaps=False)
    result = intvls.merge_overlaps()
    nptest.assert_array_equal([0, 4], result.tstarts)
    nptest.assert_array_equal([3, 5], result.tends)


def test_merge_overlaps3():
    intvls = Intervals([0, 2, 4], [2, 4, 4], merge_overlaps=True)
    result = intvls.merge_overlaps()
    nptest.assert_array_equal([0, 4], result.tstarts)
    nptest.assert_array_equal([4, 4], result.tends)


def test_starts_empty():
    intvls = Intervals.empty()
    assert len(intvls.starts()) == 0


def test_starts():
    intvls = Intervals([0, 1], [1, 2])
    nptest.assert_array_equal([0, 1], intvls.starts())


def test_ends_empty():
    intvls = Intervals.empty()
    assert len(intvls.ends()) == 0


def test_ends():
    intvls = Intervals([0, 1], [1, 2])
    nptest.assert_array_equal([0, 1], intvls.starts())


def test_start_time_empty():
    intvls = Intervals.empty()
    assert np.isnan(intvls.start_time())


def test_start_time():
    intvls = Intervals([10, 100], [11, 101])
    assert 10 == intvls.start_time()


def test_end_time_empty():
    intvls = Intervals.empty()
    assert np.isnan(intvls.end_time())


def test_end_time():
    intvls = Intervals([10, 100], [11, 101])
    assert 101 == intvls.end_time()


def test_duration_ms_empty():
    intvls = Intervals.empty()
    assert np.isnan(intvls.duration_ms())


def test_duration_ms():
    intvls = Intervals([0, 1], [1, 2])
    assert 2000 == intvls.duration_ms()


def test_durations_empty():
    intvls = Intervals.empty()
    assert len(intvls.durations()) == 0


def test_durations():
    intvls = Intervals([0, 1, 2], [1, 2, 4])
    nptest.assert_array_equal([1, 1, 2], intvls.durations())


def test_expand_left_empty():
    intvls = Intervals.empty()
    assert len(intvls.expand_left(1)) == 0


def test_expand_left():
    intvls = Intervals([1, 101], [2, 102])
    result = intvls.expand_left(1)
    nptest.assert_array_equal([0, 100], result.tstarts)
    nptest.assert_array_equal([2, 102], result.tends)


def test_expand_left_merged():
    intvls = Intervals([1, 2], [2, 3])
    result = intvls.expand_left(1)
    nptest.assert_array_equal([0], result.tstarts)
    nptest.assert_array_equal([3], result.tends)


def test_expand_right_empty():
    intvls = Intervals.empty()
    assert len(intvls.expand_right(1)) == 0


def test_expand_right():
    intvls = Intervals([1, 101], [2, 102])
    result = intvls.expand_right(1)
    nptest.assert_array_equal([1, 101], result.tstarts)
    nptest.assert_array_equal([3, 103], result.tends)


def test_expand_right_merged():
    intvls = Intervals([1, 2], [2, 3])
    result = intvls.expand_right(1)
    nptest.assert_array_equal([1], result.tstarts)
    nptest.assert_array_equal([4], result.tends)


def test_expand_empty():
    intvls = Intervals.empty()
    assert len(intvls.expand(1)) == 0


def test_expand():
    intvls = Intervals([1, 101], [2, 102])
    result = intvls.expand(1)
    nptest.assert_array_equal([0, 100], result.tstarts)
    nptest.assert_array_equal([3, 103], result.tends)


def test_expand_merged():
    intvls = Intervals([1, 2, 3], [2, 3, 4])
    result = intvls.expand(1)
    nptest.assert_array_equal([0], result.tstarts)
    nptest.assert_array_equal([5], result.tends)


def test_shrink_left_empty():
    intvls = Intervals.empty()
    assert len(intvls.shrink_left(1)) == 0


def test_shrink_left():
    intvls = Intervals([1, 101], [3, 103])
    result = intvls.shrink_left(1)
    nptest.assert_array_equal([2, 102], result.tstarts)
    nptest.assert_array_equal([3, 103], result.tends)


def test_shrink_left_resultempty():
    intvls = Intervals([1, 2], [2, 3])
    result = intvls.shrink_left(1)
    nptest.assert_array_equal([], result.tstarts)
    nptest.assert_array_equal([], result.tends)


def test_shrink_right_empty():
    intvls = Intervals.empty()
    assert len(intvls.shrink_right(1)) == 0


def test_shrink_right():
    intvls = Intervals([0, 100], [2, 102])
    result = intvls.shrink_right(1)
    nptest.assert_array_equal([0, 100], result.tstarts)
    nptest.assert_array_equal([1, 101], result.tends)


def test_shrink_right_resultempty():
    intvls = Intervals([1, 2], [2, 3])
    result = intvls.shrink_right(1)
    nptest.assert_array_equal([], result.tstarts)
    nptest.assert_array_equal([], result.tends)


def test_shrink_empty():
    intvls = Intervals.empty()
    assert len(intvls.shrink(1)) == 0


def test_shrink():
    intvls = Intervals([1, 101], [4, 104])
    result = intvls.shrink(1)
    nptest.assert_array_equal([2, 102], result.tstarts)
    nptest.assert_array_equal([3, 103], result.tends)


def test_shrink_empty_result():
    intvls = Intervals([1, 2, 3], [2, 3, 4])
    result = intvls.shrink(1)
    nptest.assert_array_equal([], result.tstarts)
    nptest.assert_array_equal([], result.tends)


def test_and_empty():
    intvls1 = Intervals.empty()
    intvls2 = Intervals.empty()
    assert len(intvls1 & intvls2) == 0


def test_and():
    intvls1 = Intervals([0], [2])
    intvls2 = Intervals([0.5], [1.5])
    result = intvls1 & intvls2
    nptest.assert_array_equal([0.5], result.tstarts)
    nptest.assert_array_equal([1.5], result.tends)


def test_and_nooverlap():
    intvls1 = Intervals([0], [2])
    intvls2 = Intervals([3], [4])
    result = intvls1 & intvls2
    nptest.assert_array_equal([], result.tstarts)
    nptest.assert_array_equal([], result.tends)


def test_and_nooverlap2():
    intvls1 = Intervals([0], [2])
    intvls2 = Intervals([2], [4])
    result = intvls1 & intvls2
    nptest.assert_array_equal([], result.tstarts)
    nptest.assert_array_equal([], result.tends)


def test_or_empty():
    intvls1 = Intervals.empty()
    intvls2 = Intervals.empty()
    assert len(intvls1 | intvls2) == 0


def test_or():
    intvls1 = Intervals([0, 1, 2], [1, 2, 3])
    intvls2 = Intervals([0.5], [1.5])
    result = intvls1 | intvls2
    nptest.assert_array_equal([0], result.tstarts)
    nptest.assert_array_equal([3], result.tends)


def test_or_nooverlap():
    intvls1 = Intervals([0, 1, 2], [1, 2, 3])
    intvls2 = Intervals([4], [5])
    result = intvls1 | intvls2
    nptest.assert_array_equal([0, 4], result.tstarts)
    nptest.assert_array_equal([3, 5], result.tends)


def test_or_nooverlap2():
    intvls1 = Intervals([0, 1, 2], [1, 2, 3])
    intvls2 = Intervals([4], [5])
    result = intvls1 | intvls2
    nptest.assert_array_equal([0, 4], result.tstarts)
    nptest.assert_array_equal([3, 5], result.tends)


# --- merge_intervals tests ---


def test_merge_intervals_basic_merge():
    """Two intervals with a gap smaller than d should be merged."""
    intvls = Intervals([0, 3], [2, 5])
    result = intvls.merge_intervals(2)
    nptest.assert_array_equal([0], result.tstarts)
    nptest.assert_array_equal([5], result.tends)


def test_merge_intervals_no_merge_when_gap_ge_d():
    """Gap exactly equal to d should NOT be merged."""
    intvls = Intervals([0, 3], [2, 5])
    result = intvls.merge_intervals(1)
    nptest.assert_array_equal([0, 3], result.tstarts)
    nptest.assert_array_equal([2, 5], result.tends)


def test_merge_intervals_multiple_groups():
    """Multiple groups merge independently."""
    intvls = Intervals([0, 2, 10, 12], [1, 3, 11, 13])
    result = intvls.merge_intervals(2)
    nptest.assert_array_equal([0, 10], result.tstarts)
    nptest.assert_array_equal([3, 13], result.tends)


def test_merge_intervals_empty():
    intvls = Intervals.empty()
    result = intvls.merge_intervals(5)
    assert len(result) == 0


def test_merge_intervals_single_interval():
    intvls = Intervals([1], [2])
    result = intvls.merge_intervals(5)
    nptest.assert_array_equal([1], result.tstarts)
    nptest.assert_array_equal([2], result.tends)


def test_merge_intervals_d_zero():
    """d=0 should not merge non-overlapping intervals."""
    intvls = Intervals([0, 2], [1, 3])
    result = intvls.merge_intervals(0)
    nptest.assert_array_equal([0, 2], result.tstarts)
    nptest.assert_array_equal([1, 3], result.tends)


def test_merge_intervals_already_overlapping():
    """Already-overlapping intervals (negative gap) are merged for any positive d."""
    intvls = Intervals([0, 1.5], [2, 3])
    result = intvls.merge_intervals(0.1)
    nptest.assert_array_equal([0], result.tstarts)
    nptest.assert_array_equal([3], result.tends)


def test_merge_intervals_negative_raises():
    intvls = Intervals([0], [1])
    with pytest.raises(ValueError):
        intvls.merge_intervals(-1)


# --- filter tests ---


def test_filter_basic_removal():
    """Intervals shorter than d are removed."""
    intvls = Intervals([0, 5], [1, 10])
    result = intvls.filter(2)
    nptest.assert_array_equal([5], result.tstarts)
    nptest.assert_array_equal([10], result.tends)


def test_filter_exact_duration_kept():
    """Interval with duration exactly d is kept."""
    intvls = Intervals([0, 5], [2, 10])
    result = intvls.filter(2)
    nptest.assert_array_equal([0, 5], result.tstarts)
    nptest.assert_array_equal([2, 10], result.tends)


def test_filter_mix_short_and_long():
    intvls = Intervals([0, 3, 10, 20], [1, 8, 11, 30])
    result = intvls.filter(2)
    nptest.assert_array_equal([3, 20], result.tstarts)
    nptest.assert_array_equal([8, 30], result.tends)


def test_filter_empty():
    intvls = Intervals.empty()
    result = intvls.filter(5)
    assert len(result) == 0


def test_filter_single_shorter():
    intvls = Intervals([0], [1])
    result = intvls.filter(2)
    assert len(result) == 0


def test_filter_single_longer():
    intvls = Intervals([0], [5])
    result = intvls.filter(2)
    nptest.assert_array_equal([0], result.tstarts)
    nptest.assert_array_equal([5], result.tends)


def test_filter_d_zero():
    """d=0 should keep all intervals."""
    intvls = Intervals([0, 1], [1, 2])
    result = intvls.filter(0)
    nptest.assert_array_equal([0, 1], result.tstarts)
    nptest.assert_array_equal([1, 2], result.tends)


def test_filter_negative_raises():
    intvls = Intervals([0], [1])
    with pytest.raises(ValueError):
        intvls.filter(-1)


def test_filter_all_removed():
    """All intervals shorter than d returns empty."""
    intvls = Intervals([0, 5, 10], [1, 6, 11])
    result = intvls.filter(5)
    assert len(result) == 0


def test_timeseries_op_merge_intervals_merges_close_gaps(mocker):
    """
    (EngSpd > 2000).merge_intervals(2) should merge events whose gap is < 2 s.

    Gap B→C = 1 s < 2  =>  B and C merge into [5, 9).
    Gap A→B = 2 s, NOT < 2  =>  A stays separate.

    Expected: [[1, 3), [5, 9)]
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    expr = (eng_spd > 2000).merge_intervals(2)

    cache = _make_cache(mocker, _eng_spd_series())
    result = expr.build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 2
    nptest.assert_array_equal([1, 5], result.tstarts)
    nptest.assert_array_equal([3, 9], result.tends)


def test_timeseries_op_merge_intervals_no_merge_when_gap_equals_d(mocker):
    """
    (EngSpd > 2000).merge_intervals(1) — gap exactly equal to d is NOT merged.

    Gap A→B = 2 s, Gap B→C = 1 s; with d=1 neither gap is strictly < 1.

    Expected: [[1, 3), [5, 7), [8, 9)]
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    expr = (eng_spd > 2000).merge_intervals(1)

    cache = _make_cache(mocker, _eng_spd_series())
    result = expr.build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 3
    nptest.assert_array_equal([1, 5, 8], result.tstarts)
    nptest.assert_array_equal([3, 7, 9], result.tends)


def test_timeseries_op_filter_removes_short_events(mocker):
    """
    (EngSpd > 2000).filter(2) should drop event C (duration 1 s < 2 s).

    Expected: [[1, 3), [5, 7)]
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    expr = (eng_spd > 2000).filter(2)

    cache = _make_cache(mocker, _eng_spd_series())
    result = expr.build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 2
    nptest.assert_array_equal([1, 5], result.tstarts)
    nptest.assert_array_equal([3, 7], result.tends)


def test_timeseries_op_filter_keeps_exact_duration(mocker):
    """
    (EngSpd > 2000).filter(2) keeps events with duration exactly 2 s.

    A=[1,3) and B=[5,7) both have duration 2 s → both kept.
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    expr = (eng_spd > 2000).filter(2)

    cache = _make_cache(mocker, _eng_spd_series())
    result = expr.build(cache)

    assert len(result) == 2


def test_timeseries_op_merge_intervals_then_filter(mocker):
    """
    (EngSpd > 2000).merge_intervals(2).filter(3) — full pipeline:

    After merge_intervals(2): [[1, 3), [5, 9)]
      durations: 2 s, 4 s
    After filter(3): only [5, 9) (4 s >= 3) survives.

    Expected: [[5, 9)]
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    expr = (eng_spd > 2000).merge_intervals(2).filter(3)

    cache = _make_cache(mocker, _eng_spd_series())
    result = expr.build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 1
    nptest.assert_array_equal([5], result.tstarts)
    nptest.assert_array_equal([9], result.tends)


def test_timeseries_op_merge_intervals_empty_series(mocker):
    """merge_intervals on an empty series returns empty Intervals."""
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    expr = (eng_spd > 2000).merge_intervals(5)

    cache = _make_cache(mocker, SampleSeries.empty())
    result = expr.build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 0


def test_timeseries_op_filter_empty_series(mocker):
    """filter on an empty series returns empty Intervals."""
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    expr = (eng_spd > 2000).filter(1)

    cache = _make_cache(mocker, SampleSeries.empty())
    result = expr.build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 0


def test_timeseries_op_merge_intervals_returns_intervals_type(mocker):
    """Result of .merge_intervals() chained on a comparison expression is an Intervals."""
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    expr = (eng_spd > 2000).merge_intervals(5)

    cache = _make_cache(mocker, _eng_spd_series())
    result = expr.build(cache)

    assert isinstance(result, Intervals)


def test_timeseries_op_filter_returns_intervals_type(mocker):
    """Result of .filter() chained on a comparison expression is an Intervals."""
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    expr = (eng_spd > 2000).filter(1)

    cache = _make_cache(mocker, _eng_spd_series())
    result = expr.build(cache)

    assert isinstance(result, Intervals)


# ---------------------------------------------------------------------------
# BasicEvent with debounce / filter
# ---------------------------------------------------------------------------
#
# Signal layout (_eng_spd_series):
#
#   t:   0  1  2  3  4  5  6  7  8  9  10
#   rpm: 1000 2500 2500 1000 1000 2500 2500 500 2500 500
#
#   Raw events (EngSpd > 2000):
#     A: [1, 3)  – 2 s
#     B: [5, 7)  – 2 s
#     C: [8, 9)  – 1 s
#
#   Gap A→B = 2 s, Gap B→C = 1 s


def test_basic_event_merge_intervals_merges_close_gaps(mocker):
    """
    BasicEvent("EngHigh", (EngSpd > 2000).merge_intervals(2))

    Gap B→C = 1 s < 2  =>  B and C merge into [5, 9).
    Gap A→B = 2 s, NOT < 2  =>  A stays separate.

    Expected: [[1, 3), [5, 9)]
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    event = BasicEvent("EngHigh", (eng_spd > 2000).merge_intervals(2))

    cache = _make_cache(mocker, _eng_spd_series())
    result = event.get_expression().build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 2
    nptest.assert_array_equal([1, 5], result.tstarts)
    nptest.assert_array_equal([3, 9], result.tends)


def test_basic_event_merge_intervals_no_merge_when_gap_equals_d(mocker):
    """
    BasicEvent("EngHigh", (EngSpd > 2000).merge_intervals(1))

    Gap B→C = 1 s, which is NOT strictly < 1  =>  no merge.

    Expected: [[1, 3), [5, 7), [8, 9)]
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    event = BasicEvent("EngHigh", (eng_spd > 2000).merge_intervals(1))

    cache = _make_cache(mocker, _eng_spd_series())
    result = event.get_expression().build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 3
    nptest.assert_array_equal([1, 5, 8], result.tstarts)
    nptest.assert_array_equal([3, 7, 9], result.tends)


def test_basic_event_filter_removes_short_events(mocker):
    """
    BasicEvent("EngHigh", (EngSpd > 2000).filter(2))

    Event C has duration 1 s < 2 s  =>  dropped.

    Expected: [[1, 3), [5, 7)]
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    event = BasicEvent("EngHigh", (eng_spd > 2000).filter(2))

    cache = _make_cache(mocker, _eng_spd_series())
    result = event.get_expression().build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 2
    nptest.assert_array_equal([1, 5], result.tstarts)
    nptest.assert_array_equal([3, 7], result.tends)


def test_basic_event_filter_keeps_exact_duration(mocker):
    """
    BasicEvent("EngHigh", (EngSpd > 2000).filter(2))

    Events A and B both have duration exactly 2 s  =>  both kept.
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    event = BasicEvent("EngHigh", (eng_spd > 2000).filter(2))

    cache = _make_cache(mocker, _eng_spd_series())
    result = event.get_expression().build(cache)

    assert len(result) == 2


def test_basic_event_merge_intervals_then_filter(mocker):
    """
    BasicEvent("EngHigh", (EngSpd > 2000).merge_intervals(2).filter(3))

    After merge_intervals(2): [[1, 3), [5, 9)]   durations: 2 s, 4 s
    After filter(3):       only [5, 9) survives (4 s >= 3 s).

    Expected: [[5, 9)]
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    event = BasicEvent("EngHigh", (eng_spd > 2000).merge_intervals(2).filter(3))

    cache = _make_cache(mocker, _eng_spd_series())
    result = event.get_expression().build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 1
    nptest.assert_array_equal([5], result.tstarts)
    nptest.assert_array_equal([9], result.tends)


def test_basic_event_filter_then_merge_intervals(mocker):
    """
    BasicEvent("EngHigh", (EngSpd > 2000).filter(2).merge_intervals(3))

    After filter(2):        drop C (1 s)  =>  [[1, 3), [5, 7)]
    After merge_intervals(3):  gap A→B = 2 s < 3  =>  merge into [[1, 7)]

    Expected: [[1, 7)]
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    event = BasicEvent("EngHigh", (eng_spd > 2000).filter(2).merge_intervals(3))

    cache = _make_cache(mocker, _eng_spd_series())
    result = event.get_expression().build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 1
    nptest.assert_array_equal([1], result.tstarts)
    nptest.assert_array_equal([7], result.tends)


def test_basic_event_merge_intervals_empty_series(mocker):
    """BasicEvent with merge_intervals on an empty series returns empty Intervals."""
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    event = BasicEvent("EngHigh", (eng_spd > 2000).merge_intervals(5))

    cache = _make_cache(mocker, SampleSeries.empty())
    result = event.get_expression().build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 0


def test_basic_event_filter_empty_series(mocker):
    """BasicEvent with filter on an empty series returns empty Intervals."""
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    event = BasicEvent("EngHigh", (eng_spd > 2000).filter(1))

    cache = _make_cache(mocker, SampleSeries.empty())
    result = event.get_expression().build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 0


def test_basic_event_has_correct_name():
    """BasicEvent stores the name correctly."""
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    event = BasicEvent("EngHighDebounced", (eng_spd > 2000).debounce(5))
    assert event.name == "EngHighDebounced"


def test_basic_event_expression_str_contains_merge_intervals():
    """BasicEvent expression string representation includes merge_intervals."""
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    event = BasicEvent("EngHigh", (eng_spd > 2000).merge_intervals(5))
    assert "merge_intervals" in event.get_expression_str()


def test_basic_event_expression_str_contains_filter():
    """BasicEvent expression string representation includes filter."""
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    event = BasicEvent("EngHigh", (eng_spd > 2000).filter(2))
    assert "filter" in event.get_expression_str()


# ---------------------------------------------------------------------------
# debounce tests (sustaining / state-machine semantics)
# ---------------------------------------------------------------------------


def test_debounce_discards_isolated_short_intervals():
    """Short intervals with no prior sustained interval are discarded."""
    intvls = Intervals([0, 5], [2, 7])  # durations: 2, 2 (both < 3)
    result = intvls.debounce(3)
    assert len(result) == 0


def test_debounce_keeps_single_long_interval():
    """A single long interval (duration >= d) is preserved unchanged."""
    intvls = Intervals([0], [5])  # duration 5 >= 3
    result = intvls.debounce(3)
    nptest.assert_array_equal([0], result.tstarts)
    nptest.assert_array_equal([5], result.tends)


def test_debounce_absorbs_short_after_confirmed():
    """Short interval within tolerance after a confirmed event extends the event."""
    # Long [0,4) confirmed, then short [5,6) at gap=1 < 3 → absorbed
    intvls = Intervals([0, 5], [4, 6])
    result = intvls.debounce(3)
    nptest.assert_array_equal([0], result.tstarts)
    nptest.assert_array_equal([6], result.tends)


def test_debounce_discards_short_after_large_gap():
    """Short interval beyond tolerance from a confirmed event is discarded."""
    # Long [0,4) confirmed, then short [10,11) at gap=6 >= 3 → discard
    intvls = Intervals([0, 10], [4, 11])
    result = intvls.debounce(3)
    nptest.assert_array_equal([0], result.tstarts)
    nptest.assert_array_equal([4], result.tends)


def test_debounce_chain_of_short_not_promoted():
    """Multiple short pulses close together are all discarded (no confirmation)."""
    # durations: all 2, gaps: all 1 — no interval reaches duration >= 3
    intvls = Intervals([0, 3, 6, 9], [2, 5, 8, 11])
    result = intvls.debounce(3)
    assert len(result) == 0


def test_debounce_empty():
    """debounce on empty Intervals returns empty."""
    result = Intervals.empty().debounce(5)
    assert len(result) == 0


def test_debounce_negative_raises():
    """Negative d raises ValueError."""
    with pytest.raises(ValueError):
        Intervals([0], [1]).debounce(-1)


def test_debounce_d_zero():
    """d=0: every interval is long (duration >= 0), gaps >= 0 separate events."""
    intvls = Intervals([0, 2], [1, 3])  # gap = 1 >= 0
    result = intvls.debounce(0)
    nptest.assert_array_equal([0, 2], result.tstarts)
    nptest.assert_array_equal([1, 3], result.tends)


def test_debounce_long_short_long_within_tolerance():
    """Long → short (gap < d) → long (gap < d): all three merge into one block."""
    # [0,4): dur=4>=3, confirmed; [5,6): gap=1<3, absorbed; [7,11): gap=1<3, absorbed
    intvls = Intervals([0, 5, 7], [4, 6, 11])
    result = intvls.debounce(3)
    nptest.assert_array_equal([0], result.tstarts)
    nptest.assert_array_equal([11], result.tends)


def test_debounce_second_long_interval_starts_new_event():
    """Second long interval separated by gap >= d finalises the first and starts new."""
    # [0,4) gap=10 [14,18) — gap 10 >= 3 → two separate events
    intvls = Intervals([0, 14], [4, 18])
    result = intvls.debounce(3)
    nptest.assert_array_equal([0, 14], result.tstarts)
    nptest.assert_array_equal([4, 18], result.tends)


def test_debounce_ascii_example_scenario():
    """
    Encodes the ASCII example from implementation_plan.md (d=3).

    Raw signal intervals:
      [8,10), [12,14), [16,31), [35,36), [40,44), [46,50), [52,54), [56,59)

    merge_intervals(3) → 3 events: [8,31), [35,36), [40,59)
    filter(3)       → 4 events: [16,31), [40,44), [46,50), [56,59)
    debounce(3)     → 2 events: [16,31), [40,59)
    """
    intvls = Intervals(
        [8, 12, 16, 35, 40, 46, 52, 56],
        [10, 14, 31, 36, 44, 50, 54, 59],
    )

    # merge_intervals(3): bridge gaps < 3
    merged = intvls.merge_intervals(3)
    assert len(merged) == 3
    nptest.assert_array_equal([8, 35, 40], merged.tstarts)
    nptest.assert_array_equal([31, 36, 59], merged.tends)

    # filter(3): drop intervals with duration < 3
    filtered = intvls.filter(3)
    assert len(filtered) == 4
    nptest.assert_array_equal([16, 40, 46, 56], filtered.tstarts)
    nptest.assert_array_equal([31, 44, 50, 59], filtered.tends)

    # debounce(3): sustaining state-machine → only 2 events
    debounced = intvls.debounce(3)
    assert len(debounced) == 2
    nptest.assert_array_equal([16, 40], debounced.tstarts)
    nptest.assert_array_equal([31, 59], debounced.tends)


# ---------------------------------------------------------------------------
# Expression-layer debounce tests (new sustaining semantics)
# ---------------------------------------------------------------------------
#
# Signal layout (_eng_spd_series):
#
#   t:   0  1  2  3  4  5  6  7  8  9  10
#   rpm: 1000 2500 2500 1000 1000 2500 2500 500 2500 500
#
#   Raw events (EngSpd > 2000):
#     A: [1, 3)  – duration 2 s
#     B: [5, 7)  – duration 2 s
#     C: [8, 9)  – duration 1 s
#
# New debounce(d) with sustaining semantics:
#   debounce(1): every interval is long (dur >= 1), gaps >= 1 separate them
#                → 3 events: [[1,3), [5,7), [8,9)]
#   debounce(2): A dur=2>=2 confirmed, gap A→B=2>=2 → finalize A start new B
#                B dur=2>=2 confirmed, gap B→C=1<2 → extend to [5,9)
#                → 2 events: [[1,3), [5,9)]
#   debounce(3): A,B dur=2<3 → both discarded (no confirmed); C dur=1<3 → discarded
#                → 0 events


def test_timeseries_op_debounce_sustaining_all_short(mocker):
    """
    (EngSpd > 2000).debounce(3): all raw events have duration < 3 s → empty result.
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    expr = (eng_spd > 2000).debounce(3)

    cache = _make_cache(mocker, _eng_spd_series())
    result = expr.build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 0


def test_timeseries_op_debounce_sustaining_two_per_d2(mocker):
    """
    (EngSpd > 2000).debounce(2): A confirmed, B confirmed and absorbs C.

    A=[1,3) dur=2>=2 → confirmed; gap A→B=2>=2 → new event
    B=[5,7) dur=2>=2 → confirmed; gap B→C=1<2  → C absorbed → [5,9)

    Expected: [[1,3), [5,9)]
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    expr = (eng_spd > 2000).debounce(2)

    cache = _make_cache(mocker, _eng_spd_series())
    result = expr.build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 2
    nptest.assert_array_equal([1, 5], result.tstarts)
    nptest.assert_array_equal([3, 9], result.tends)


def test_timeseries_op_debounce_sustaining_returns_intervals_type(mocker):
    """Result of .debounce() on a comparison expression is an Intervals."""
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    expr = (eng_spd > 2000).debounce(2)

    cache = _make_cache(mocker, _eng_spd_series())
    result = expr.build(cache)

    assert isinstance(result, Intervals)


def test_timeseries_op_debounce_empty_series(mocker):
    """debounce on an empty series returns empty Intervals."""
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    expr = (eng_spd > 2000).debounce(2)

    cache = _make_cache(mocker, SampleSeries.empty())
    result = expr.build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 0


def test_basic_event_debounce_sustaining_semantics(mocker):
    """
    BasicEvent with new debounce(2): A confirmed separately, B absorbs C.

    Expected: [[1, 3), [5, 9)]
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    event = BasicEvent("EngHigh", (eng_spd > 2000).debounce(2))

    cache = _make_cache(mocker, _eng_spd_series())
    result = event.get_expression().build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 2
    nptest.assert_array_equal([1, 5], result.tstarts)
    nptest.assert_array_equal([3, 9], result.tends)


def test_basic_event_debounce_all_discarded(mocker):
    """
    BasicEvent with debounce(3): all raw events shorter than 3 s → empty.
    """
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    event = BasicEvent("EngHigh", (eng_spd > 2000).debounce(3))

    cache = _make_cache(mocker, _eng_spd_series())
    result = event.get_expression().build(cache)

    assert isinstance(result, Intervals)
    assert len(result) == 0


def test_basic_event_expression_str_contains_debounce():
    """BasicEvent expression string representation includes debounce."""
    eng_spd = TimeSeriesSelector(TagSelector("channel_name") == "EngSpd")
    event = BasicEvent("EngHigh", (eng_spd > 2000).debounce(2))
    assert "debounce" in event.get_expression_str()


def test_plane_sweep_intervals_and_intervals():
    """Test plane_sweep with two Intervals objects."""
    intervals1 = Intervals([0, 1, 2], [1, 2, 3])
    intervals2 = Intervals([0, 1, 2], [1, 2, 3])

    result = Intervals.plane_sweep(intervals1, intervals2)
    expected = [(0, 0), (1, 1), (2, 2)]
    assert result == expected


def test_plane_sweep_intervals_and_points_in_time():
    """Test plane_sweep with intervals and points in time."""
    intervals1 = Intervals([0, 1, 2], [1, 2, 3])
    points_in_time = PointsInTime([0.9, 1.0, 2.0])

    result = Intervals.plane_sweep(intervals1, points_in_time)
    expected = [(0, 0), (1, 1), (2, 2)]
    assert result == expected
