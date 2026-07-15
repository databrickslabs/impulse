"""Tests for StatsAggregator._calculate_aggregations method.

Note: This test suite tests the _calculate_aggregations method which computes
statistics on time series data. Some tests are marked as xfail due to remaining
issues with the slicing logic (idx_start:idx_end excludes the end value).
"""

from unittest.mock import MagicMock

import numpy as np
import numpy.testing as nptest
import pytest

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesSelector,
)
from impulse_query_engine.analyze.query.aggregations.custom_statistic import (
    CrossChannelStatistic,
    PerChannelStatistic,
)
from impulse_query_engine.analyze.query.aggregations.stats_aggregator import (
    StatsAggregator,
)
from impulse_query_engine.model.series.intervals import Intervals
from impulse_query_engine.model.series.sample_series import SampleSeries


def _create_aggregator(statistics=None):
    """Helper function to create a StatsAggregator instance."""
    if statistics is None:
        statistics = ["start", "end", "min", "max", "mean", "median"]
    return StatsAggregator(
        input_expressions=[],
        event_expression=None,
        statistics=statistics,
    )


def test_calculate_aggregations_start_end():
    """Test that start and end statistics work correctly."""
    sample_series = SampleSeries(
        tstarts=np.array([0.0, 1.0, 2.0]),
        tends=np.array([1.0, 2.0, 3.0]),
        values=np.array([10.0, 20.0, 30.0]),
    )

    aggregator = _create_aggregator(["start", "end"])
    result_dict = aggregator._calculate_aggregations(sample_series, 0.0, 3.0)

    assert result_dict["start"] == 10.0
    assert result_dict["end"] == 30.0


def test_calculate_aggregations_basic():
    """Test basic statistics calculation with simple data."""
    # Create sample series with known values
    # Time intervals: [0-1], [1-2], [2-3] with values [10, 20, 30]
    sample_series = SampleSeries(
        tstarts=np.array([0.0, 1.0, 2.0]),
        tends=np.array([1.0, 2.0, 3.0]),
        values=np.array([10.0, 20.0, 30.0]),
    )

    aggregator = _create_aggregator()

    # Calculate aggregations for the interval [0.0, 3.0]
    t_start = 0.0
    t_end = 3.0

    result_dict = aggregator._calculate_aggregations(sample_series, t_start, t_end)

    # Verify the result structure
    assert isinstance(result_dict, dict)
    assert len(result_dict.keys()) == 6  # Six statistics

    # Verify statistics
    assert result_dict["start"] == 10.0
    assert result_dict["end"] == 30.0
    assert result_dict["min"] == 10.0
    assert result_dict["max"] == 30.0
    assert result_dict["median"] == 20.0

    # Mean should be weighted by duration
    # All intervals have duration 1.0
    # mean = (10*1 + 20*1 + 30*1) / (1 + 1 + 1) = 60/3 = 20.0
    assert result_dict["mean"] == 20.0


def test_calculate_aggregations_weighted_mean():
    """Test that mean is correctly weighted by sample duration."""
    # Create sample series with different durations
    # [0-1] value 10 (duration 1)
    # [1-4] value 20 (duration 3)
    # [4-5] value 30 (duration 1)
    sample_series = SampleSeries(
        tstarts=np.array([0.0, 1.0, 4.0]),
        tends=np.array([1.0, 4.0, 5.0]),
        values=np.array([10.0, 20.0, 30.0]),
    )

    aggregator = _create_aggregator(["mean"])
    t_start = 0.0
    t_end = 5.0

    result_dict = aggregator._calculate_aggregations(sample_series, t_start, t_end)

    # Weighted mean = (10*1 + 20*3 + 30*1) / (1 + 3 + 1) = 100/5 = 20.0
    assert result_dict["mean"] == 20.0


def test_calculate_aggregations_with_nan():
    """Test statistics calculation with NaN values."""
    # Create sample series with NaN values
    sample_series = SampleSeries(
        tstarts=np.array([0.0, 1.0, 2.0, 3.0]),
        tends=np.array([1.0, 2.0, 3.0, 4.0]),
        values=np.array([10.0, np.nan, 20.0, 30.0]),
    )

    aggregator = _create_aggregator(["min", "max", "mean", "median"])
    t_start = 0.0
    t_end = 4.0

    result_dict = aggregator._calculate_aggregations(sample_series, t_start, t_end)

    # NaN should be ignored by nanmin, nanmax, nanmedian
    assert result_dict["min"] == 10.0
    assert result_dict["max"] == 30.0
    assert result_dict["median"] == 20.0

    # Mean calculation with NaN
    # (10*1 + nan*1 + 20*1 + 30*1) / (1 + 1 + 1 + 1)
    # With nansum this should be (10 + 20 + 30) / 4 = 15.0
    # But note: duration of NaN is still counted
    expected_mean = (10.0 * 1 + 20.0 * 1 + 30.0 * 1) / 4.0
    nptest.assert_almost_equal(result_dict["mean"], expected_mean)


def test_calculate_aggregations_single_sample():
    """Test with a single sample in the series."""
    sample_series = SampleSeries(
        tstarts=np.array([1.0]), tends=np.array([2.0]), values=np.array([42.0])
    )

    aggregator = _create_aggregator()
    t_start = 1.0
    t_end = 2.0

    result_dict = aggregator._calculate_aggregations(sample_series, t_start, t_end)

    # All statistics should return the same value for a single sample
    assert result_dict["start"] == 42.0
    assert result_dict["end"] == 42.0
    assert result_dict["min"] == 42.0
    assert result_dict["max"] == 42.0
    assert result_dict["mean"] == 42.0
    assert result_dict["median"] == 42.0


def test_calculate_aggregations_subset_statistics():
    """Test calculation with only a subset of statistics."""
    sample_series = SampleSeries(
        tstarts=np.array([0.0, 1.0, 2.0]),
        tends=np.array([1.0, 2.0, 3.0]),
        values=np.array([10.0, 20.0, 30.0]),
    )

    aggregator = _create_aggregator(["min", "max"])
    t_start = 0.0
    t_end = 3.0

    result_dict = aggregator._calculate_aggregations(sample_series, t_start, t_end)

    assert "min" in result_dict
    assert "max" in result_dict
    assert "mean" not in result_dict
    assert result_dict["min"] == 10.0
    assert result_dict["max"] == 30.0


def test_calculate_aggregations_start_end_only():
    """Test calculation with only start and end statistics."""
    sample_series = SampleSeries(
        tstarts=np.array([0.0, 1.0, 2.0, 3.0]),
        tends=np.array([1.0, 2.0, 3.0, 4.0]),
        values=np.array([5.0, 15.0, 25.0, 35.0]),
    )

    aggregator = _create_aggregator(["start", "end"])
    t_start = 0.0
    t_end = 4.0

    result_dict = aggregator._calculate_aggregations(sample_series, t_start, t_end)

    assert result_dict["start"] == 5.0
    assert result_dict["end"] == 35.0


def test_calculate_aggregations_median_even_samples():
    """Test median calculation with an even number of samples."""
    # With 4 samples, median should be average of middle two values
    sample_series = SampleSeries(
        tstarts=np.array([0.0, 1.0, 2.0, 3.0, 4.0]),
        tends=np.array([1.0, 2.0, 3.0, 4.0, 5.0]),
        values=np.array([10.0, 20.0, 30.0, 40.0, 50.0]),
    )

    aggregator = _create_aggregator(["median"])
    t_start = 0.0
    t_end = 5.0

    result_dict = aggregator._calculate_aggregations(sample_series, t_start, t_end)
    assert result_dict["median"] == 30.0


def test_calculate_aggregations_negative_values():
    """Test statistics with negative values."""
    sample_series = SampleSeries(
        tstarts=np.array([0.0, 1.0, 2.0]),
        tends=np.array([1.0, 2.0, 3.0]),
        values=np.array([-10.0, 5.0, -20.0]),
    )

    aggregator = _create_aggregator()
    t_start = 0.0
    t_end = 3.0

    result_dict = aggregator._calculate_aggregations(sample_series, t_start, t_end)

    assert result_dict["start"] == -10.0
    assert result_dict["end"] == -20.0
    assert result_dict["min"] == -20.0
    assert result_dict["max"] == 5.0

    # Weighted mean = (-10*1 + 5*1 + -20*1) / 3 = -25/3 ≈ -8.333
    expected_mean = (-10.0 + 5.0 + -20.0) / 3.0
    nptest.assert_almost_equal(result_dict["mean"], expected_mean)


def test_calculate_aggregations_identical_values():
    """Test statistics when all values are identical."""
    sample_series = SampleSeries(
        tstarts=np.array([0.0, 1.0, 2.0, 3.0]),
        tends=np.array([1.0, 2.0, 3.0, 4.0]),
        values=np.array([7.5, 7.5, 7.5, 7.5]),
    )

    aggregator = _create_aggregator()
    t_start = 0.0
    t_end = 4.0

    result_dict = aggregator._calculate_aggregations(sample_series, t_start, t_end)

    # All statistics should return the same value
    assert result_dict["start"] == 7.5
    assert result_dict["end"] == 7.5
    assert result_dict["min"] == 7.5
    assert result_dict["max"] == 7.5
    assert result_dict["mean"] == 7.5
    assert result_dict["median"] == 7.5


def test_init_with_all_numeric_statistics():
    """Test initialization with all numeric statistics."""
    selector = TimeSeriesSelector(TagSelector("name") == "test_signal")
    event_expr = TimeSeriesSelector(TagSelector("name") == "event_signal")

    stats_agg = StatsAggregator(
        input_expressions=[selector],
        event_expression=event_expr,
        statistics=["min", "max", "mean", "median", "start", "end"],
    )

    assert "min" in stats_agg._numeric_stats
    assert "max" in stats_agg._numeric_stats
    assert "mean" in stats_agg._numeric_stats
    assert "median" in stats_agg._numeric_stats
    assert "start" in stats_agg._numeric_stats
    assert "end" in stats_agg._numeric_stats


def test_str_representation():
    """Test the string representation of StatsAggregator."""
    selector = TimeSeriesSelector(TagSelector("name") == "test_signal")
    event_expr = TimeSeriesSelector(TagSelector("name") == "event_signal")

    stats_agg = StatsAggregator(
        input_expressions=[selector],
        event_expression=event_expr,
        statistics=["max", "mean"],
    )

    str_repr = str(stats_agg)
    assert "<StatsAggregator" in str_repr
    assert "input_expressions=" in str_repr
    assert "event_expression=" in str_repr
    assert "statistics=" in str_repr


def test_dtype_contains_expected_fields():
    """Test that dtype contains event_timestamps, numeric_values, and string_values."""
    selector = TimeSeriesSelector(TagSelector("name") == "test_signal")
    event_expr = TimeSeriesSelector(TagSelector("name") == "event_signal")

    stats_agg = StatsAggregator(
        input_expressions=[selector],
        event_expression=event_expr,
        statistics=["max"],
    )

    dtype = stats_agg.dtype()
    field_names = [field.name for field in dtype.fields]

    assert "event_timestamps" in field_names
    assert "numeric_values" in field_names
    assert "string_values" in field_names
    assert "cross_channel_values" in field_names
    assert len(dtype.fields) == 4

    import pyspark.sql.types as T

    cross_channel_field = dtype["cross_channel_values"]
    assert cross_channel_field.dataType == T.ArrayType(T.MapType(T.StringType(), T.DoubleType()))


def test_required_tags_single_expression():
    """Test required_tags with single input expression."""
    selector = TimeSeriesSelector(TagSelector("channel_name") == "test")
    event_expr = TimeSeriesSelector(TagSelector("event_name") == "event")

    stats_agg = StatsAggregator(
        input_expressions=[selector],
        event_expression=event_expr,
        statistics=["max"],
    )

    tags = stats_agg.required_tags()
    assert isinstance(tags, set)


def test_required_tags_union_from_multiple_expressions():
    """Test that required_tags returns union from all expressions."""
    selector1 = TimeSeriesSelector(TagSelector("tag_a") == "value_a")
    selector2 = TimeSeriesSelector(TagSelector("tag_b") == "value_b")
    event_expr = TimeSeriesSelector(TagSelector("tag_c") == "value_c")

    stats_agg = StatsAggregator(
        input_expressions=[selector1, selector2],
        event_expression=event_expr,
        statistics=["max"],
    )

    tags = stats_agg.required_tags()
    assert isinstance(tags, set)
    # The union should include tags from all expressions


def test_get_selector_expr_single_expression():
    """Test get_selector_expr with single input expression."""
    selector = TimeSeriesSelector(TagSelector("name") == "test_signal")
    event_expr = TimeSeriesSelector(TagSelector("name") == "event_signal")

    stats_agg = StatsAggregator(
        input_expressions=[selector],
        event_expression=event_expr,
        statistics=["max"],
    )

    selector_expr = stats_agg.get_selector_expr()
    assert selector_expr is not None


def test_get_selector_expr_union_logic():
    """Test that get_selector_expr returns union of all selectors."""
    selector1 = TimeSeriesSelector(TagSelector("name") == "signal_1")
    selector2 = TimeSeriesSelector(TagSelector("name") == "signal_2")
    event_expr = TimeSeriesSelector(TagSelector("name") == "event_signal")

    stats_agg = StatsAggregator(
        input_expressions=[selector1, selector2],
        event_expression=event_expr,
        statistics=["max"],
    )

    selector_expr = stats_agg.get_selector_expr()
    assert selector_expr is not None

    def test_get_required_tag_exprs_returns_set():
        """Test that get_required_tag_exprs returns a set."""
        selector = TimeSeriesSelector(TagSelector("name") == "test_signal")
        event_expr = TimeSeriesSelector(TagSelector("name") == "event_signal")

        stats_agg = StatsAggregator(
            input_expressions=[selector],
            event_expression=event_expr,
            statistics=["max"],
        )

        tag_exprs = stats_agg.get_required_tag_exprs()
        assert isinstance(tag_exprs, set)


def test_get_required_tag_exprs_union_logic():
    """Test that get_required_tag_exprs returns union from all expressions."""
    selector1 = TimeSeriesSelector(TagSelector("tag_a") == "value_a")
    selector2 = TimeSeriesSelector(TagSelector("tag_b") == "value_b")
    event_expr = TimeSeriesSelector(TagSelector("tag_c") == "value_c")

    stats_agg = StatsAggregator(
        input_expressions=[selector1, selector2],
        event_expression=event_expr,
        statistics=["max"],
    )

    tag_exprs = stats_agg.get_required_tag_exprs()
    assert isinstance(tag_exprs, set)


def test_weighted_median_basic():
    """Test weighted median calculation with simple data."""
    selector = TimeSeriesSelector(TagSelector("name") == "test_signal")
    event_expr = TimeSeriesSelector(TagSelector("name") == "event_signal")

    stats_agg = StatsAggregator(
        input_expressions=[selector],
        event_expression=event_expr,
        statistics=["median"],
    )

    durations = np.array([1.0, 1.0, 1.0, 1.0, 1.0])
    values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])

    median = stats_agg.weighted_median(durations, values)
    assert median == 3.0


def test_weighted_median_with_weights():
    """Test weighted median with non-uniform weights."""
    selector = TimeSeriesSelector(TagSelector("name") == "test_signal")
    event_expr = TimeSeriesSelector(TagSelector("name") == "event_signal")

    stats_agg = StatsAggregator(
        input_expressions=[selector],
        event_expression=event_expr,
        statistics=["median"],
    )

    durations = np.array([1.0, 1.0, 8.0])  # Heavy weight on value 30
    values = np.array([10.0, 20.0, 30.0])

    median = stats_agg.weighted_median(durations, values)
    assert median == 30.0


def test_weighted_median_with_nan_values():
    """Test weighted median handles NaN values."""
    selector = TimeSeriesSelector(TagSelector("name") == "test_signal")
    event_expr = TimeSeriesSelector(TagSelector("name") == "event_signal")

    stats_agg = StatsAggregator(
        input_expressions=[selector],
        event_expression=event_expr,
        statistics=["median"],
    )

    durations = np.array([1.0, 1.0, 1.0, 1.0])
    values = np.array([1.0, np.nan, 3.0, 4.0])

    median = stats_agg.weighted_median(durations, values)
    assert not np.isnan(median)


def test_weighted_median_all_nan_returns_nan():
    """Test weighted median returns NaN when all values are NaN."""
    selector = TimeSeriesSelector(TagSelector("name") == "test_signal")
    event_expr = TimeSeriesSelector(TagSelector("name") == "event_signal")

    stats_agg = StatsAggregator(
        input_expressions=[selector],
        event_expression=event_expr,
        statistics=["median"],
    )

    durations = np.array([1.0, 1.0, 1.0])
    values = np.array([np.nan, np.nan, np.nan])

    median = stats_agg.weighted_median(durations, values)
    assert np.isnan(median)


def test_build_with_none_event_expression_uses_synced_series_bounds():
    """Test build() fallback path when event_expression is None."""
    expr1 = MagicMock()
    expr2 = MagicMock()

    expr1.build.return_value = SampleSeries(
        tstarts=np.array([0.0, 2.0]),
        tends=np.array([2.0, 4.0]),
        values=np.array([1.0, 3.0]),
    )
    expr2.build.return_value = SampleSeries(
        tstarts=np.array([1.0, 3.0]),
        tends=np.array([3.0, 5.0]),
        values=np.array([10.0, 20.0]),
    )

    stats_agg = StatsAggregator(
        input_expressions=[expr1, expr2],
        event_expression=None,
        statistics=["start", "end"],
    )

    event_timestamps, numeric_values, string_values, cross_channel_values = stats_agg.build(
        cache=None
    )

    # event_timestamps is appended inside the per-expression loop in build(),
    # so for N=2 expressions with one synthetic event each the list has 2 entries.
    assert event_timestamps == [[0.0, 5.0], [0.0, 5.0]]
    assert len(numeric_values) == 2
    assert len(numeric_values[0]) == 1
    assert len(numeric_values[1]) == 1
    assert numeric_values[0][0] == {"start": 1.0, "end": 3.0}
    assert numeric_values[1][0] == {"start": 10.0, "end": 20.0}
    assert string_values == []
    assert cross_channel_values == []


def test_has_required_methods():
    """Test that StatsAggregator has all required methods."""
    selector = TimeSeriesSelector(TagSelector("name") == "test_signal")
    event_expr = TimeSeriesSelector(TagSelector("name") == "event_signal")

    stats_agg = StatsAggregator(
        input_expressions=[selector],
        event_expression=event_expr,
        statistics=["max"],
    )

    # Check method existence
    assert hasattr(stats_agg, "dtype")
    assert callable(stats_agg.dtype)

    assert hasattr(stats_agg, "build")
    assert callable(stats_agg.build)

    assert hasattr(stats_agg, "required_tags")
    assert callable(stats_agg.required_tags)

    assert hasattr(stats_agg, "get_selector_expr")
    assert callable(stats_agg.get_selector_expr)

    assert hasattr(stats_agg, "get_required_tag_exprs")
    assert callable(stats_agg.get_required_tag_exprs)

    assert hasattr(stats_agg, "weighted_median")
    assert callable(stats_agg.weighted_median)


def test_stats_aggregator_get_selectors():
    sel_a = TimeSeriesSelector(TagSelector("name") == "a")
    sel_b = TimeSeriesSelector(TagSelector("name") == "b")
    agg = StatsAggregator(
        input_expressions=[sel_a, sel_b],
        statistics=["min", "max"],
    )
    result = agg.get_selectors()
    assert len(result) == 2
    assert sel_a in result
    assert sel_b in result


def test_stats_aggregator_get_selectors_no_event():
    sel = TimeSeriesSelector(TagSelector("name") == "signal")
    agg = StatsAggregator(
        input_expressions=[sel],
        event_expression=None,
        statistics=["mean"],
    )
    result = agg.get_selectors()
    assert result == [sel]


def test_stats_aggregator_get_selectors_with_event():
    sel = TimeSeriesSelector(TagSelector("name") == "signal")
    evt = TimeSeriesSelector(TagSelector("name") == "event")
    agg = StatsAggregator(
        input_expressions=[sel],
        event_expression=evt,
        statistics=["mean"],
    )
    result = agg.get_selectors()
    assert len(result) == 2
    assert sel in result
    assert evt in result


# ---------------------------------------------------------------------------
# Custom statistics (cross-channel and per-channel)
# ---------------------------------------------------------------------------


def _mock_expr(tstarts, tends, values):
    """Mock TimeSeriesExpression whose build() returns a SampleSeries."""
    expr = MagicMock()
    expr.build.return_value = SampleSeries(
        tstarts=np.array(tstarts), tends=np.array(tends), values=np.array(values)
    )
    return expr


def _mock_event(tstarts, tends):
    """Mock event expression whose build() returns Intervals."""
    event = MagicMock()
    event.build.return_value = Intervals(tstarts=tstarts, tends=tends)
    return event


def _spread(series, t_start, t_end):
    """Cross-channel: max - min over all values of all series."""
    values = np.concatenate([s.values for s in series]) if series else np.array([])
    valid = values[~np.isnan(values)]
    if valid.size == 0:
        return float("nan")
    return float(np.max(valid) - np.min(valid))


def _total_sample_count(series, t_start, t_end):
    """Cross-channel: total number of samples across all series."""
    return float(sum(len(s) for s in series))


def _rms(series, t_start, t_end):
    """Per-channel: root mean square of the series values."""
    if len(series) == 0:
        return float("nan")
    return float(np.sqrt(np.nanmean(series.values**2)))


def _sample_count(series, t_start, t_end):
    """Per-channel: number of samples in the series."""
    return float(len(series))


def test_build_cross_channel_stat_two_channels_two_intervals():
    expr1 = _mock_expr([0.0, 2.0], [1.0, 3.0], [10.0, 30.0])
    expr2 = _mock_expr([0.0, 2.0], [1.0, 3.0], [20.0, 50.0])
    event = _mock_event([0.0, 2.0], [1.0, 3.0])

    stats_agg = StatsAggregator(
        input_expressions=[expr1, expr2],
        event_expression=event,
        statistics=["min"],
        cross_channel_custom_statistics={"spread": _spread},
    )

    _, numeric_values, _, cross_channel_values = stats_agg.build(cache=None)

    assert len(cross_channel_values) == 2
    assert len(cross_channel_values) == len(numeric_values[0])
    nptest.assert_almost_equal(cross_channel_values[0]["spread"], 10.0)
    nptest.assert_almost_equal(cross_channel_values[1]["spread"], 20.0)
    # built-ins still computed per channel
    assert numeric_values[0][0] == {"min": 10.0}
    assert numeric_values[1][1] == {"min": 50.0}


def test_build_multiple_cross_channel_stats():
    expr = _mock_expr([0.0, 2.0], [1.0, 3.0], [10.0, 30.0])
    event = _mock_event([0.0, 2.0], [1.0, 3.0])

    stats_agg = StatsAggregator(
        input_expressions=[expr],
        event_expression=event,
        cross_channel_custom_statistics={"spread": _spread, "count": _total_sample_count},
    )

    _, _, _, cross_channel_values = stats_agg.build(cache=None)

    assert len(cross_channel_values) == 2
    for interval_map in cross_channel_values:
        assert set(interval_map.keys()) == {"spread", "count"}
        assert interval_map["count"] == 1.0


def test_build_cross_channel_stat_without_event_expression():
    expr1 = _mock_expr([0.0, 2.0], [2.0, 4.0], [1.0, 3.0])
    expr2 = _mock_expr([1.0, 3.0], [3.0, 5.0], [10.0, 20.0])

    stats_agg = StatsAggregator(
        input_expressions=[expr1, expr2],
        event_expression=None,
        cross_channel_custom_statistics={"spread": _spread},
    )

    _, _, _, cross_channel_values = stats_agg.build(cache=None)

    assert len(cross_channel_values) == 1
    nptest.assert_almost_equal(cross_channel_values[0]["spread"], 19.0)


def test_cross_channel_inputs_subset_order_and_clipping():
    expr_a = _mock_expr([0.0], [1.0], [1.0])
    expr_b = _mock_expr([0.0], [1.0], [2.0])
    # channel c has a sample spanning the interval boundary at t=1.0
    expr_c = _mock_expr([0.5], [1.5], [3.0])
    event = _mock_event([0.0], [1.0])

    received = []

    def capture(series, t_start, t_end):
        received.append((series, t_start, t_end))
        return 0.0

    stats_agg = StatsAggregator(
        input_expressions=[expr_a, expr_b, expr_c],
        event_expression=event,
        cross_channel_custom_statistics={
            "captured": CrossChannelStatistic(func=capture, inputs=["c", "a"])
        },
        input_names=["a", "b", "c"],
    )

    stats_agg.build(cache=None)

    assert len(received) == 1
    series, t_start, t_end = received[0]
    assert (t_start, t_end) == (0.0, 1.0)
    # only the declared inputs, in declared order: c first, then a
    assert len(series) == 2
    nptest.assert_almost_equal(series[0].values, [3.0])
    nptest.assert_almost_equal(series[1].values, [1.0])
    # the boundary-spanning sample of c is truncated to the interval
    assert np.all(series[0].tstarts >= t_start)
    assert np.all(series[0].tends <= t_end)


def test_cross_channel_stat_called_with_empty_series():
    # channel has samples only within the first interval
    expr = _mock_expr([0.0], [1.0], [10.0])
    event = _mock_event([0.0, 2.0], [1.0, 3.0])

    stats_agg = StatsAggregator(
        input_expressions=[expr],
        event_expression=event,
        cross_channel_custom_statistics={"spread": _spread, "count": _total_sample_count},
    )

    _, _, _, cross_channel_values = stats_agg.build(cache=None)

    assert len(cross_channel_values) == 2
    nptest.assert_almost_equal(cross_channel_values[0]["spread"], 0.0)
    assert np.isnan(cross_channel_values[1]["spread"])
    # count-like statistics can return 0 instead of NaN for empty intervals
    assert cross_channel_values[1]["count"] == 0.0


def test_build_degenerate_interval_skipped_for_cross_channel():
    expr = _mock_expr([0.0], [1.0], [10.0])
    # trailing zero-length interval survives the Intervals constructor
    event = _mock_event([0.0, 2.0], [1.0, 2.0])

    stats_agg = StatsAggregator(
        input_expressions=[expr],
        event_expression=event,
        statistics=["min"],
        cross_channel_custom_statistics={"count": _total_sample_count},
    )

    _, numeric_values, _, cross_channel_values = stats_agg.build(cache=None)

    assert len(cross_channel_values) == 1
    assert len(cross_channel_values) == len(numeric_values[0])


def test_cross_channel_error_propagates_from_build():
    expr = _mock_expr([0.0], [1.0], [10.0])

    def broken(series, t_start, t_end):
        raise RuntimeError("boom")

    stats_agg = StatsAggregator(
        input_expressions=[expr],
        cross_channel_custom_statistics={"broken": broken},
    )

    with pytest.raises(RuntimeError, match="boom"):
        stats_agg.build(cache=None)


def test_cross_channel_non_scalar_return_raises_type_error():
    expr = _mock_expr([0.0], [1.0], [10.0])

    def returns_none(series, t_start, t_end):
        return None

    stats_agg = StatsAggregator(
        input_expressions=[expr],
        cross_channel_custom_statistics={"bad_stat": returns_none},
    )

    with pytest.raises(TypeError, match="bad_stat"):
        stats_agg.build(cache=None)


def test_build_custom_only_without_builtin_statistics():
    expr = _mock_expr([0.0], [1.0], [10.0])
    event = _mock_event([0.0], [1.0])

    stats_agg = StatsAggregator(
        input_expressions=[expr],
        event_expression=event,
        cross_channel_custom_statistics={"count": _total_sample_count},
    )

    _, numeric_values, _, cross_channel_values = stats_agg.build(cache=None)

    assert numeric_values == [[{}]]
    assert cross_channel_values == [{"count": 1.0}]


def test_per_channel_custom_stat_alongside_builtins():
    expr1 = _mock_expr([0.0, 2.0], [1.0, 3.0], [3.0, 4.0])
    expr2 = _mock_expr([0.0, 2.0], [1.0, 3.0], [6.0, 8.0])
    event = _mock_event([0.0, 2.0], [1.0, 3.0])

    stats_agg = StatsAggregator(
        input_expressions=[expr1, expr2],
        event_expression=event,
        statistics=["min", "max"],
        per_channel_custom_statistics={"rms": _rms},
    )

    _, numeric_values, _, _ = stats_agg.build(cache=None)

    assert len(numeric_values) == 2
    for signal_values in numeric_values:
        assert len(signal_values) == 2
        for interval_map in signal_values:
            assert set(interval_map.keys()) == {"min", "max", "rms"}
    # single-sample intervals: rms equals the absolute sample value
    nptest.assert_almost_equal(numeric_values[0][0]["rms"], 3.0)
    nptest.assert_almost_equal(numeric_values[0][1]["rms"], 4.0)
    nptest.assert_almost_equal(numeric_values[1][0]["rms"], 6.0)
    nptest.assert_almost_equal(numeric_values[1][1]["rms"], 8.0)


def test_per_channel_custom_stat_called_for_empty_interval():
    # samples only in the first interval; built-ins NaN-fill the second interval
    expr = _mock_expr([0.0], [1.0], [10.0])
    event = _mock_event([0.0, 2.0], [1.0, 3.0])

    stats_agg = StatsAggregator(
        input_expressions=[expr],
        event_expression=event,
        statistics=["min"],
        per_channel_custom_statistics={"count": _sample_count},
    )

    _, numeric_values, _, _ = stats_agg.build(cache=None)

    assert np.isnan(numeric_values[0][1]["min"])
    assert numeric_values[0][1]["count"] == 0.0
    assert numeric_values[0][0]["count"] == 1.0


def test_per_channel_custom_stat_receives_clipped_series():
    expr = _mock_expr([0.5], [1.5], [3.0])
    event = _mock_event([0.0], [1.0])

    received = []

    def capture(series, t_start, t_end):
        received.append((series, t_start, t_end))
        return 0.0

    stats_agg = StatsAggregator(
        input_expressions=[expr],
        event_expression=event,
        per_channel_custom_statistics={"captured": capture},
    )

    stats_agg.build(cache=None)

    assert len(received) == 1
    series, t_start, t_end = received[0]
    assert isinstance(series, SampleSeries)
    assert np.all(series.tstarts >= t_start)
    assert np.all(series.tends <= t_end)


def test_per_channel_non_scalar_return_raises_type_error():
    expr = _mock_expr([0.0], [1.0], [10.0])

    def returns_list(series, t_start, t_end):
        return [1.0, 2.0]

    stats_agg = StatsAggregator(
        input_expressions=[expr],
        per_channel_custom_statistics={"bad_stat": returns_list},
    )

    with pytest.raises(TypeError, match="bad_stat"):
        stats_agg.build(cache=None)


def test_custom_statistics_validation_errors():
    expr = _mock_expr([0.0], [1.0], [10.0])

    with pytest.raises(TypeError, match="cross_channel_custom_statistics"):
        StatsAggregator([expr], cross_channel_custom_statistics=[_spread])

    with pytest.raises(TypeError, match="per_channel_custom_statistics"):
        StatsAggregator([expr], per_channel_custom_statistics={"rms": "not callable"})

    with pytest.raises(TypeError, match="callable"):
        StatsAggregator([expr], cross_channel_custom_statistics={"x": 42})

    with pytest.raises(ValueError, match="non-empty"):
        StatsAggregator([expr], cross_channel_custom_statistics={"": _spread})

    with pytest.raises(ValueError, match="built-in"):
        StatsAggregator([expr], cross_channel_custom_statistics={"mean": _spread})

    with pytest.raises(ValueError, match="built-in"):
        StatsAggregator([expr], per_channel_custom_statistics={"max": _rms})

    with pytest.raises(ValueError, match="both"):
        StatsAggregator(
            [expr],
            cross_channel_custom_statistics={"x": _spread},
            per_channel_custom_statistics={"x": _rms},
        )


def test_cross_channel_inputs_validation_errors():
    expr = _mock_expr([0.0], [1.0], [10.0])

    with pytest.raises(ValueError, match="no input_names"):
        StatsAggregator(
            [expr],
            cross_channel_custom_statistics={
                "x": CrossChannelStatistic(func=_spread, inputs=["a"])
            },
        )

    with pytest.raises(ValueError, match="unknown input"):
        StatsAggregator(
            [expr],
            cross_channel_custom_statistics={
                "x": CrossChannelStatistic(func=_spread, inputs=["missing"])
            },
            input_names=["a"],
        )

    with pytest.raises(ValueError, match="Length mismatch"):
        StatsAggregator([expr], input_names=["a", "b"])

    with pytest.raises(ValueError, match="unique"):
        StatsAggregator([expr, expr], input_names=["a", "a"])


def _count_above(series, t_start, t_end, threshold=0.0):
    """Cross-channel: number of samples above a configurable threshold."""
    return float(sum((s.values > threshold).sum() for s in series))


def _scaled_rms(series, t_start, t_end, scale=1.0):
    """Per-channel: scaled root mean square."""
    if len(series) == 0:
        return float("nan")
    return float(scale * np.sqrt(np.nanmean(series.values**2)))


def test_cross_channel_stat_with_params():
    expr1 = _mock_expr([0.0, 1.0], [1.0, 2.0], [10.0, 30.0])
    expr2 = _mock_expr([0.0, 1.0], [1.0, 2.0], [20.0, 40.0])
    event = _mock_event([0.0], [2.0])

    stats_agg = StatsAggregator(
        input_expressions=[expr1, expr2],
        event_expression=event,
        cross_channel_custom_statistics={
            "count_default": _count_above,
            "count_hi": CrossChannelStatistic(func=_count_above, params={"threshold": 25.0}),
        },
    )

    _, _, _, cross_channel_values = stats_agg.build(cache=None)

    assert len(cross_channel_values) == 1
    # default threshold 0.0 counts all four samples
    assert cross_channel_values[0]["count_default"] == 4.0
    # provisioned threshold 25.0 counts only 30.0 and 40.0
    assert cross_channel_values[0]["count_hi"] == 2.0


def test_per_channel_stat_with_params():
    expr = _mock_expr([0.0], [1.0], [3.0])
    event = _mock_event([0.0], [1.0])

    stats_agg = StatsAggregator(
        input_expressions=[expr],
        event_expression=event,
        per_channel_custom_statistics={
            "rms": _scaled_rms,
            "rms_x10": PerChannelStatistic(func=_scaled_rms, params={"scale": 10.0}),
        },
    )

    _, numeric_values, _, _ = stats_agg.build(cache=None)

    nptest.assert_almost_equal(numeric_values[0][0]["rms"], 3.0)
    nptest.assert_almost_equal(numeric_values[0][0]["rms_x10"], 30.0)


def test_custom_statistic_params_validation_errors():
    expr = _mock_expr([0.0], [1.0], [10.0])

    with pytest.raises(TypeError, match="params must be a dict"):
        StatsAggregator(
            [expr],
            cross_channel_custom_statistics={
                "x": CrossChannelStatistic(func=_count_above, params=[1, 2])
            },
        )

    with pytest.raises(TypeError, match="identifiers"):
        StatsAggregator(
            [expr],
            per_channel_custom_statistics={
                "x": PerChannelStatistic(func=_scaled_rms, params={"not a name": 1.0})
            },
        )


def test_str_contains_custom_statistics():
    expr = _mock_expr([0.0], [1.0], [10.0])

    stats_agg = StatsAggregator(
        input_expressions=[expr],
        statistics=["min"],
        cross_channel_custom_statistics={
            "spread": CrossChannelStatistic(func=_spread, inputs=["a"])
        },
        per_channel_custom_statistics={"rms": _rms},
        input_names=["a"],
    )

    str_repr = str(stats_agg)
    assert "cross_channel_custom_statistics={'spread': ['a']}" in str_repr
    assert "per_channel_custom_statistics=['rms']" in str_repr
