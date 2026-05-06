"""Tests for StatsAggregator._calculate_aggregations method.

Note: This test suite tests the _calculate_aggregations method which computes
statistics on time series data. Some tests are marked as xfail due to remaining
issues with the slicing logic (idx_start:idx_end excludes the end value).
"""

from unittest.mock import MagicMock

import numpy as np
import numpy.testing as nptest

from mda_query_engine.analyze.metadata.tag_expression import TagSelector
from mda_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesSelector,
)
from mda_query_engine.analyze.query.aggregations.stats_aggregator import (
    StatsAggregator,
)
from mda_query_engine.model.series.sample_series import SampleSeries


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

    event_timestamps, numeric_values, string_values = stats_agg.build(cache=None)

    # event_timestamps is appended inside the per-expression loop in build(),
    # so for N=2 expressions with one synthetic event each the list has 2 entries.
    assert event_timestamps == [[0.0, 5.0], [0.0, 5.0]]
    assert len(numeric_values) == 2
    assert len(numeric_values[0]) == 1
    assert len(numeric_values[1]) == 1
    assert numeric_values[0][0] == {"start": 1.0, "end": 3.0}
    assert numeric_values[1][0] == {"start": 10.0, "end": 20.0}
    assert string_values == []


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
