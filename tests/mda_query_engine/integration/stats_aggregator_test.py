"""Integration tests for StatsAggregator with end-to-end usage."""

import pytest

from mda_query_engine.analyze.query.aggregations.stats_aggregator import StatsAggregator
from mda_query_engine.analyze.query.solvers.key_value_store_solver import KeyValueStoreSolver


def test_stats_case_check_numeric_values(spark, basic_narrow_db):
    """Test StatsAggregator with single input expression and all statistics."""
    query = basic_narrow_db.query

    eng_rpm = query.channel(channel_name="Engine RPM")
    veh_spd = query.channel(channel_name="Vehicle Speed Sensor")
    air_temp = query.channel(channel_name="Intake Air Temperature")
    air_temp_event = air_temp >= 0

    stats_aggregator = StatsAggregator(
        [air_temp], ["start", "end", "min", "max", "mean"], air_temp_event
    )

    metric_container_id = query.metric("container_id")
    filter_cond = metric_container_id == 1
    df = (
        query.where(filter_cond)
        .select(stats_aggregator.alias("my_stats"))
        .solve(spark, solver=KeyValueStoreSolver(spark))
    )

    base_stats = df.select("my_stats").collect()

    assert df.count() == 1

    # Expected values for Row 0
    expected_numeric_values = [
        [
            {
                "start": 22.0,
                "end": 25.0,
                "min": 22.0,
                "max": 34.0,
                "mean": 26.938194608826354,
            }
        ]
    ]
    expected_event_timestamps = [[1499929242072000.0, 1499929442993000.0]]

    # Get values from Row 0
    my_stats = base_stats[0]["my_stats"]
    numeric_values = my_stats["numeric_values"]
    event_timestamps = my_stats["event_timestamps"]

    # Assert event_timestamps
    assert len(event_timestamps) == 1
    assert event_timestamps[0][0] == pytest.approx(expected_event_timestamps[0][0])
    assert event_timestamps[0][1] == pytest.approx(expected_event_timestamps[0][1])

    # Assert numeric_values for air_temp (index 0)
    assert len(numeric_values) == 1
    assert numeric_values[0][0]["start"] == pytest.approx(expected_numeric_values[0][0]["start"])
    assert numeric_values[0][0]["end"] == pytest.approx(expected_numeric_values[0][0]["end"])
    assert numeric_values[0][0]["min"] == pytest.approx(expected_numeric_values[0][0]["min"])
    assert numeric_values[0][0]["max"] == pytest.approx(expected_numeric_values[0][0]["max"])
    assert numeric_values[0][0]["mean"] == pytest.approx(expected_numeric_values[0][0]["mean"])


def test_stats_aggregator_multiple_input_expressions(spark, basic_narrow_db):
    """Test StatsAggregator with multiple input expressions."""
    query = basic_narrow_db.query

    eng_rpm = query.channel(channel_name="Engine RPM")
    veh_spd = query.channel(channel_name="Vehicle Speed Sensor")
    air_temp = query.channel(channel_name="Intake Air Temperature")

    # Event based on air temperature threshold
    air_temp_event = air_temp >= 0

    # Create StatsAggregator with multiple input expressions
    stats_aggregator = StatsAggregator([eng_rpm, veh_spd], ["max", "mean"], air_temp_event)

    metric_container_id = query.metric("container_id")
    filter_cond = metric_container_id == 1
    df = (
        query.where(filter_cond)
        .select(stats_aggregator.alias("multi_stats"))
        .solve(spark, solver=KeyValueStoreSolver(spark))
    )

    base_stats = df.select("multi_stats").collect()

    assert df.count() == 1

    # Get values from Row 0
    my_stats = base_stats[0]["multi_stats"]
    numeric_values = my_stats["numeric_values"]
    event_timestamps = my_stats["event_timestamps"]

    # Verify we have results for both input expressions (eng_rpm and veh_spd)
    assert len(numeric_values) == 2, "Should have numeric values for 2 input expressions"

    # Verify each input expression has statistics for each event
    for i, channel_values in enumerate(numeric_values):
        assert len(channel_values) > 0, f"Channel {i} should have at least one event"
        for event_stats in channel_values:
            assert "max" in event_stats, f"Channel {i} should have 'max' statistic"
            assert "mean" in event_stats, f"Channel {i} should have 'mean' statistic"


def test_stats_aggregator_event_interval_filtering(spark, basic_narrow_db):
    """Test that StatsAggregator correctly filters data by event intervals."""
    query = basic_narrow_db.query

    eng_rpm = query.channel(channel_name="Engine RPM")
    air_temp = query.channel(channel_name="Intake Air Temperature")

    # Create a more restrictive event condition
    air_temp_high = air_temp > 25

    stats_aggregator = StatsAggregator([eng_rpm], ["min", "max", "mean"], air_temp_high)

    metric_container_id = query.metric("container_id")
    filter_cond = metric_container_id == 1
    df = (
        query.where(filter_cond)
        .select(stats_aggregator.alias("filtered_stats"))
        .solve(spark, solver=KeyValueStoreSolver(spark))
    )

    base_stats = df.select("filtered_stats").collect()

    # Verify that the statistics are computed only within the event intervals
    for row in base_stats:
        my_stats = row["filtered_stats"]
        numeric_values = my_stats["numeric_values"]
        event_timestamps = my_stats["event_timestamps"]

        # Each event should have valid timestamps
        for ts in event_timestamps:
            assert len(ts) == 2, "Each event should have [start, end] timestamps"
            assert ts[0] <= ts[1], "Start timestamp should be <= end timestamp"


def test_stats_aggregator_statistics_computation_accuracy(spark, basic_narrow_db):
    """Test accuracy of statistics computation."""
    query = basic_narrow_db.query

    air_temp = query.channel(channel_name="Intake Air Temperature")
    air_temp_event = air_temp >= 0

    # Test all numeric statistics
    stats_aggregator = StatsAggregator(
        [air_temp], ["min", "max", "mean", "start", "end"], air_temp_event
    )

    metric_container_id = query.metric("container_id")
    filter_cond = metric_container_id == 1
    df = (
        query.where(filter_cond)
        .select(stats_aggregator.alias("accuracy_stats"))
        .solve(spark, solver=KeyValueStoreSolver(spark))
    )

    base_stats = df.select("accuracy_stats").collect()

    for row in base_stats:
        my_stats = row["accuracy_stats"]
        numeric_values = my_stats["numeric_values"]

        for channel_values in numeric_values:
            for event_stats in channel_values:
                # Verify all requested statistics are present
                assert "min" in event_stats
                assert "max" in event_stats
                assert "mean" in event_stats
                assert "start" in event_stats
                assert "end" in event_stats

                # Verify logical constraints
                assert event_stats["min"] <= event_stats["max"], "min should be <= max"
                assert (
                    event_stats["min"] <= event_stats["mean"] <= event_stats["max"]
                ), "mean should be between min and max"


def test_stats_aggregator_with_median(spark, basic_narrow_db):
    """Test StatsAggregator with median statistic."""
    query = basic_narrow_db.query

    air_temp = query.channel(channel_name="Intake Air Temperature")
    air_temp_event = air_temp >= 0

    stats_aggregator = StatsAggregator([air_temp], ["min", "max", "median"], air_temp_event)

    metric_container_id = query.metric("container_id")
    filter_cond = metric_container_id == 1
    df = (
        query.where(filter_cond)
        .select(stats_aggregator.alias("median_stats"))
        .solve(spark, solver=KeyValueStoreSolver(spark))
    )

    base_stats = df.select("median_stats").collect()

    for row in base_stats:
        my_stats = row["median_stats"]
        numeric_values = my_stats["numeric_values"]

        for channel_values in numeric_values:
            for event_stats in channel_values:
                assert "median" in event_stats
                # Median should be between min and max
                assert (
                    event_stats["min"] <= event_stats["median"] <= event_stats["max"]
                ), "median should be between min and max"
