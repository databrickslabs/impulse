"""Integration tests for StatsAggregator using basic_narrow_db fixture."""

import pytest

from mda_query_engine.analyze.query.aggregations.stats_aggregator import StatsAggregator
from mda_query_engine.analyze.query.solvers.key_value_store_solver import KeyValueStoreSolver


class TestStatisticsAggregatorIntegration:
    """Integration tests using test data from basic_narrow_db."""

    def test_statistics_single_channel_no_event(self, spark, basic_narrow_db):
        """Test statistics calculation for a single channel without event filtering."""
        query = basic_narrow_db.query

        # Select Engine RPM channel
        eng_rpm = query.channel(channel_name="Engine RPM")

        # Create statistics aggregator
        stats_agg = StatsAggregator(
            input_expressions=[eng_rpm],
            statistics=["min", "max", "mean", "median"],
        )

        # Build query and solve
        stats_query = query.select(stats_agg.alias("engine_rpm_stats"))
        result = stats_query.solve(spark=spark, solver=KeyValueStoreSolver(spark))

        # Should have results for each container (3 containers in test data)
        assert result.count() == 3

        # Collect results and check structure
        rows = result.collect()
        for row in rows:
            stats_data = row["engine_rpm_stats"]
            event_timestamps, numeric_values, string_values = stats_data

            # Should have one event (entire series)
            assert len(event_timestamps) == 1

            # Should have one expression with statistics
            assert len(numeric_values) == 1
            assert len(numeric_values[0]) == 1

            # Check statistics exist and are valid
            stats = numeric_values[0][0]
            assert "min" in stats
            assert "max" in stats
            assert "mean" in stats
            assert "median" in stats

            # Engine RPM should have min >= 0 (based on test data)
            assert stats["min"] >= 0
            # Engine RPM max should be positive (based on test data)
            assert stats["max"] > 0
            # Mean should be between min and max
            assert stats["min"] <= stats["mean"] <= stats["max"]

    def test_statistics_multiple_channels_no_event(self, spark, basic_narrow_db):
        """Test statistics calculation for multiple channels without event filtering."""
        query = basic_narrow_db.query

        # Select multiple channels
        eng_rpm = query.channel(channel_name="Engine RPM")
        veh_speed = query.channel(channel_name="Vehicle Speed Sensor")

        # Create statistics aggregator with multiple selections
        stats_agg = StatsAggregator(
            input_expressions=[eng_rpm, veh_speed],
            statistics=["min", "max", "mean"],
        )

        # Build query and solve
        stats_query = query.select(stats_agg.alias("multi_channel_stats"))
        result = stats_query.solve(spark=spark, solver=KeyValueStoreSolver(spark))

        # Should have results for each container
        assert result.count() == 3

        # Collect results and check structure
        rows = result.collect()
        for row in rows:
            stats_data = row["multi_channel_stats"]
            event_timestamps, numeric_values, string_values = stats_data

            # event_timestamps is appended inside the per-expression loop,
            # so N=2 expressions with one synthetic event each give 2 entries.
            assert len(event_timestamps) == 2

            # Should have two expressions (eng_rpm and veh_speed)
            assert len(numeric_values) == 2

            # Each expression should have statistics for one event
            for expr_stats in numeric_values:
                assert len(expr_stats) == 1
                stats = expr_stats[0]
                assert "min" in stats
                assert "max" in stats
                assert "mean" in stats

    def test_statistics_with_event_expression(self, spark, basic_narrow_db):
        """Test statistics calculation with event filtering (Vehicle Speed > 50)."""
        query = basic_narrow_db.query

        # Select channels
        eng_rpm = query.channel(channel_name="Engine RPM")
        veh_speed = query.channel(channel_name="Vehicle Speed Sensor")

        # Create event expression: when vehicle speed > 50 km/h
        high_speed_event = veh_speed > 50

        # Create statistics aggregator with event filtering
        stats_agg = StatsAggregator(
            input_expressions=[eng_rpm],
            statistics=["min", "max", "mean"],
            event_expression=high_speed_event,
        )

        # Build query and solve
        stats_query = query.select(stats_agg.alias("high_speed_rpm_stats"))
        result = stats_query.solve(spark=spark, solver=KeyValueStoreSolver(spark))

        # Should have results for each container
        assert result.count() == 3

        # Collect results and check structure
        rows = result.collect()
        for row in rows:
            stats_data = row["high_speed_rpm_stats"]
            event_timestamps, numeric_values, string_values = stats_data

            # May have zero or more events depending on data
            # (some containers may never exceed 50 km/h)
            assert len(event_timestamps) >= 0

            # Should have one expression
            assert len(numeric_values) == 1

            # Check each event has statistics
            for event_stats in numeric_values[0]:
                assert "min" in event_stats
                assert "max" in event_stats
                assert "mean" in event_stats

    def test_statistics_subset_only_mean(self, spark, basic_narrow_db):
        """Test requesting only mean statistic."""
        query = basic_narrow_db.query

        # Select channel
        eng_rpm = query.channel(channel_name="Engine RPM")

        # Create statistics aggregator with only mean
        stats_agg = StatsAggregator(
            input_expressions=[eng_rpm],
            statistics=["mean"],
        )

        # Build query and solve
        stats_query = query.select(stats_agg.alias("rpm_mean_only"))
        result = stats_query.solve(spark=spark, solver=KeyValueStoreSolver(spark))

        # Collect results
        rows = result.collect()
        for row in rows:
            stats_data = row["rpm_mean_only"]
            event_timestamps, numeric_values, string_values = stats_data

            stats = numeric_values[0][0]
            # Should only have mean
            assert "mean" in stats
            assert "min" not in stats
            assert "max" not in stats
            assert "median" not in stats

    def test_statistics_with_compound_event(self, spark, basic_narrow_db):
        """Test statistics with compound event expression (RPM > 1000 AND Speed > 20)."""
        query = basic_narrow_db.query

        # Select channels
        eng_rpm = query.channel(channel_name="Engine RPM")
        veh_speed = query.channel(channel_name="Vehicle Speed Sensor")
        ambient_temp = query.channel(channel_name="Ambient Air Temperature")

        # Create compound event expression
        driving_event = (eng_rpm > 1000) & (veh_speed > 20)

        # Create statistics aggregator for ambient temperature during driving
        stats_agg = StatsAggregator(
            input_expressions=[ambient_temp],
            statistics=["min", "max", "mean", "median"],
            event_expression=driving_event,
        )

        # Build query and solve
        stats_query = query.select(stats_agg.alias("temp_during_driving"))
        result = stats_query.solve(spark=spark, solver=KeyValueStoreSolver(spark))

        # Should have results for each container
        assert result.count() == 3

        # Collect and verify structure
        rows = result.collect()
        for row in rows:
            stats_data = row["temp_during_driving"]
            event_timestamps, numeric_values, string_values = stats_data

            # Should have one expression
            assert len(numeric_values) == 1

    def test_statistics_aggregator_dtype_in_query(self, spark, basic_narrow_db):
        """Test that dtype schema is correctly used in query results."""
        query = basic_narrow_db.query

        eng_rpm = query.channel(channel_name="Engine RPM")

        stats_agg = StatsAggregator(
            input_expressions=[eng_rpm],
            statistics=["min", "max"],
        )

        stats_query = query.select(stats_agg.alias("rpm_stats"))
        result = stats_query.solve(spark=spark, solver=KeyValueStoreSolver(spark))

        # Check schema of result
        schema = result.schema
        stats_field = schema["rpm_stats"]

        # Should be a struct with event_timestamps, numeric_values, string_values
        struct_fields = {f.name for f in stats_field.dataType.fields}
        assert "event_timestamps" in struct_fields
        assert "numeric_values" in struct_fields
        assert "string_values" in struct_fields

    def test_compare_statistics_with_native_methods(self, spark, basic_narrow_db):
        """Compare StatisticsAggregator results with native SampleSeries methods."""
        query = basic_narrow_db.query

        eng_rpm = query.channel(channel_name="Engine RPM")

        # Get statistics using aggregator
        stats_agg = StatsAggregator(
            input_expressions=[eng_rpm],
            statistics=["min", "max", "mean"],
        )

        # Also get native min, max, mean
        native_min = eng_rpm.min().alias("native_min")
        native_max = eng_rpm.max().alias("native_max")
        native_mean = eng_rpm.mean().alias("native_mean")

        # Run queries
        stats_query = query.select(
            stats_agg.alias("agg_stats"), native_min, native_max, native_mean
        )
        result = stats_query.solve(spark=spark, solver=KeyValueStoreSolver(spark))

        # Compare results
        rows = result.collect()
        for row in rows:
            stats_data = row["agg_stats"]
            event_timestamps, numeric_values, string_values = stats_data

            agg_stats = numeric_values[0][0]

            # Compare with native methods (should be approximately equal)
            assert abs(agg_stats["min"] - row["native_min"]) < 0.001
            assert abs(agg_stats["max"] - row["native_max"]) < 0.001
            assert abs(agg_stats["mean"] - row["native_mean"]) < 0.001

    def test_start_end_statistics_with_exact_values(self, spark, basic_narrow_db):
        """Test start/end statistics with exact expected values for a single container."""
        query = basic_narrow_db.query

        air_temp = query.channel(channel_name="Intake Air Temperature")
        air_temp_event = air_temp >= 0

        stats_agg = StatsAggregator(
            input_expressions=[air_temp],
            statistics=["start", "end", "min", "max", "mean"],
            event_expression=air_temp_event,
        )

        metric_container_id = query.metric("container_id")
        filter_cond = metric_container_id == 1
        df = (
            query.where(filter_cond)
            .select(stats_agg.alias("my_stats"))
            .solve(spark, solver=KeyValueStoreSolver(spark))
        )

        assert df.count() == 1

        my_stats = df.select("my_stats").collect()[0]["my_stats"]
        numeric_values = my_stats["numeric_values"]
        event_timestamps = my_stats["event_timestamps"]

        expected_numeric = {
            "start": 22.0,
            "end": 25.0,
            "min": 22.0,
            "max": 34.0,
            "mean": 26.938194608826354,
        }
        expected_timestamps = [1499929242072000.0, 1499929442993000.0]

        assert len(event_timestamps) == 1
        assert event_timestamps[0][0] == pytest.approx(expected_timestamps[0])
        assert event_timestamps[0][1] == pytest.approx(expected_timestamps[1])

        stats = numeric_values[0][0]
        for key, expected_val in expected_numeric.items():
            assert stats[key] == pytest.approx(expected_val), f"{key} mismatch"

    def test_multiple_input_expressions_with_event(self, spark, basic_narrow_db):
        """Test multiple input expressions with event filtering."""
        query = basic_narrow_db.query

        eng_rpm = query.channel(channel_name="Engine RPM")
        veh_spd = query.channel(channel_name="Vehicle Speed Sensor")
        air_temp = query.channel(channel_name="Intake Air Temperature")
        air_temp_event = air_temp >= 0

        stats_agg = StatsAggregator(
            input_expressions=[eng_rpm, veh_spd],
            statistics=["max", "mean"],
            event_expression=air_temp_event,
        )

        metric_container_id = query.metric("container_id")
        df = (
            query.where(metric_container_id == 1)
            .select(stats_agg.alias("multi_stats"))
            .solve(spark, solver=KeyValueStoreSolver(spark))
        )

        assert df.count() == 1

        my_stats = df.select("multi_stats").collect()[0]["multi_stats"]
        numeric_values = my_stats["numeric_values"]

        assert len(numeric_values) == 2, "Should have results for 2 input expressions"
        for i, channel_values in enumerate(numeric_values):
            assert len(channel_values) > 0, f"Channel {i} should have at least one event"
            for event_stats in channel_values:
                assert "max" in event_stats
                assert "mean" in event_stats

    def test_event_interval_filtering_structure(self, spark, basic_narrow_db):
        """Test that event intervals have valid structure."""
        query = basic_narrow_db.query

        eng_rpm = query.channel(channel_name="Engine RPM")
        air_temp = query.channel(channel_name="Intake Air Temperature")
        air_temp_high = air_temp > 25

        stats_agg = StatsAggregator(
            input_expressions=[eng_rpm],
            statistics=["min", "max", "mean"],
            event_expression=air_temp_high,
        )

        metric_container_id = query.metric("container_id")
        df = (
            query.where(metric_container_id == 1)
            .select(stats_agg.alias("filtered_stats"))
            .solve(spark, solver=KeyValueStoreSolver(spark))
        )

        for row in df.collect():
            my_stats = row["filtered_stats"]
            event_timestamps = my_stats["event_timestamps"]
            for ts in event_timestamps:
                assert len(ts) == 2, "Each event should have [start, end]"
                assert ts[0] <= ts[1], "Start should be <= end"

    def test_all_statistics_with_logical_constraints(self, spark, basic_narrow_db):
        """Test all statistics are present and satisfy logical constraints."""
        query = basic_narrow_db.query

        air_temp = query.channel(channel_name="Intake Air Temperature")
        air_temp_event = air_temp >= 0

        stats_agg = StatsAggregator(
            input_expressions=[air_temp],
            statistics=["min", "max", "mean", "start", "end"],
            event_expression=air_temp_event,
        )

        metric_container_id = query.metric("container_id")
        df = (
            query.where(metric_container_id == 1)
            .select(stats_agg.alias("accuracy_stats"))
            .solve(spark, solver=KeyValueStoreSolver(spark))
        )

        for row in df.collect():
            for channel_values in row["accuracy_stats"]["numeric_values"]:
                for event_stats in channel_values:
                    assert all(k in event_stats for k in ["min", "max", "mean", "start", "end"])
                    assert event_stats["min"] <= event_stats["max"]
                    assert event_stats["min"] <= event_stats["mean"] <= event_stats["max"]

    def test_median_between_min_and_max(self, spark, basic_narrow_db):
        """Test that median is between min and max."""
        query = basic_narrow_db.query

        air_temp = query.channel(channel_name="Intake Air Temperature")
        air_temp_event = air_temp >= 0

        stats_agg = StatsAggregator(
            input_expressions=[air_temp],
            statistics=["min", "max", "median"],
            event_expression=air_temp_event,
        )

        metric_container_id = query.metric("container_id")
        df = (
            query.where(metric_container_id == 1)
            .select(stats_agg.alias("median_stats"))
            .solve(spark, solver=KeyValueStoreSolver(spark))
        )

        for row in df.collect():
            for channel_values in row["median_stats"]["numeric_values"]:
                for event_stats in channel_values:
                    assert "median" in event_stats
                    assert event_stats["min"] <= event_stats["median"] <= event_stats["max"]
