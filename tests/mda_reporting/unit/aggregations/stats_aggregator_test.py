"""Unit tests for StatsAggregator reporting class.

This module contains unit tests for the StatsAggregator class from mda_reporting.
Tests follow the same pattern as histogram_test.py.
"""

import pytest

from mda_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from mda_query_engine.analyze.query.solvers.key_value_store_solver import KeyValueStoreSolver
from mda_reporting.aggregations.stats_aggregator import StatsAggregator
from mda_reporting.events.basic_event import BasicEvent


def test_as_spark_row():
    """Test that as_spark_row returns a Row with expected fields."""
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    stats_agg = StatsAggregator(
        name="my_stats_1",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Signal 1"],
        statistics=["min", "max", "mean"],
        event=basic_event,
    )
    row = stats_agg.as_spark_row()

    # Verify the row has the expected structure
    assert hasattr(row, "name")
    assert row.name == "my_stats_1"


def test_stats_aggregator_init():
    """Test StatsAggregator initialization with required parameters."""
    input_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    stats_agg = StatsAggregator(
        name="test_stats",
        input_expressions=[input_expr],
        channel_names=["Test Signal"],
        statistics=["min", "max", "mean"],
        event=basic_event,
    )

    assert stats_agg.name == "test_stats"
    assert stats_agg.page_number == -1  # Default value
    assert stats_agg.input_expressions == [input_expr]
    assert stats_agg.channel_names == ["Test Signal"]
    assert stats_agg.statistics == ["min", "max", "mean"]
    assert stats_agg.event == basic_event
    assert stats_agg.desc is None
    assert stats_agg.agg_type == "stats_aggregator"
    assert stats_agg.values_unit is None


def test_stats_aggregator_init_with_optional_params():
    """Test StatsAggregator initialization with all optional parameters."""
    input_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    stats_agg = StatsAggregator(
        name="test_stats",
        input_expressions=[input_expr],
        channel_names=["Test Signal"],
        statistics=["min", "max", "mean", "median"],
        event=basic_event,
        desc="Test statistics aggregation",
        agg_type="custom_stats",
        values_unit="rpm",
    )

    assert stats_agg.name == "test_stats"
    assert stats_agg.input_expressions == [input_expr]
    assert stats_agg.channel_names == ["Test Signal"]
    assert stats_agg.statistics == ["min", "max", "mean", "median"]
    assert stats_agg.event == basic_event
    assert stats_agg.desc == "Test statistics aggregation"
    assert stats_agg.agg_type == "custom_stats"
    assert stats_agg.values_unit == "rpm"


def test_stats_aggregator_init_multiple_expressions():
    """Test StatsAggregator initialization with multiple input expressions."""
    input_expr1 = TimeSeriesSelector(None)
    input_expr2 = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    stats_agg = StatsAggregator(
        name="multi_stats",
        input_expressions=[input_expr1, input_expr2],
        channel_names=["Signal 1", "Signal 2"],
        statistics=["min", "max"],
        event=basic_event,
    )

    assert len(stats_agg.input_expressions) == 2
    assert len(stats_agg.channel_names) == 2
    assert stats_agg.channel_names == ["Signal 1", "Signal 2"]


def test_stats_aggregator_channel_names_validation():
    """Test that channel_names validation raises error on length mismatch."""
    input_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    with pytest.raises(ValueError) as exc_info:
        StatsAggregator(
            name="invalid_stats",
            input_expressions=[input_expr],
            channel_names=["Signal 1", "Signal 2"],  # Mismatch: 1 expr, 2 names
            statistics=["min", "max"],
            event=basic_event,
        )

    assert "Length mismatch" in str(exc_info.value)


def test_stats_aggregator_empty_expressions_raises():
    """Test that empty input_expressions raises error."""
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    with pytest.raises(ValueError) as exc_info:
        StatsAggregator(
            name="invalid_stats",
            input_expressions=[],
            channel_names=[],
            statistics=["min", "max"],
            event=basic_event,
        )

    assert "At least one input expression is required" in str(exc_info.value)


def test_get_name():
    """Test get_name method."""
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    stats_agg = StatsAggregator(
        name="my_statistics",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Signal"],
        statistics=["min"],
        event=basic_event,
    )
    assert stats_agg.get_name() == "my_statistics"


def test_get_id():
    """Test get_id returns consistent unique identifier."""
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    stats_agg = StatsAggregator(
        name="test_stats",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Signal"],
        statistics=["min", "max"],
        event=basic_event,
    )

    # get_id should return the same value on repeated calls
    id1 = stats_agg.get_id()
    id2 = stats_agg.get_id()
    assert id1 == id2
    assert isinstance(id1, int)
    assert id1 >= 0


def test_get_id_different_for_different_names():
    """Test that get_id returns different values for different names."""
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    stats_agg1 = StatsAggregator(
        name="stats_1",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Signal"],
        statistics=["min"],
        event=basic_event,
    )

    stats_agg2 = StatsAggregator(
        name="stats_2",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Signal"],
        statistics=["min"],
        event=basic_event,
    )

    assert stats_agg1.get_id() != stats_agg2.get_id()


def test_get_expression():
    """Test get_expression method returns TimeSeriesExpression."""
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)
    input_expr = TimeSeriesSelector(None)

    stats_agg = StatsAggregator(
        name="test",
        input_expressions=[input_expr],
        channel_names=["Signal"],
        statistics=["min", "max", "mean"],
        event=basic_event,
    )

    expression = stats_agg.get_expression()
    assert expression is not None
    assert hasattr(expression, "__str__")


def test_get_expression_str():
    """Test get_expression_str method."""
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    stats_agg = StatsAggregator(
        name="test",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Signal"],
        statistics=["min", "max"],
        event=basic_event,
    )

    expr_str = stats_agg.get_expression_str()
    assert isinstance(expr_str, str)
    assert expr_str != "NA"


def test_get_event():
    """Test get_event method returns the associated event."""
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    stats_agg = StatsAggregator(
        name="test",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Signal"],
        statistics=["min"],
        event=basic_event,
    )

    assert stats_agg.get_event() == basic_event
    assert stats_agg.get_event().get_name() == "test_event"


def test_as_dict():
    """Test as_dict method returns dictionary with expected keys."""
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    stats_agg = StatsAggregator(
        name="test_stats",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Test Signal"],
        statistics=["min", "max", "mean"],
        event=basic_event,
        desc="Test description",
        values_unit="rpm",
    )

    stats_dict = stats_agg.as_dict()

    assert isinstance(stats_dict, dict)
    assert stats_dict["name"] == "test_stats"
    assert stats_dict["page_number"] == -1  # Default value
    assert stats_dict["description"] == "Test description"
    assert stats_dict["agg_type"] == "stats_aggregator"
    assert stats_dict["statistics"] == ["min", "max", "mean"]
    assert stats_dict["channel_names"] == ["Test Signal"]
    assert stats_dict["values_unit"] == "rpm"
    assert "signal_expressions" in stats_dict
    assert "visual_id" in stats_dict


def test_as_dict_multiple_signals():
    """Test as_dict with multiple signals."""
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    stats_agg = StatsAggregator(
        name="multi_stats",
        input_expressions=[TimeSeriesSelector(None), TimeSeriesSelector(None)],
        channel_names=["Signal 1", "Signal 2"],
        statistics=["min", "max"],
        event=basic_event,
    )

    stats_dict = stats_agg.as_dict()

    assert len(stats_dict["channel_names"]) == 2
    assert stats_dict["channel_names"] == ["Signal 1", "Signal 2"]
    assert len(stats_dict["signal_expressions"]) == 2


def test_as_spark_row_complete():
    """Test as_spark_row with all fields populated."""
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    stats_agg = StatsAggregator(
        name="complete_stats",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Complete Signal"],
        statistics=["min", "max", "mean", "median"],
        event=basic_event,
        desc="Complete stats test",
        agg_type="full_stats",
        values_unit="count",
    )

    row = stats_agg.as_spark_row()

    assert row.name == "complete_stats"
    assert row.description == "Complete stats test"
    assert row.agg_type == "full_stats"
    assert row.statistics == ["min", "max", "mean", "median"]
    assert row.channel_names == ["Complete Signal"]
    assert row.values_unit == "count"


def test_as_spark_row_minimal():
    """Test as_spark_row with minimal required fields."""
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    stats_agg = StatsAggregator(
        name="minimal_stats",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Minimal Signal"],
        statistics=["min"],
        event=basic_event,
    )

    row = stats_agg.as_spark_row()

    assert row.name == "minimal_stats"
    assert row.description is None
    assert row.agg_type == "stats_aggregator"
    assert row.statistics == ["min"]
    assert row.values_unit is None


def test_statistics_list_validation():
    """Test that statistics parameter accepts different statistic types."""
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    # Test with various statistics
    stats_all = StatsAggregator(
        name="all_stats",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Signal"],
        statistics=["min", "max", "mean", "median", "start", "end"],
        event=basic_event,
    )
    assert stats_all.statistics == ["min", "max", "mean", "median", "start", "end"]

    # Test with single statistic
    stats_single = StatsAggregator(
        name="single_stat",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Signal"],
        statistics=["mean"],
        event=basic_event,
    )
    assert stats_single.statistics == ["mean"]


def test_determine_aggregations(spark, basic_narrow_db):
    """Test that determine_aggregations returns a DataFrame with expected columns."""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    veh_spd = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")
    event = BasicEvent(name="test_event_1", expr=eng_rpm > 1000)

    stats_agg = StatsAggregator(
        name="test_stats",
        input_expressions=[eng_rpm],
        channel_names=["Engine RPM"],
        statistics=["min", "max", "mean"],
        event=event,
    )

    solver = KeyValueStoreSolver(spark)
    solved_df = basic_narrow_db.query.select(stats_agg.get_expression()).solve(spark, solver)
    df = StatsAggregator.determine_aggregations(
        spark=spark,
        aggregations=[stats_agg],
        solved_df=solved_df,
    )

    assert df.count() > 0  # Ensure that some data is returned
    assert "container_id" in df.columns
    assert "visual_id" in df.columns
    assert "channel_name" in df.columns
    assert "aggregation_label" in df.columns
    assert "statistic_value" in df.columns


def test_determine_aggregations_multiple_stats(spark, basic_narrow_db):
    """Test determine_aggregations with multiple StatsAggregator instances."""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    veh_spd = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")

    event1 = BasicEvent(name="rpm_event", expr=eng_rpm > 500)
    event2 = BasicEvent(name="speed_event", expr=veh_spd > 10)

    stats_agg1 = StatsAggregator(
        name="rpm_stats",
        input_expressions=[eng_rpm],
        channel_names=["Engine RPM"],
        statistics=["min", "max"],
        event=event1,
    )

    stats_agg2 = StatsAggregator(
        name="speed_stats",
        input_expressions=[veh_spd],
        channel_names=["Vehicle Speed"],
        statistics=["min", "max", "mean"],
        event=event2,
    )

    solver = KeyValueStoreSolver(spark)
    solved_df = basic_narrow_db.query.select(
        stats_agg1.get_expression(), stats_agg2.get_expression()
    ).solve(spark, solver)
    df = StatsAggregator.determine_aggregations(
        spark=spark,
        aggregations=[stats_agg1, stats_agg2],
        solved_df=solved_df,
    )

    assert df.count() > 0

    # Verify both stats aggregators have results using visual_id
    # (stats_name is not in final output schema)
    visual_ids = df.select("visual_id").distinct().collect()
    visual_ids_list = [row["visual_id"] for row in visual_ids]

    assert stats_agg1.get_id() in visual_ids_list
    assert stats_agg2.get_id() in visual_ids_list

    # Verify both signal names are present
    channel_names = df.select("channel_name").distinct().collect()
    channel_names_list = [row["channel_name"] for row in channel_names]

    assert "Engine RPM" in channel_names_list
    assert "Vehicle Speed" in channel_names_list


def test_determine_metadata_df(spark, basic_narrow_db):
    """Test that determine_metadata_df returns a DataFrame with expected columns."""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    veh_spd = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")

    event1 = BasicEvent(name="event1", expr=eng_rpm > 0)
    event2 = BasicEvent(name="event2", expr=veh_spd > 0)

    stats1 = StatsAggregator(
        name="test_stats_1",
        desc="Engine RPM statistics",
        input_expressions=[eng_rpm],
        channel_names=["Engine RPM"],
        statistics=["min", "max", "mean"],
        event=event1,
        values_unit="rpm",
    )

    stats2 = StatsAggregator(
        name="test_stats_2",
        input_expressions=[veh_spd],
        channel_names=["Vehicle Speed"],
        statistics=["min", "max"],
        event=event2,
    )

    df = StatsAggregator.determine_metadata_df(spark=spark, stats_aggregators=[stats1, stats2])

    assert df.count() == 2  # Ensure that metadata for two aggregators is returned
    assert "page_number" in df.columns
    assert "name" in df.columns
    assert "description" in df.columns
    assert "agg_type" in df.columns
    assert "statistics" in df.columns
    assert "channel_names" in df.columns
    assert "values_unit" in df.columns


def test_get_visual_id_column(spark, basic_narrow_db):
    """Test get_visual_id_column static method with list of aggregations."""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    event = BasicEvent(name="test_event", expr=eng_rpm > 0)

    stats1 = StatsAggregator(
        name="stats_1",
        input_expressions=[eng_rpm],
        channel_names=["Engine RPM"],
        statistics=["min", "max"],
        event=event,
    )

    stats2 = StatsAggregator(
        name="stats_2",
        input_expressions=[eng_rpm],
        channel_names=["Engine RPM"],
        statistics=["mean"],
        event=event,
    )

    aggregations = [stats1, stats2]

    col_expr = StatsAggregator.get_visual_id_column(aggregations, "stats_name")

    test_data = [("stats_1",), ("stats_2",), ("unknown_stats",)]
    df = spark.createDataFrame(test_data, ["stats_name"])

    result_df = df.withColumn("visual_id", col_expr)
    results = result_df.collect()

    assert results[0]["visual_id"] == stats1.get_id()
    assert results[1]["visual_id"] == stats2.get_id()
    assert results[2]["visual_id"] is None


def test_get_visual_id_column_empty_aggregations(spark):
    """Test get_visual_id_column static method with empty aggregations list."""
    aggregations = []

    col_expr = StatsAggregator.get_visual_id_column(aggregations, "stats_name")

    test_data = [("any_stats",), ("another_stats",)]
    df = spark.createDataFrame(test_data, ["stats_name"])

    result_df = df.withColumn("visual_id", col_expr)
    results = result_df.collect()

    # All should map to None when aggregations list is empty
    assert results[0]["visual_id"] is None
    assert results[1]["visual_id"] is None


def test_stats_aggregator_with_all_statistics(spark, basic_narrow_db):
    """Test StatsAggregator with all available statistics."""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    event = BasicEvent(name="all_stats_event", expr=eng_rpm >= 0)

    stats_agg = StatsAggregator(
        name="all_statistics_test",
        input_expressions=[eng_rpm],
        channel_names=["Engine RPM"],
        statistics=["min", "max", "mean", "median", "start", "end"],
        event=event,
        desc="Test all statistics",
    )

    solver = KeyValueStoreSolver(spark)
    solved_df = basic_narrow_db.query.select(stats_agg.get_expression()).solve(spark, solver)
    df = StatsAggregator.determine_aggregations(
        spark=spark,
        aggregations=[stats_agg],
        solved_df=solved_df,
    )

    # Verify all statistics are present in results
    agg_labels = df.select("aggregation_label").distinct().collect()
    agg_labels_list = [row["aggregation_label"] for row in agg_labels]

    expected_stats = ["min", "max", "mean", "median", "start", "end"]
    for stat in expected_stats:
        assert stat in agg_labels_list, f"Expected statistic '{stat}' not found in results"


def test_stats_aggregator_multiple_input_expressions(spark, basic_narrow_db):
    """Test StatsAggregator with multiple input expressions."""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    veh_spd = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")
    event = BasicEvent(name="multi_expr_event", expr=eng_rpm >= 0)

    stats_agg = StatsAggregator(
        name="multi_expression_stats",
        input_expressions=[eng_rpm, veh_spd],
        channel_names=["Engine RPM", "Vehicle Speed"],
        statistics=["min", "max"],
        event=event,
    )

    solver = KeyValueStoreSolver(spark)
    solved_df = basic_narrow_db.query.select(stats_agg.get_expression()).solve(spark, solver)
    df = StatsAggregator.determine_aggregations(
        spark=spark,
        aggregations=[stats_agg],
        solved_df=solved_df,
    )

    # Verify both signals are present in results
    channel_names = df.select("channel_name").distinct().collect()
    channel_names_list = [row["channel_name"] for row in channel_names]

    assert "Engine RPM" in channel_names_list
    assert "Vehicle Speed" in channel_names_list


def test_report_id_and_page_number_assignment():
    """Test that report_id and page_number can be assigned."""
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    stats_agg = StatsAggregator(
        name="test_stats",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Signal"],
        statistics=["min"],
        event=basic_event,
    )

    # Verify default values
    assert stats_agg.page_number == -1

    # Assign values
    stats_agg.page_number = 5
    stats_agg.report_id = 123

    assert stats_agg.page_number == 5
    assert stats_agg.report_id == 123

    # Verify reflected in as_dict
    stats_dict = stats_agg.as_dict()
    assert stats_dict["page_number"] == 5
    assert stats_dict["report_id"] == 123


def test_determine_aggregations_requires_solved_df(spark):
    stats_agg = StatsAggregator(
        name="test_stats",
        input_expressions=[TimeSeriesSelector(None)],
        channel_names=["Engine RPM"],
        statistics=["min", "max", "mean"],
        event=BasicEvent(name="test_event_1", expr=TimeSeriesSelector(None)),
    )

    with pytest.raises(ValueError, match="requires solved_df"):
        StatsAggregator.determine_aggregations(spark=spark, aggregations=[stats_agg])
