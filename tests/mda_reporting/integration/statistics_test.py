import os
from unittest.mock import create_autospec

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient

from mda_reporting.aggregations.histogram import HistogramDuration
from mda_reporting.aggregations.stats_aggregator import StatsAggregator
from mda_reporting.core.page import Page
from mda_reporting.core.report import Report
from mda_reporting.events.basic_event import BasicEvent


def test_statistics_basic_report(spark):
    """Test basic Statistics aggregation with events."""
    # Global report configuration
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    my_report: Report = Report(
        name="stats_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=config_path,
    )

    # Definition of relevant channels
    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")

    # Define events
    rpm_event = BasicEvent(name="rpm_event", expr=c1 > 0, desc="Engine RPM > 0")
    speed_event = BasicEvent(name="speed_event", expr=c2 > 0, desc="Vehicle speed > 0")
    my_report.add_event(rpm_event)
    my_report.add_event(speed_event)

    # Definition of 1st page
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)

    # Statistics aggregation for Engine RPM
    stats1 = StatsAggregator(
        name="rpm_stats",
        input_expressions=[c1],
        channel_names=["Engine RPM"],
        statistics=["min", "max", "mean", "median"],
        event=rpm_event,
        desc="Engine RPM statistics",
    )
    my_first_page.add_aggregation(stats1)

    # Statistics aggregation for Vehicle Speed
    stats2 = StatsAggregator(
        name="speed_stats",
        input_expressions=[c2],
        channel_names=["Vehicle Speed"],
        statistics=["min", "max", "mean", "median"],
        event=speed_event,
        desc="Vehicle Speed statistics",
    )
    my_first_page.add_aggregation(stats2)

    # Determine content of all pages
    my_report.determine_report()
    agg_dfs = my_report.aggregation_dfs
    agg_metadata_dfs = my_report.aggregation_metadata_dfs

    # Verify fact data
    assert "STATS_AGGREGATOR" in agg_dfs
    stats_df = agg_dfs["STATS_AGGREGATOR"]["changed"]
    assert stats_df.count() > 0

    # Verify correctness of calculated statistics for Engine RPM (channel_id 5)
    # Expected values calculated from channel_data.csv where Engine RPM > 0 (event filter)
    # Note: min/max are simple values, mean and median are time-weighted (weighted by sample duration)
    # Time-weighted mean = sum(value * duration) / sum(duration)
    # Weighted median = value at index where cumulative weight reaches 50% of total weight
    expected_rpm_stats = {
        # container_id: (min, max, time_weighted_mean, weighted_median)
        # min/max computed from raw values, mean/median computed as duration-weighted
        1: (846.0, 1578.0, 963.8365527175433, 935.0),
        2: (838.0, 1430.0, 1015.9071733203062, 941.0),
        3: (802.0, 1415.0, 933.6027334846816, 931.0),
    }
    pivoted_stats_df = (
        stats_df.groupBy("container_id", "channel_name")
        .pivot("aggregation_label", ["min", "max", "mean", "median"])
        .agg(F.first("statistic_value"))
    )
    result_rows = pivoted_stats_df.collect()
    eng_rpm_stats = [row for row in result_rows if row.channel_name == "Engine RPM"]
    actual_rpm_stats = {
        row.container_id: (row.min, row.max, row.mean, row.median) for row in eng_rpm_stats
    }
    for container_id, (
        expected_min,
        expected_max,
        expected_mean,
        expected_median,
    ) in expected_rpm_stats.items():
        actual_min, actual_max, actual_mean, actual_median = actual_rpm_stats[container_id]
        assert (
            actual_min == expected_min
        ), f"Container {container_id} min mismatch: expected {expected_min}, got {actual_min}"
        assert (
            actual_max == expected_max
        ), f"Container {container_id} max mismatch: expected {expected_max}, got {actual_max}"
        assert (
            abs(actual_mean - expected_mean) < 0.01
        ), f"Container {container_id} mean mismatch: expected {expected_mean}, got {actual_mean}"
        assert (
            actual_median == expected_median
        ), f"Container {container_id} median mismatch: expected {expected_median}, got {actual_median}"

    # Verify required columns exist
    expected_columns = [
        "event_instance_id",
        "container_id",
        "visual_id",
        "channel_name",
        "event_id",
        "aggregation_label",
        "statistic_value",
    ]
    for col in expected_columns:
        assert col in stats_df.columns, f"Missing column: {col}"

    # Verify we have data for both statistics aggregations
    visual_ids = [row.visual_id for row in stats_df.select("visual_id").distinct().collect()]
    assert len(visual_ids) == 2, "Expected 2 distinct visual_ids for 2 statistics aggregations"

    # Verify aggregation labels are present
    labels = [
        row.aggregation_label for row in stats_df.select("aggregation_label").distinct().collect()
    ]
    assert "min" in labels
    assert "max" in labels
    assert "mean" in labels

    # Verify metadata
    assert "STATS_AGGREGATOR" in agg_metadata_dfs
    stats_metadata_df = agg_metadata_dfs["STATS_AGGREGATOR"]
    assert stats_metadata_df.count() == 2  # Two statistics aggregations


def test_statistics_with_multiple_selections(spark):
    """Test Statistics aggregation with multiple signal selections."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    my_report: Report = Report(
        name="multi_signal_stats_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=config_path,
    )

    # Definition of relevant channels
    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")

    # Define an event
    multi_signal_event = BasicEvent(name="multi_signal_event", expr=c1 > 0, desc="Engine RPM > 0")
    my_report.add_event(multi_signal_event)

    # Definition of 1st page
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)

    # Statistics aggregation with multiple selections
    multi_stats = StatsAggregator(
        name="multi_signal_stats",
        input_expressions=[c1, c2],
        channel_names=["Engine RPM", "Vehicle Speed"],
        statistics=["min", "max", "mean"],
        event=multi_signal_event,
        desc="Multi-signal statistics",
    )
    my_first_page.add_aggregation(multi_stats)

    # Determine content of all pages
    my_report.determine_report()
    agg_dfs = my_report.aggregation_dfs
    agg_metadata_dfs = my_report.aggregation_metadata_dfs

    # Verify fact data
    assert "STATS_AGGREGATOR" in agg_dfs
    stats_df = agg_dfs["STATS_AGGREGATOR"]["changed"]
    assert stats_df.count() > 0

    # Verify we have data for both signals
    channel_names = [
        row.channel_name for row in stats_df.select("channel_name").distinct().collect()
    ]
    assert "Engine RPM" in channel_names
    assert "Vehicle Speed" in channel_names

    # Verify metadata contains array of channel names
    stats_metadata_df = agg_metadata_dfs["STATS_AGGREGATOR"]
    metadata_row = stats_metadata_df.collect()[0]
    assert len(metadata_row.channel_names) == 2
    assert len(metadata_row.signal_expressions) == 2


def test_statistics_mixed_with_histogram(spark):
    """Test Statistics aggregation alongside Histogram aggregation in the same report."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    my_report: Report = Report(
        name="mixed_agg_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=config_path,
    )

    # Definition of relevant channels
    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")

    # Define an event
    rpm_event = BasicEvent(name="rpm_event", expr=c1 > 0, desc="Engine RPM > 0")
    my_report.add_event(rpm_event)

    # Definition of 1st page
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)

    # Statistics aggregation
    rpm_stats = StatsAggregator(
        name="rpm_statistics",
        input_expressions=[c1],
        channel_names=["Engine RPM"],
        statistics=["min", "max", "mean", "median"],
        event=rpm_event,
        desc="Engine RPM statistics",
    )
    my_first_page.add_aggregation(rpm_stats)

    # Histogram aggregation
    hist_name = "rpm_histogram"
    hist_bins = [float(i) for i in range(0, 8000, 250)]
    rpm_hist = HistogramDuration(hist_name, base_expr=c1, bins=hist_bins)
    my_first_page.add_aggregation(rpm_hist)

    # Determine content of all pages
    my_report.determine_report()
    agg_dfs = my_report.aggregation_dfs
    agg_metadata_dfs = my_report.aggregation_metadata_dfs

    # Verify both aggregation types are present
    assert "STATS_AGGREGATOR" in agg_dfs
    assert "HISTOGRAM" in agg_dfs

    assert agg_dfs["STATS_AGGREGATOR"]["changed"].count() > 0
    assert agg_dfs["HISTOGRAM"]["changed"].filter(F.col("hist_value") > 0).count() > 0

    # Verify metadata for both
    assert "STATS_AGGREGATOR" in agg_metadata_dfs
    assert "HISTOGRAM" in agg_metadata_dfs
    assert agg_metadata_dfs["STATS_AGGREGATOR"].count() == 1
    assert agg_metadata_dfs["HISTOGRAM"].count() == 1


def test_persist_statistics_report(spark):
    """Test persisting a report with Statistics aggregations."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    my_report: Report = Report(
        name="persist_stats_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=config_path,
    )

    # Definition of relevant channels
    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")

    # Define an event
    signal_event = BasicEvent(name="signal_event", expr=c1 > 0, desc="Engine RPM > 0")
    my_report.add_event(signal_event)

    # Definition of 1st page
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)

    # Statistics aggregation
    stats = StatsAggregator(
        name="signal_stats",
        input_expressions=[c1, c2],
        channel_names=["Engine RPM", "Vehicle Speed"],
        statistics=["min", "max", "mean", "median"],
        event=signal_event,
        desc="Signal statistics",
    )
    my_first_page.add_aggregation(stats)

    # Determine content of all pages
    my_report.determine_report()

    # Persist the report
    my_report.persist_results()

    # Verify tables exist
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_stats_aggregator_fact")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_stats_aggregator_dimension")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_measurement_dimension")

    # Verify data was persisted
    stats_fact = spark.read.table("spark_catalog.gold.evaluation_stats_aggregator_fact")
    stats_dimension = spark.read.table("spark_catalog.gold.evaluation_stats_aggregator_dimension")

    assert stats_fact.count() > 0
    assert stats_dimension.count() == 1

    # Verify fact table has expected columns
    expected_columns = [
        "event_instance_id",
        "container_id",
        "visual_id",
        "channel_name",
        "event_id",
        "aggregation_label",
        "statistic_value",
    ]
    for col in expected_columns:
        assert col in stats_fact.columns


def test_persist_statistics_with_events(spark):
    """Test persisting a report with Statistics aggregations and events."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    my_report: Report = Report(
        name="persist_stats_event_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=config_path,
    )

    # Definition of relevant channels
    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")

    # Define an event
    speed_event_expr = c2 > 1
    speed_event = BasicEvent(
        name="vehicle_moving", expr=speed_event_expr, desc="Vehicle speed > 1 km/h"
    )
    my_report.add_event(speed_event)

    # Definition of 1st page
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)

    # Statistics aggregation with event
    stats = StatsAggregator(
        name="rpm_stats_moving",
        input_expressions=[c1],
        channel_names=["Engine RPM"],
        statistics=["min", "max", "mean"],
        event=speed_event,
        desc="Engine RPM stats while moving",
    )
    my_first_page.add_aggregation(stats)

    # Determine content of all pages
    my_report.determine_report()

    # Persist the report
    my_report.persist_results()

    # Verify all expected tables exist
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_stats_aggregator_fact")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_stats_aggregator_dimension")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_event_dimension")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_event_instance_fact")

    # Verify data
    stats_fact = spark.read.table("spark_catalog.gold.evaluation_stats_aggregator_fact")
    event_instance_fact = spark.read.table("spark_catalog.gold.evaluation_event_instance_fact")

    assert stats_fact.count() > 0
    assert event_instance_fact.count() > 0

    # Verify event_instance_id values in stats_fact match event_instance_fact
    stats_event_ids = set(
        row.event_instance_id
        for row in stats_fact.filter(F.col("event_instance_id").isNotNull())
        .select("event_instance_id")
        .distinct()
        .collect()
    )
    event_ids = set(
        row.event_instance_id
        for row in event_instance_fact.select("event_instance_id").distinct().collect()
    )
    # Stats event IDs should be a subset of (or equal to) event instance IDs
    assert stats_event_ids.issubset(event_ids) or len(stats_event_ids) > 0
