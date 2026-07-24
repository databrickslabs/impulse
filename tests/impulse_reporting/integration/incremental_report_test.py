from unittest.mock import create_autospec

import pyspark.sql.functions as F
import pyspark.sql.types as T
from databricks.sdk import WorkspaceClient

from impulse_reporting.aggregations.histogram import (
    HistogramDuration,
)
from impulse_reporting.aggregations.point_value_aggregator import PointValueAggregator
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.channels.calculated_channel import CalculatedChannel
from impulse_reporting.config.config_parser import (
    IncrementalConfig,
    ImpulseConfig,
    Source,
    UnitySink,
)
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.events.container_event import ContainerEvent
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent
from tests.conftest import spark
from tests.impulse_reporting.integration.test_helpers import (
    clone_silver_with_shrunk_container,
)


def set_config(silver_table, is_enabled=True):
    return ImpulseConfig(
        source=Source(
            container_metrics_table=f"spark_catalog.silver.{silver_table}",
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri="spark_catalog.silver.channels",
        ),
        unity_sink=UnitySink(
            catalog="spark_catalog",
            schema="gold",
            table_prefix="evaluation",
        ),
        incremental=IncrementalConfig(
            enabled=is_enabled,
            silver_last_modified_column="timestamp",
            gold_last_modified_column="_created_at",
        ),
    )


def test_incremental_in_report_case_1(spark):
    # Global report configuration
    # case 1 nothing changes in silver, no updates in gold, no results expected

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(set_config("container_metrics_inc_1", False)),
    )

    add_aggs_to_report(my_report)
    # Determine content of all pages
    my_report.determine_report()

    my_report.persist_results()

    meas_df_pre = spark.read.table("spark_catalog.gold.evaluation_measurement_dimension")

    meas_df_pre_collected = meas_df_pre.collect()

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(set_config("container_metrics_inc_1", True)),
    )

    add_aggs_to_report(my_report)

    # Determine content of all pages
    my_report.determine_report()

    my_report.persist_results()

    meas_df_post_case_1 = spark.read.table("spark_catalog.gold.evaluation_measurement_dimension")

    assert meas_df_post_case_1.count() == 1

    assert meas_df_pre_collected == meas_df_post_case_1.collect()


def test_incremental_in_report_case_2(spark):
    # #case 2 silver updated, gold not updated, results expected for all containers

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(set_config("container_metrics_inc_1_2", True)),
    )

    add_aggs_to_report(my_report)

    # # Determine content of all pages
    my_report.determine_report()

    my_report.persist_results()

    meas_df_post_case_2 = spark.read.table("spark_catalog.gold.evaluation_measurement_dimension")

    assert meas_df_post_case_2.count() == 2


def test_incremental_in_report_case_3(spark):
    # case 3 modify one container in silver, no updates in gold, results expected only for modified container

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(set_config("container_metrics_inc_1_2", True)),
    )
    add_aggs_to_report(my_report)

    my_report.determine_report()
    my_report.persist_results()

    # Save state before case 3 for comparison
    meas_df_pre_case3 = spark.read.table("spark_catalog.gold.evaluation_measurement_dimension")
    container_2_created_at_pre = (
        meas_df_pre_case3.where(F.col("container_id") == 2).select("_created_at").collect()[0][0]
    )

    hist_fact_pre_case3 = spark.read.table("spark_catalog.gold.evaluation_histogram_fact")
    hist_fact_container1_pre = (
        hist_fact_pre_case3.where(F.col("container_id") == 1)
        .orderBy("visual_id", "bin_id")
        .collect()
    )
    hist_fact_container2_pre = (
        hist_fact_pre_case3.where(F.col("container_id") == 2)
        .orderBy("visual_id", "bin_id")
        .collect()
    )

    # Also save container 1's created_at before modification
    container_1_created_at_pre = (
        meas_df_pre_case3.where(F.col("container_id") == 1).select("_created_at").collect()[0][0]
    )

    # Create a modified silver table with timestamp column
    # Container 1 gets a future timestamp (newer than gold _created_at) -> detected as updated
    # Container 2 gets an old timestamp (older than gold _created_at) -> not detected as updated
    container_metrics_12 = spark.read.table("spark_catalog.silver.container_metrics_inc_1_2")
    modified = container_metrics_12.withColumn(
        "timestamp",
        F.when(
            F.col("container_id") == 1,
            F.current_timestamp(),
        ).otherwise(F.lit("2020-01-01 00:00:00").cast("timestamp")),
    )
    modified.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver.container_metrics_inc_1_2_modified"
    )

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(set_config("container_metrics_inc_1_2_modified", True)),
    )
    add_aggs_to_report(my_report)
    my_report.determine_report()
    my_report.persist_results()

    meas_df_post_case_3 = spark.read.table("spark_catalog.gold.evaluation_measurement_dimension")
    assert meas_df_post_case_3.count() == 2

    # Container 1 should be updated (has future timestamp)
    container_1_created_at_post = (
        meas_df_post_case_3.where(F.col("container_id") == 1).select("_created_at").collect()[0][0]
    )
    assert (
        container_1_created_at_pre < container_1_created_at_post
    ), "Container 1's _created_at should be updated (newer) due to timestamp change in silver"

    # Container 1's histogram facts should be updated
    hist_fact_post_case3 = spark.read.table("spark_catalog.gold.evaluation_histogram_fact")
    hist_fact_container1_post = (
        hist_fact_post_case3.where(F.col("container_id") == 1)
        .orderBy("visual_id", "bin_id")
        .collect()
    )
    assert (
        hist_fact_container1_pre != hist_fact_container1_post
    ), "Container 1's histogram facts should be recalculated due to container update"

    # Container 2 should be unchanged
    container_2_created_at_post = (
        meas_df_post_case_3.where(F.col("container_id") == 2).select("_created_at").collect()[0][0]
    )
    assert container_2_created_at_pre == container_2_created_at_post

    # Container 2's histogram facts should be unchanged

    hist_fact_container2_post = (
        hist_fact_post_case3.where(F.col("container_id") == 2)
        .orderBy("visual_id", "bin_id")
        .collect()
    )
    assert hist_fact_container2_pre == hist_fact_container2_post


def test_incremental_in_report_case_4(spark):

    # case 4 change definition hash of an aggregation and an event

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(set_config("container_metrics_inc_1_2", True)),
    )

    add_aggs_to_report(my_report)
    # # Determine content of all pages
    my_report.determine_report()

    my_report.persist_results()

    # Save histogram dimension and facts before the definition change
    hist_dim_pre_case4 = spark.read.table("spark_catalog.gold.evaluation_histogram_dimension")

    stats_dim_pre_case4 = spark.read.table(
        "spark_catalog.gold.evaluation_stats_aggregator_dimension"
    )

    hist1_hash_pre = (
        hist_dim_pre_case4.where(F.col("name") == "rpm_hist_p1")
        .select("definition_hash")
        .collect()[0][0]
    )
    hist1_visual_id = (
        hist_dim_pre_case4.where(F.col("name") == "rpm_hist_p1")
        .select("visual_id")
        .collect()[0][0]
    )
    hist2_visual_id = (
        hist_dim_pre_case4.where(F.col("name") == "speed_hist_p1")
        .select("visual_id")
        .collect()[0][0]
    )

    hist_fact_pre_case4 = spark.read.table("spark_catalog.gold.evaluation_histogram_fact")

    stats_fact_pre_case4 = spark.read.table("spark_catalog.gold.evaluation_stats_aggregator_fact")

    hist1_fact_count_pre = hist_fact_pre_case4.where(F.col("visual_id") == hist1_visual_id).count()

    hist2_fact_pre = (
        hist_fact_pre_case4.where(F.col("visual_id") == hist2_visual_id)
        .orderBy("container_id", "bin_id")
        .collect()
    )

    # Save stats aggregator definition hash and facts before the change
    stats_agg_hash_pre = (
        stats_dim_pre_case4.where(F.col("name") == "stats_agg")
        .select("definition_hash")
        .collect()[0][0]
    )
    stats_agg_visual_id = (
        stats_dim_pre_case4.where(F.col("name") == "stats_agg").select("visual_id").collect()[0][0]
    )

    stats_fact_count_pre = stats_fact_pre_case4.where(
        F.col("visual_id") == stats_agg_visual_id
    ).count()

    # Use same silver table (no container changes), but change hist1's bins
    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(set_config("container_metrics_inc_1_2", True)),
    )

    add_aggs_to_report_changed_bins(my_report)
    my_report.determine_report()
    my_report.persist_results()

    meas_df_post_case_4 = spark.read.table("spark_catalog.gold.evaluation_measurement_dimension")
    assert meas_df_post_case_4.count() == 2  # No container changes

    hist_dim_post_case4 = spark.read.table("spark_catalog.gold.evaluation_histogram_dimension")
    stats_dim_post_case4 = spark.read.table(
        "spark_catalog.gold.evaluation_stats_aggregator_dimension"
    )

    hist1_hash_post = (
        hist_dim_post_case4.where(F.col("name") == "rpm_hist_p1")
        .select("definition_hash")
        .collect()[0][0]
    )
    assert hist1_hash_pre != hist1_hash_post

    # stats_agg's definition_hash should have changed
    stats_agg_hash_post = (
        stats_dim_post_case4.where(F.col("name") == "stats_agg")
        .select("definition_hash")
        .collect()[0][0]
    )
    assert (
        stats_agg_hash_pre != stats_agg_hash_post
    ), "stats_agg definition_hash should change when statistics list changes"

    # hist1's fact count should differ (fewer bins -> fewer rows per container)
    hist_fact_post_case4 = spark.read.table("spark_catalog.gold.evaluation_histogram_fact")

    stats_fact_post_case4 = spark.read.table("spark_catalog.gold.evaluation_stats_aggregator_fact")
    hist1_fact_count_post = hist_fact_post_case4.where(
        F.col("visual_id") == hist1_visual_id
    ).count()

    assert hist1_fact_count_pre != hist1_fact_count_post

    # stats_agg fact count should differ (different statistics -> different number of rows)
    stats_fact_count_post = stats_fact_post_case4.where(
        F.col("visual_id") == stats_agg_visual_id
    ).count()
    assert (
        stats_fact_count_pre != stats_fact_count_post
    ), "stats_agg facts should be recalculated when definition_hash changes"

    # hist2's facts should be unchanged (no container changes, definition unchanged)
    hist2_fact_post = (
        hist_fact_post_case4.where(F.col("visual_id") == hist2_visual_id)
        .orderBy("container_id", "bin_id")
        .collect()
    )
    assert hist2_fact_pre == hist2_fact_post


def test_incremental_in_report_case_5(spark):

    # case 5 both new files and changed definition hash

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(set_config("container_metrics_inc_1_2", True)),
    )

    add_aggs_to_report(my_report)

    my_report.determine_report()
    my_report.persist_results()

    meas_df_pre_case_5 = spark.read.table("spark_catalog.gold.evaluation_measurement_dimension")
    assert meas_df_pre_case_5.count() == 2

    # Save histogram dimension and facts before the definition change
    hist_dim_pre_case4 = spark.read.table("spark_catalog.gold.evaluation_histogram_dimension")

    hist1_hash_pre = (
        hist_dim_pre_case4.where(F.col("name") == "rpm_hist_p1")
        .select("definition_hash")
        .collect()[0][0]
    )
    hist1_visual_id = (
        hist_dim_pre_case4.where(F.col("name") == "rpm_hist_p1")
        .select("visual_id")
        .collect()[0][0]
    )
    hist2_visual_id = (
        hist_dim_pre_case4.where(F.col("name") == "speed_hist_p1")
        .select("visual_id")
        .collect()[0][0]
    )

    hist_fact_pre_case5 = spark.read.table("spark_catalog.gold.evaluation_histogram_fact")

    agg_fact_pre_case5 = spark.read.table("spark_catalog.gold.evaluation_stats_aggregator_fact")

    agg_pre_count = agg_fact_pre_case5.count()

    total_hist_agg_count_pre = hist_fact_pre_case5.count()

    count_hist_1_container_1_pre = hist_fact_pre_case5.where(
        (F.col("visual_id") == hist1_visual_id) & (F.col("container_id") == 1)
    ).count()

    # Save hist2 facts before definition change (should remain unchanged)
    hist2_fact_pre_case5 = (
        hist_fact_pre_case5.where(F.col("visual_id") == hist2_visual_id)
        .orderBy("container_id", "bin_id")
        .collect()
    )

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(set_config("container_metrics", True)),
    )
    add_aggs_to_report_changed_bins(my_report)

    my_report.determine_report()
    my_report.persist_results()

    meas_df_post_case_5 = spark.read.table("spark_catalog.gold.evaluation_measurement_dimension")
    assert meas_df_post_case_5.count() == 3  # Container 3 added

    # hist1's definition_hash should have changed
    hist_dim_post_case4 = spark.read.table("spark_catalog.gold.evaluation_histogram_dimension")
    hist1_hash_post = (
        hist_dim_post_case4.where(F.col("name") == "rpm_hist_p1")
        .select("definition_hash")
        .collect()[0][0]
    )

    assert hist1_hash_pre != hist1_hash_post

    hist_fact_post_case5 = spark.read.table("spark_catalog.gold.evaluation_histogram_fact")

    total_hist_agg_count_post = hist_fact_post_case5.count()

    agg_fact_post_case5 = spark.read.table("spark_catalog.gold.evaluation_stats_aggregator_fact")

    agg_post_count = agg_fact_post_case5.count()

    hist1_container_ids = sorted(
        [
            row.container_id
            for row in hist_fact_post_case5.where(F.col("visual_id") == hist1_visual_id)
            .select("container_id")
            .distinct()
            .collect()
        ]
    )
    assert hist1_container_ids == [1, 2, 3]

    count_hist_1_container_1_post = hist_fact_post_case5.where(
        (F.col("visual_id") == hist1_visual_id) & (F.col("container_id") == 1)
    ).count()

    assert (
        count_hist_1_container_1_pre == 31
    )  # 31 bins for container 1 with new definition (0, 8000, 250) for 32 items
    assert (
        count_hist_1_container_1_post == 7999
    )  # 7999 bins for container 1 with new definition (0, 8000, 1) for 8000 items

    # hist2 facts should be unchanged (no definition change, no expression change)
    hist2_fact_post_case5 = (
        hist_fact_post_case5.where(
            (F.col("visual_id") == hist2_visual_id) & (F.col("container_id").isin([1, 2]))
        )
        .orderBy("container_id", "bin_id")
        .collect()
    )
    assert (
        hist2_fact_pre_case5 == hist2_fact_post_case5
    ), "hist2 (speed_hist_p1) facts should be unchanged for containers 1 and 2 as definition and expressions didn't change"

    assert total_hist_agg_count_pre == 660
    assert total_hist_agg_count_post == 24894

    assert agg_pre_count == 12  # 3 aggregations x 2 files
    assert agg_post_count == 27  # 9 aggregations x 3 files


def add_aggs_to_report(my_report):
    # Definition of relevant channels
    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")
    # Definition of 1st page
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)
    # RPM histogram
    hist1_name = "rpm_hist_p1"
    hist1_bins = [float(i) for i in range(0, 8000, 250)]
    hist1 = HistogramDuration(hist1_name, base_expr=c1, bins=hist1_bins)
    my_first_page.add_aggregation(hist1)
    # Speed histogram
    hist2_name = "speed_hist_p1"
    hist2_bins = [float(i) for i in range(0, 300, 1)]
    hist2 = HistogramDuration(name=hist2_name, base_expr=c2, bins=hist2_bins)
    my_first_page.add_aggregation(hist2)
    engine_rpm_event_expr = c1 > 0

    # Add event to report
    engine_rpm_event = BasicEvent(
        name="rpm_event", expr=engine_rpm_event_expr, desc="engine speed > 0 rpm"
    )

    stats_agg = StatsAggregator(
        name="stats_agg",
        input_expressions=[c1],
        channel_names=["Engine RPM"],
        event=engine_rpm_event,
        statistics=["start", "end", "mean"],
    )

    container_event = ContainerEvent("Measurement Event")
    stats_agg_2 = StatsAggregator(
        name="stats_agg_container",
        input_expressions=[c1],
        channel_names=["Engine RPM"],
        event=container_event,
        statistics=["start", "end", "mean"],
    )

    my_report.add_event(engine_rpm_event)

    my_report.add_event(container_event)

    my_first_page.add_aggregation(stats_agg)

    my_first_page.add_aggregation(stats_agg_2)


def add_aggs_to_report_changed_bins(my_report):
    """Same as add_aggs_to_report but with changed bins for RPM histogram (step 500 instead of 250)."""
    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)
    # RPM histogram with DIFFERENT bins (500 step instead of 250)
    hist1_name = "rpm_hist_p1"
    hist1_bins = [float(i) for i in range(0, 8000, 1)]
    hist1 = HistogramDuration(hist1_name, base_expr=c1, bins=hist1_bins)
    my_first_page.add_aggregation(hist1)
    # Speed histogram UNCHANGED
    hist2_name = "speed_hist_p1"
    hist2_bins = [float(i) for i in range(0, 300, 1)]
    hist2 = HistogramDuration(name=hist2_name, base_expr=c2, bins=hist2_bins)
    my_first_page.add_aggregation(hist2)

    engine_rpm_event_expr = c1 > 0

    # Add event to report
    engine_rpm_event = BasicEvent(
        name="rpm_event", expr=engine_rpm_event_expr, desc="engine speed > 0 rpm"
    )
    stats_agg = StatsAggregator(
        name="stats_agg",
        input_expressions=[c1],
        channel_names=["Engine RPM"],
        event=engine_rpm_event,
        statistics=["start", "end", "mean", "median", "min", "max"],
    )

    my_report.add_event(engine_rpm_event)
    my_first_page.add_aggregation(stats_agg)

    container_event = ContainerEvent("Measurement Event")
    stats_agg_2 = StatsAggregator(
        name="stats_agg_container",
        input_expressions=[c1],
        channel_names=["Engine RPM"],
        event=container_event,
        statistics=["start", "end", "mean"],
    )

    my_report.add_event(container_event)
    my_first_page.add_aggregation(stats_agg_2)


def add_aggs_to_report_changed_bins_v2(my_report):
    """Same as add_aggs_to_report but with further changed bins for RPM histogram (step 1000)."""
    query = my_report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")
    my_first_page = Page(page_number=1)
    my_report.add_page(my_first_page)
    # RPM histogram with DIFFERENT bins (1000 step)
    hist1_name = "rpm_hist_p1"
    hist1_bins = [float(i) for i in range(0, 8000, 1000)]
    hist1 = HistogramDuration(hist1_name, base_expr=c1, bins=hist1_bins)
    my_first_page.add_aggregation(hist1)
    # Speed histogram UNCHANGED
    hist2_name = "speed_hist_p1"
    hist2_bins = [float(i) for i in range(0, 300, 1)]
    hist2 = HistogramDuration(name=hist2_name, base_expr=c2, bins=hist2_bins)
    my_first_page.add_aggregation(hist2)


def test_incremental_cross_type_event_definition_change(spark):
    """When both a BasicEvent and a SequenceOfEvents have changed definitions,
    both event types' fact data must survive in the shared event_instance_fact
    table after incremental persistence."""
    from impulse_reporting.events.sequence_of_events import SequenceOfEvents

    # --- Run 1: initial population with BasicEvent + SequenceOfEvents ---
    report_1 = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(set_config("container_metrics_inc_1_2", False)),
    )
    query = report_1.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")
    page = Page(page_number=1)
    report_1.add_page(page)

    basic_event = BasicEvent(name="rpm_event", expr=c1 > 0)
    seq_event = SequenceOfEvents(
        name="rpm_then_speed",
        expressions=[c1 > 0, c2 > 1],
    )
    report_1.add_event(basic_event)
    report_1.add_event(seq_event)

    hist = HistogramDuration(
        "rpm_hist_p1", base_expr=c1, bins=[float(i) for i in range(0, 8000, 250)]
    )
    page.add_aggregation(hist)

    report_1.determine_report()
    report_1.persist_results()

    event_fact_run1 = spark.read.table("spark_catalog.gold.evaluation_event_instance_fact")
    basic_event_id = basic_event.get_id()
    seq_event_id = seq_event.get_id()

    basic_count_run1 = event_fact_run1.where(F.col("event_id") == basic_event_id).count()
    seq_count_run1 = event_fact_run1.where(F.col("event_id") == seq_event_id).count()
    assert basic_count_run1 > 0, "BasicEvent should have fact rows after run 1"
    assert seq_count_run1 > 0, "SequenceOfEvents should have fact rows after run 1"

    # --- Run 2: change both definitions ---
    # BasicEvent: changed expression (c1 > 100 instead of c1 > 0)
    # SequenceOfEvents: changed expressions (c1 > 100, c2 > 10 instead of c1 > 0, c2 > 1)
    report_2 = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(set_config("container_metrics_inc_1_2", True)),
    )
    query2 = report_2.get_db().query
    c1_v2 = query2.channel(channel_name="Engine RPM")
    c2_v2 = query2.channel(channel_name="Vehicle Speed Sensor")
    page2 = Page(page_number=1)
    report_2.add_page(page2)

    basic_event_v2 = BasicEvent(name="rpm_event", expr=c1_v2 > 100)
    seq_event_v2 = SequenceOfEvents(
        name="rpm_then_speed",
        expressions=[c1_v2 > 100, c2_v2 > 10],
    )
    report_2.add_event(basic_event_v2)
    report_2.add_event(seq_event_v2)

    hist2 = HistogramDuration(
        "rpm_hist_p1", base_expr=c1_v2, bins=[float(i) for i in range(0, 8000, 250)]
    )
    page2.add_aggregation(hist2)

    report_2.determine_report()
    report_2.persist_results()

    event_fact_run2 = spark.read.table("spark_catalog.gold.evaluation_event_instance_fact")

    basic_count_run2 = event_fact_run2.where(F.col("event_id") == basic_event_id).count()
    seq_count_run2 = event_fact_run2.where(F.col("event_id") == seq_event_id).count()

    assert (
        basic_count_run2 > 0
    ), "BasicEvent fact data must survive after cross-type incremental persistence"
    assert (
        seq_count_run2 > 0
    ), "SequenceOfEvents fact data must survive after cross-type incremental persistence"


def test_incremental_long_seeded_event_fact_accepts_double_event(spark):
    """A mixture of event types (PointsInTimeEvent, ContainerEvent, BasicEvent) added across
    incremental runs must keep a consistent event_instance_fact output structure.

    Run 1 seeds the shared table with PointsInTimeEvent + ContainerEvent; run 2 adds a new
    BasicEvent incrementally. All event types must agree on the ``start_ts``/``end_ts`` type
    (double) so the run-2 write fits the seeded table.
    """
    # --- Run 1 (seed): only long-emitting event types (PointsInTimeEvent + ContainerEvent) ---
    report_1 = Report(
        name="long_seeded_event_fact_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(set_config("container_metrics_inc_1_2", False)),
    )
    eng_rpm = report_1.get_db().query.channel(channel_name="Engine RPM")
    pit_event = PointsInTimeEvent(name="rpm_rising", expr=eng_rpm.rising_edges())
    container_event = ContainerEvent(name="full_measurement")
    report_1.add_event(pit_event)
    report_1.add_event(container_event)
    page = Page(page_number=1)
    report_1.add_page(page)
    page.add_aggregation(
        PointValueAggregator(
            name="rpm_at_edges",
            input_expressions=[eng_rpm],
            channel_names=["Engine RPM"],
            event=pit_event,
        )
    )
    report_1.determine_report()
    report_1.persist_results()

    event_fact_run1 = spark.read.table("spark_catalog.gold.evaluation_event_instance_fact")
    pit_event_id = pit_event.get_id()
    container_event_id = container_event.get_id()
    assert (
        event_fact_run1.where(F.col("event_id") == pit_event_id).count() > 0
    ), "PointsInTimeEvent should have fact rows after run 1"
    assert (
        event_fact_run1.where(F.col("event_id") == container_event_id).count() > 0
    ), "ContainerEvent should have fact rows after run 1"

    # --- Run 2 (incremental): keep the long events, add a NEW (double-emitting) BasicEvent ---
    report_2 = Report(
        name="long_seeded_event_fact_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(set_config("container_metrics_inc_1_2", True)),
    )
    eng_rpm_2 = report_2.get_db().query.channel(channel_name="Engine RPM")
    pit_event_2 = PointsInTimeEvent(name="rpm_rising", expr=eng_rpm_2.rising_edges())
    container_event_2 = ContainerEvent(name="full_measurement")
    basic_event = BasicEvent(name="rpm_gt_0", expr=eng_rpm_2 > 0)
    report_2.add_event(pit_event_2)
    report_2.add_event(container_event_2)
    report_2.add_event(basic_event)
    page_2 = Page(page_number=1)
    report_2.add_page(page_2)
    page_2.add_aggregation(
        PointValueAggregator(
            name="rpm_at_edges",
            input_expressions=[eng_rpm_2],
            channel_names=["Engine RPM"],
            event=pit_event_2,
        )
    )
    page_2.add_aggregation(
        StatsAggregator(
            name="rpm_stats",
            input_expressions=[eng_rpm_2],
            channel_names=["Engine RPM"],
            statistics=["min", "max"],
            event=basic_event,
        )
    )
    report_2.determine_report()
    report_2.persist_results()

    event_fact_run2 = spark.read.table("spark_catalog.gold.evaluation_event_instance_fact")
    basic_event_id = basic_event.get_id()

    # The long-seeded event types AND the newly-added double BasicEvent all survive together.
    assert (
        event_fact_run2.where(F.col("event_id") == pit_event_id).count() > 0
    ), "PointsInTimeEvent fact data must survive incremental persistence"
    assert (
        event_fact_run2.where(F.col("event_id") == container_event_id).count() > 0
    ), "ContainerEvent fact data must survive incremental persistence"
    assert (
        event_fact_run2.where(F.col("event_id") == basic_event_id).count() > 0
    ), "newly-added BasicEvent must be persisted into the shared event_instance_fact"

    # The shared timestamp columns are double across every event type (not long-seeded).
    assert event_fact_run2.schema["start_ts"].dataType == T.DoubleType()
    assert event_fact_run2.schema["end_ts"].dataType == T.DoubleType()


# ---------------------------------------------------------------------------
# Multi-family shrink: one modified container producing FEWER rows under
# UNCHANGED definitions must have its surplus rows deleted across every fact
# table (calc channel + event + event-scoped stats), while non-reprocessed
# containers stay byte-identical.
# ---------------------------------------------------------------------------

_CC_FACT = "spark_catalog.gold.evaluation_calculated_channel_fact"
_EVENT_FACT = "spark_catalog.gold.evaluation_event_instance_fact"
_STATS_FACT = "spark_catalog.gold.evaluation_stats_aggregator_fact"


def _shrink_config(container_metrics_table, channels_uri, is_enabled):
    return ImpulseConfig(
        source=Source(
            container_metrics_table=container_metrics_table,
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri=channels_uri,
        ),
        unity_sink=UnitySink(catalog="spark_catalog", schema="gold", table_prefix="evaluation"),
        incremental=IncrementalConfig(
            enabled=is_enabled,
            silver_last_modified_column="timestamp",
            gold_last_modified_column="_created_at",
        ),
    )


def _add_multi_family(report):
    """Register calc channel + event + event-scoped stats on one report.

    - calc channel ``speed_kmh`` = Vehicle Speed Sensor (channel 7) * 3.6 → rows
      1:1 with that channel's silver intervals.
    - ``rpm_high`` = Engine RPM (channel 5) > 1000 → one event instance per
      contiguous crossing above 1000.
    - ``rpm_high_stats`` = min/max of Engine RPM over ``rpm_high`` → one row per
      (instance × statistic).
    Returns ``(calc_channel, event, stats)``.
    """
    q = report.get_db().query
    c1 = q.channel(channel_name="Engine RPM")
    c2 = q.channel(channel_name="Vehicle Speed Sensor")

    calc = CalculatedChannel(
        name="speed_kmh",
        expr=c2 * 3.6,
        identity={"channel_name": "speed_kmh", "data_key": "CALC"},
    )
    report.add_calculated_channel(calc)

    event = BasicEvent(name="rpm_high", expr=c1 > 1000)
    report.add_event(event)

    page = Page(page_number=1)
    report.add_page(page)
    stats = StatsAggregator(
        name="rpm_high_stats",
        input_expressions=[c1],
        channel_names=["Engine RPM"],
        statistics=["min", "max"],
        event=event,
    )
    page.add_aggregation(stats)
    return calc, event, stats


def _rows(spark, table, id_col, entity_id, container_id):
    return (
        spark.read.table(table)
        .filter((F.col(id_col) == entity_id) & (F.col("container_id") == container_id))
        .count()
    )


def _others_snapshot(spark, table, id_col, entity_id, value_cols):
    """Ordered snapshot of the non-container-1 rows for byte-identical comparison."""
    return (
        spark.read.table(table)
        .filter((F.col(id_col) == entity_id) & (F.col("container_id") != 1))
        .select(*value_cols)
        .orderBy(*value_cols)
        .collect()
    )


def test_incremental_modified_container_multi_family_shrink_deletes_stale(spark):
    """A modified container emitting FEWER rows under UNCHANGED definitions must
    leave no stale rows in any fact table, and must not touch other containers.

    One incremental run reprocesses container 1 (bumped ``timestamp``) whose Engine
    RPM (ch 5) and Vehicle Speed Sensor (ch 7) signals are truncated to 5 samples.
    Exact counts are pinned from a probe run:
      - calculated_channel_fact: 50 → 5   (1:1 with the 5 kept ch-7 samples)
      - event_instance_fact:      3 → 1   (``rpm > 1000`` crossings collapse to 1)
      - stats_aggregator_fact:    6 → 2   (1 instance × 1 signal × 2 statistics)
    Containers 2 & 3 keep an old timestamp → not reprocessed → byte-identical.
    """
    # --- Run 1 (full): seed all three fact tables from base silver (3 containers) ---
    r1 = Report(
        name="multi_family_shrink_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(
            _shrink_config(
                "spark_catalog.silver.container_metrics", "spark_catalog.silver.channels", False
            )
        ),
    )
    calc, event, stats = _add_multi_family(r1)
    r1.determine_report()
    r1.persist_results()

    # Baseline container-1 counts (exact) + byte-identical snapshots of 2 & 3.
    assert _rows(spark, _CC_FACT, "channel_id", calc.get_id(), 1) == 50
    assert _rows(spark, _EVENT_FACT, "event_id", event.get_id(), 1) == 3
    assert _rows(spark, _STATS_FACT, "visual_id", stats.get_id(), 1) == 6

    cc_others_before = _others_snapshot(
        spark, _CC_FACT, "channel_id", calc.get_id(), ["container_id", "tstart", "value"]
    )
    event_others_before = _others_snapshot(
        spark, _EVENT_FACT, "event_id", event.get_id(), ["container_id", "start_ts", "end_ts"]
    )
    stats_others_before = _others_snapshot(
        spark,
        _STATS_FACT,
        "visual_id",
        stats.get_id(),
        ["container_id", "aggregation_label", "statistic_value"],
    )

    shrink_cm = "spark_catalog.silver.container_metrics_multi_shrink"
    shrink_channels = "spark_catalog.silver.channels_multi_shrink"
    try:
        # Modified silver: container 1 updated (bumped timestamp) with Engine RPM
        # (ch 5) and Vehicle Speed (ch 7) truncated to the first 5 samples; 2 & 3
        # keep the old timestamp so they are skipped.
        clone_silver_with_shrunk_container(
            spark,
            updated_container_id=1,
            shrink_channel_ids=[5, 7],
            keep_n=5,
            cm_table=shrink_cm,
            channels_table=shrink_channels,
        )

        # --- Run 2 (incremental): same definitions (unchanged path) ---
        r2 = Report(
            name="multi_family_shrink_report",
            spark=spark,
            workspace_client=create_autospec(WorkspaceClient),
            config=dict(_shrink_config(shrink_cm, shrink_channels, True)),
        )
        calc2, event2, stats2 = _add_multi_family(r2)
        # Same identities → unchanged definitions → the incremental (not replace) path.
        assert calc2.get_id() == calc.get_id()
        assert event2.get_id() == event.get_id()
        assert stats2.get_id() == stats.get_id()
        r2.determine_report()
        r2.persist_results()

        # Container 1 shrank on every fact table — no stale orphans survive.
        assert _rows(spark, _CC_FACT, "channel_id", calc.get_id(), 1) == 5
        assert _rows(spark, _EVENT_FACT, "event_id", event.get_id(), 1) == 1
        assert _rows(spark, _STATS_FACT, "visual_id", stats.get_id(), 1) == 2

        # Containers 2 & 3 were not reprocessed → byte-identical across all tables.
        assert (
            _others_snapshot(
                spark, _CC_FACT, "channel_id", calc.get_id(), ["container_id", "tstart", "value"]
            )
            == cc_others_before
        )
        assert (
            _others_snapshot(
                spark,
                _EVENT_FACT,
                "event_id",
                event.get_id(),
                ["container_id", "start_ts", "end_ts"],
            )
            == event_others_before
        )
        assert (
            _others_snapshot(
                spark,
                _STATS_FACT,
                "visual_id",
                stats.get_id(),
                ["container_id", "aggregation_label", "statistic_value"],
            )
            == stats_others_before
        )
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {shrink_cm}")
        spark.sql(f"DROP TABLE IF EXISTS {shrink_channels}")
