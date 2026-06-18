import os
from unittest.mock import create_autospec

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient

from impulse_reporting.aggregations.point_value_aggregator import PointValueAggregator
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent


def _config_path():
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    return os.path.join(base_path, "tests", "data", "config", "config.json")


def test_persist_point_value_aggregator(spark):
    """End-to-end: a PointValueAggregator samples a channel at a PointsInTimeEvent's instants,
    writes to stats_aggregator_fact, and its rows link to the event's instances."""
    my_report: Report = Report(
        name="pva_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=_config_path(),
    )

    eng_rpm = my_report.get_db().query.channel(channel_name="Engine RPM")
    pit_event = PointsInTimeEvent(name="rpm_rising_edges", expr=eng_rpm.rising_edges())
    my_report.add_event(pit_event)

    page = Page(page_number=1)
    my_report.add_page(page)
    pva = PointValueAggregator(
        name="rpm_at_edges",
        input_expressions=[eng_rpm],
        channel_names=["Engine RPM"],
        event=pit_event,
        desc="Engine RPM sampled at rising edges",
    )
    page.add_aggregation(pva)

    my_report.determine_report()

    assert "POINT_VALUE_AGGREGATOR" in my_report.aggregation_dfs
    assert my_report.aggregation_dfs["POINT_VALUE_AGGREGATOR"]["changed"].count() > 0
    assert my_report.aggregation_metadata_dfs["POINT_VALUE_AGGREGATOR"].count() == 1

    my_report.persist_results()

    fact = spark.read.table("spark_catalog.gold.evaluation_stats_aggregator_fact")
    dim = spark.read.table("spark_catalog.gold.evaluation_stats_aggregator_dimension")
    event_fact = spark.read.table("spark_catalog.gold.evaluation_event_instance_fact")

    pva_rows = fact.filter(F.col("visual_id") == pva.get_id())
    assert pva_rows.count() > 0
    # every row is a single sampled value
    assert pva_rows.filter(F.col("aggregation_label") != "value").count() == 0
    assert pva_rows.filter(F.col("statistic_value").isNull()).count() == 0

    # every aggregator row's event_instance_id links to a PointsInTimeEvent instance
    pit_instances = event_fact.filter(F.col("event_id") == pit_event.get_id())
    unmatched = pva_rows.join(
        pit_instances.select("event_instance_id").distinct(),
        on="event_instance_id",
        how="left_anti",
    )
    assert unmatched.count() == 0

    # dimension row persisted with the right agg_type
    assert (
        dim.filter(
            (F.col("name") == "rpm_at_edges") & (F.col("agg_type") == "point_value_aggregator")
        ).count()
        == 1
    )


def test_stats_and_point_value_share_fact_table_without_clobber(spark):
    """A StatsAggregator and a PointValueAggregator both persist to stats_aggregator_fact;
    grouping by table must keep both (no overwrite)."""
    my_report: Report = Report(
        name="shared_fact_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=_config_path(),
    )

    eng_rpm = my_report.get_db().query.channel(channel_name="Engine RPM")
    rpm_event = BasicEvent(name="rpm_gt_0", expr=eng_rpm > 0)
    pit_event = PointsInTimeEvent(name="rpm_rising", expr=eng_rpm.rising_edges())
    my_report.add_event(rpm_event)
    my_report.add_event(pit_event)

    page = Page(page_number=1)
    my_report.add_page(page)
    stats = StatsAggregator(
        name="rpm_stats",
        input_expressions=[eng_rpm],
        channel_names=["Engine RPM"],
        statistics=["min", "max"],
        event=rpm_event,
    )
    pva = PointValueAggregator(
        name="rpm_at_edges",
        input_expressions=[eng_rpm],
        channel_names=["Engine RPM"],
        event=pit_event,
    )
    page.add_aggregation(stats)
    page.add_aggregation(pva)

    my_report.determine_report()
    my_report.persist_results()

    fact = spark.read.table("spark_catalog.gold.evaluation_stats_aggregator_fact")
    visual_ids = {row.visual_id for row in fact.select("visual_id").distinct().collect()}
    # both aggregators survive in the shared fact table
    assert stats.get_id() in visual_ids
    assert pva.get_id() in visual_ids
