import os
from unittest.mock import create_autospec

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient

from impulse_reporting.core.report import Report
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent


def test_persist_points_in_time_event(spark):
    """End-to-end: a PointsInTimeEvent writes zero-duration rows to event_instance_fact, and its
    event_type lands in event_dimension."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    config_path = os.path.join(base_path, "tests", "data", "config", "config.json")

    my_report: Report = Report(
        name="my_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=config_path,
    )

    eng_rpm = my_report.get_db().query.channel(channel_name="Engine RPM")
    pit_event = PointsInTimeEvent(
        name="rpm_rising_edges",
        expr=eng_rpm.rising_edges(),
        desc="Engine RPM rising edges",
    )
    my_report.add_event(pit_event)

    my_report.determine_report()

    event_dfs = my_report.event_dfs
    event_metadata_dfs = my_report.event_metadata_dfs
    assert event_dfs["POINTS_IN_TIME_EVENT"]["changed"].count() > 0
    assert event_metadata_dfs["POINTS_IN_TIME_EVENT"].count() > 0

    my_report.persist_results()

    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_event_instance_fact")
    assert spark.catalog.tableExists("spark_catalog.gold.evaluation_event_dimension")

    fact = spark.read.table("spark_catalog.gold.evaluation_event_instance_fact")
    dim = spark.read.table("spark_catalog.gold.evaluation_event_dimension")

    point_rows = fact.filter(F.col("event_id") == pit_event.get_id())
    assert point_rows.count() > 0
    # every point-in-time instance is a zero-duration row
    assert point_rows.filter(F.col("start_ts") != F.col("end_ts")).count() == 0

    # event_type is persisted in the dimension table (value-level check)
    assert (
        dim.filter(
            (F.col("event_name") == "rpm_rising_edges")
            & (F.col("event_type") == "POINTS_IN_TIME_EVENT")
        ).count()
        == 1
    )
