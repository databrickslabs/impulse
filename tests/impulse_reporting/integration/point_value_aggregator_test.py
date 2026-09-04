import os
from unittest.mock import create_autospec

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient

from impulse_reporting.aggregations.point_value_aggregator import PointValueAggregator
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.config.config_parser import (
    ImpulseConfig,
    IncrementalConfig,
    Source,
    UnitySink,
)
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent


def _config_path():
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    return os.path.join(base_path, "tests", "data", "config", "config.json")


def _incremental_config(is_enabled: bool):
    """Incremental-capable config writing to the shared ``evaluation_*`` gold tables."""
    return ImpulseConfig(
        source=Source(
            container_metrics_table="spark_catalog.silver.container_metrics_inc_1_2",
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
    pit_instances = event_fact.filter(F.col("event_id") == pit_event.get_id())
    assert pva_rows.count() > 0

    # every fact row is a single sampled value of the right channel and event
    assert pva_rows.filter(F.col("aggregation_label") != "value").count() == 0
    assert pva_rows.filter(F.col("channel_name") != "Engine RPM").count() == 0
    assert pva_rows.filter(F.col("event_id") != pit_event.get_id()).count() == 0

    # sampled values are real Engine RPM measurements (non-null and positive at rising edges)
    assert pva_rows.filter(F.col("statistic_value").isNull()).count() == 0
    vmin, vmax = pva_rows.agg(F.min("statistic_value"), F.max("statistic_value")).first()
    assert vmin > 0 and vmax >= vmin

    # bijection on event_instance_id: every instant produced exactly one value, and every
    # value links back to a (zero-duration) PointsInTimeEvent instance
    assert (
        pva_rows.join(
            pit_instances.select("event_instance_id").distinct(),
            on="event_instance_id",
            how="left_anti",
        ).count()
        == 0
    )
    assert (
        pit_instances.join(
            pva_rows.select("event_instance_id").distinct(),
            on="event_instance_id",
            how="left_anti",
        ).count()
        == 0
    )
    assert pit_instances.filter(F.col("start_ts") != F.col("end_ts")).count() == 0

    # dimension row persisted with the right metadata
    dim_rows = dim.filter(F.col("name") == "rpm_at_edges").collect()
    assert len(dim_rows) == 1
    assert dim_rows[0]["agg_type"] == "point_value_aggregator"
    assert dim_rows[0]["channel_names"] == ["Engine RPM"]
    assert dim_rows[0]["statistics"] == ["value"]


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
    dim = spark.read.table("spark_catalog.gold.evaluation_stats_aggregator_dimension")

    stats_rows = fact.filter(F.col("visual_id") == stats.get_id())
    pva_rows = fact.filter(F.col("visual_id") == pva.get_id())

    # both aggregators survive in the shared fact table with their own rows
    assert stats_rows.count() > 0
    assert pva_rows.count() > 0

    # each type kept its own aggregation labels (no clobber, no cross-contamination)
    stats_labels = {
        r["aggregation_label"] for r in stats_rows.select("aggregation_label").distinct().collect()
    }
    pva_labels = {
        r["aggregation_label"] for r in pva_rows.select("aggregation_label").distinct().collect()
    }
    assert stats_labels == {"min", "max"}
    assert pva_labels == {"value"}

    # both dimension rows survive with the correct agg_type
    agg_types = {
        r["name"]: r["agg_type"]
        for r in dim.filter(F.col("name").isin("rpm_stats", "rpm_at_edges")).collect()
    }
    assert agg_types == {
        "rpm_stats": "stats_aggregator",
        "rpm_at_edges": "point_value_aggregator",
    }


def _add_stats_and_pva(report, *, statistics, pva_scale):
    """Add a StatsAggregator + PointValueAggregator (both → stats_aggregator_fact).

    Events are identical across runs (stable ``event_instance_fact``); only the
    aggregation definitions vary: ``statistics`` changes the stats hash and
    ``pva_scale`` changes the PVA's input expression (and thus its hash), so both
    aggregation types land in the changed-DF union on the shared fact table.
    """
    q = report.get_db().query
    eng_rpm = q.channel(channel_name="Engine RPM")

    rpm_event = BasicEvent(name="rpm_gt_0", expr=eng_rpm > 0)
    pit_event = PointsInTimeEvent(name="rpm_rising", expr=eng_rpm.rising_edges())
    report.add_event(rpm_event)
    report.add_event(pit_event)

    page = Page(page_number=1)
    report.add_page(page)
    stats = StatsAggregator(
        name="rpm_stats",
        input_expressions=[eng_rpm],
        channel_names=["Engine RPM"],
        statistics=statistics,
        event=rpm_event,
    )
    pva = PointValueAggregator(
        name="rpm_at_edges",
        input_expressions=[eng_rpm * pva_scale],
        channel_names=["Engine RPM"],
        event=pit_event,
    )
    page.add_aggregation(stats)
    page.add_aggregation(pva)
    return stats, pva


def test_incremental_stats_and_point_value_shared_fact_changed_defs(spark):
    """Two aggregation types sharing stats_aggregator_fact, both with CHANGED
    definitions, persisted incrementally.

    Exercises the aggregation branch of ``persist_facts_incremental`` where the
    changed DataFrames of *different* types on one fact table are ``unionByName``
    -combined into a single ``merge_incremental`` (delete scope on ``visual_id``)
    — the cross-type union must not clobber either type's rows.
    """
    # --- Run 1: seed both aggregators into the shared fact table ---
    report_1 = Report(
        name="agg_shared_fact_incremental",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_incremental_config(is_enabled=False)),
    )
    stats_1, pva_1 = _add_stats_and_pva(report_1, statistics=["min", "max"], pva_scale=1.0)
    report_1.determine_report()
    report_1.persist_results()

    fact_run1 = spark.read.table("spark_catalog.gold.evaluation_stats_aggregator_fact")
    assert fact_run1.where(F.col("visual_id") == stats_1.get_id()).count() > 0
    assert fact_run1.where(F.col("visual_id") == pva_1.get_id()).count() > 0

    # --- Run 2 (incremental): change BOTH definitions ---
    # stats: expanded statistics list; PVA: changed event expression threshold.
    report_2 = Report(
        name="agg_shared_fact_incremental",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_incremental_config(is_enabled=True)),
    )
    stats_2, pva_2 = _add_stats_and_pva(report_2, statistics=["min", "max", "mean"], pva_scale=2.0)
    # same identities → same visual_ids across runs
    assert stats_2.get_id() == stats_1.get_id()
    assert pva_2.get_id() == pva_1.get_id()

    report_2.determine_report()
    report_2.persist_results()

    fact_run2 = spark.read.table("spark_catalog.gold.evaluation_stats_aggregator_fact")
    stats_rows = fact_run2.where(F.col("visual_id") == stats_2.get_id())
    pva_rows = fact_run2.where(F.col("visual_id") == pva_2.get_id())

    # Both types survive the cross-type union + single merge_incremental (no clobber).
    assert stats_rows.count() > 0
    assert pva_rows.count() > 0
    # The changed stats definition took effect: the expanded statistics are present.
    stats_labels = {
        r["aggregation_label"] for r in stats_rows.select("aggregation_label").distinct().collect()
    }
    assert stats_labels == {"min", "max", "mean"}
    assert {
        r["aggregation_label"] for r in pva_rows.select("aggregation_label").distinct().collect()
    } == {"value"}
