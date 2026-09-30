"""Integration tests for TimeWindowEvent with end-to-end Report usage."""

from unittest.mock import create_autospec

import pyspark.sql.functions as F
import pytest
from databricks.sdk import WorkspaceClient

from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.config.config_parser import (
    Comparator,
    ContainerFilters,
    ImpulseConfig,
    MetricFilter,
    QueryEngine,
    Solvers,
    Source,
    UnitySink,
)
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.time_window_event import TimeWindowEvent
from tests.conftest import setup_basic_db, spark  # noqa: F401  (pytest fixtures)

# Container boundaries (epoch ms) for the Seat_Leon measurements in
# container_metrics.csv, as documented in container_event_test.py.
# c1: span 107545 ms, c2: 108752 ms, c3: 110083 ms
EXPECTED_CONTAINERS = {
    1: {"start_ts": 1751528502708, "stop_ts": 1751528610253},
    2: {"start_ts": 1751528501483, "stop_ts": 1751528610235},
    3: {"start_ts": 1751528500169, "stop_ts": 1751528610252},
}
WINDOW_LENGTH = 10_000  # 10 seconds, in epoch-ms units


def _config(table_prefix: str) -> ImpulseConfig:
    return ImpulseConfig(
        source=Source(
            container_metrics_table="spark_catalog.silver.container_metrics",
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri="spark_catalog.silver.channels",
        ),
        unity_sink=UnitySink(
            catalog="spark_catalog",
            schema="gold",
            table_prefix=table_prefix,
        ),
        container_filters=ContainerFilters(
            metric_filters=[
                [
                    MetricFilter(
                        column_name="vehicle_key", comparator=Comparator.EQ, value="Seat_Leon"
                    ),
                    MetricFilter(
                        column_name="start_dt",
                        comparator=Comparator.GE,
                        value="2025-07-03T07:00:00.000Z",
                    ),
                ]
            ]
        ),
        query_engine=QueryEngine(solver=Solvers.KEY_VALUE_STORE_SOLVER),
        measurement_dimensions=["container_id", "start_ts", "stop_ts"],
    )


def _expected_window_count(container_id: int) -> int:
    span = (
        EXPECTED_CONTAINERS[container_id]["stop_ts"]
        - EXPECTED_CONTAINERS[container_id]["start_ts"]
    )
    return -(-span // WINDOW_LENGTH)  # ceil division


def test_time_window_event_in_report(spark, basic_narrow_db):
    """A TimeWindowEvent (co-solved with an aggregation) tiles each container into windows."""
    my_report = Report(
        name="time_window_event_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config("time_window_event_test")),
    )

    window_evt = TimeWindowEvent(
        name="ten_sec", window_length=WINDOW_LENGTH, desc="Ten second windows"
    )
    my_report.add_event(window_evt)

    # A channel-bearing aggregation must be present so the batched solve forms
    # per-container groups the (selector-less) window expression can ride on.
    query = my_report.get_db().query
    page = Page(page_number=1)
    my_report.add_page(page)
    page.add_aggregation(
        StatsAggregator(
            name="rpm_stats_per_window",
            input_expressions=[query.channel(channel_name="Engine RPM")],
            channel_names=["Engine RPM"],
            statistics=["min", "max", "mean"],
            event=window_evt,
            desc="Engine RPM stats per window",
        )
    )

    my_report.determine_report()

    event_dfs = my_report.event_dfs
    assert "TIME_WINDOW_EVENT" in event_dfs
    rows = event_dfs["TIME_WINDOW_EVENT"]["changed"].collect()

    total_expected = sum(_expected_window_count(cid) for cid in EXPECTED_CONTAINERS)
    assert len(rows) == total_expected

    for container_id, expected in EXPECTED_CONTAINERS.items():
        windows = sorted(
            ((r.start_ts, r.end_ts) for r in rows if r.container_id == container_id),
            key=lambda w: w[0],
        )
        assert len(windows) == _expected_window_count(container_id)
        # First window starts at the container start.
        assert windows[0][0] == expected["start_ts"]
        # Windows are contiguous: each end equals the next start.
        for (_, end), (nxt_start, _) in zip(windows, windows[1:], strict=False):
            assert end == nxt_start
        # Final window is clamped to the container stop.
        assert windows[-1][1] == expected["stop_ts"]
        # Every window is a valid, non-empty interval.
        assert all(start < end for start, end in windows)
        # Per-window instances are distinct (unlike ContainerEvent's single id).
        instance_ids = [r.event_instance_id for r in rows if r.container_id == container_id]
        assert len(set(instance_ids)) == len(instance_ids)

    dim_rows = my_report.event_metadata_dfs["TIME_WINDOW_EVENT"].collect()
    assert len(dim_rows) == 1
    assert dim_rows[0].event_type == "TIME_WINDOW_EVENT"
    assert dim_rows[0].attributes["window_length"] == str(WINDOW_LENGTH)


# Window length (channel-sample time unit, µs) for the aligned-boundary test.
# Per-container sample spans are ~3.9–5.4e9 µs, so 600s (=6e8 µs) yields several
# windows per container, each overlapping RPM samples.
ALIGNED_WINDOW_LENGTH = 600_000_000
_ALIGNED_SCHEMA = "spark_catalog.silver_tw_aligned"


@pytest.fixture
def setup_tw_aligned_db(spark, setup_basic_db):  # noqa: F811
    """Silver tables cloned from the basic db with container_metrics start_ts / stop_ts
    recomputed from each container's channel-sample range, so the container boundaries
    (and thus the windows) share the samples' time base."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_ALIGNED_SCHEMA}")
    channels = spark.read.table("spark_catalog.silver.channels")
    bounds = channels.groupBy("container_id").agg(
        F.min("tstart").alias("_agg_start"),
        F.max("tend").alias("_agg_stop"),
    )
    container_metrics = spark.read.table("spark_catalog.silver.container_metrics")
    start_type = container_metrics.schema["start_ts"].dataType
    stop_type = container_metrics.schema["stop_ts"].dataType
    aligned_cm = (
        container_metrics.join(bounds, on="container_id", how="left")
        .withColumn("start_ts", F.coalesce("_agg_start", "start_ts").cast(start_type))
        .withColumn("stop_ts", F.coalesce("_agg_stop", "stop_ts").cast(stop_type))
        .drop("_agg_start", "_agg_stop")
    )
    aligned_cm.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(f"{_ALIGNED_SCHEMA}.container_metrics")
    for table in ("channel_metrics", "channels"):
        spark.read.table(f"spark_catalog.silver.{table}").write.format("delta").mode(
            "overwrite"
        ).saveAsTable(f"{_ALIGNED_SCHEMA}.{table}")
    yield
    spark.sql(f"DROP SCHEMA IF EXISTS {_ALIGNED_SCHEMA} CASCADE")


def test_time_window_event_aggregation_join(spark, setup_tw_aligned_db):
    """Stats scoped to a TimeWindowEvent yield per-window values that join to the fact."""
    config = dict(
        ImpulseConfig(
            source=Source(
                container_metrics_table=f"{_ALIGNED_SCHEMA}.container_metrics",
                channel_metrics_table=f"{_ALIGNED_SCHEMA}.channel_metrics",
                channels_uri=f"{_ALIGNED_SCHEMA}.channels",
            ),
            unity_sink=UnitySink(
                catalog="spark_catalog", schema="gold", table_prefix="time_window_join_test"
            ),
            container_filters=ContainerFilters(
                metric_filters=[
                    [
                        MetricFilter(
                            column_name="vehicle_key", comparator=Comparator.EQ, value="Seat_Leon"
                        )
                    ]
                ]
            ),
            query_engine=QueryEngine(solver=Solvers.KEY_VALUE_STORE_SOLVER),
            measurement_dimensions=["container_id", "start_ts", "stop_ts"],
        )
    )
    my_report = Report(
        name="time_window_join_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=config,
    )

    window_evt = TimeWindowEvent(name="ten_min", window_length=ALIGNED_WINDOW_LENGTH)
    my_report.add_event(window_evt)

    query = my_report.get_db().query
    page = Page(page_number=1)
    my_report.add_page(page)
    page.add_aggregation(
        StatsAggregator(
            name="rpm_stats_per_window",
            input_expressions=[query.channel(channel_name="Engine RPM")],
            channel_names=["Engine RPM"],
            statistics=["min", "max", "mean"],
            event=window_evt,
            desc="Engine RPM stats per window",
        )
    )

    my_report.determine_report()
    my_report.persist_results()

    stats_fact = spark.read.table("spark_catalog.gold.time_window_join_test_stats_aggregator_fact")
    event_instance_fact = spark.read.table(
        "spark_catalog.gold.time_window_join_test_event_instance_fact"
    )
    assert stats_fact.count() > 0
    assert event_instance_fact.count() > 0

    # Real computed values: with aligned boundaries every window overlaps RPM samples,
    # so each produces a positive max.
    max_values = [
        r.statistic_value
        for r in stats_fact.filter(F.col("aggregation_label") == "max").collect()
        if r.statistic_value is not None
    ]
    assert len(max_values) > 0
    assert any(v > 0 for v in max_values)

    stats_event_ids = {
        r.event_instance_id
        for r in stats_fact.filter(F.col("event_instance_id").isNotNull())
        .select("event_instance_id")
        .distinct()
        .collect()
    }
    event_ids = {
        r.event_instance_id
        for r in event_instance_fact.select("event_instance_id").distinct().collect()
    }
    assert len(stats_event_ids) > 0
    # Every per-window stats instance must map to a materialized window instance.
    assert stats_event_ids.issubset(
        event_ids
    ), f"stats event_instance_ids not in event_instance_fact: {stats_event_ids - event_ids}"


def test_multiple_time_window_events_coexist(spark, basic_narrow_db):
    """Two TimeWindowEvents with different windows are allowed and both materialize."""
    my_report = Report(
        name="time_window_multi_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config("time_window_multi_test")),
    )

    evt_10s = TimeWindowEvent(name="ten_sec", window_length=WINDOW_LENGTH)
    evt_30s = TimeWindowEvent(name="thirty_sec", window_length=3 * WINDOW_LENGTH)
    my_report.add_event(evt_10s)
    my_report.add_event(evt_30s)

    query = my_report.get_db().query
    page = Page(page_number=1)
    my_report.add_page(page)
    page.add_aggregation(
        StatsAggregator(
            name="rpm_stats",
            input_expressions=[query.channel(channel_name="Engine RPM")],
            channel_names=["Engine RPM"],
            statistics=["mean"],
            event=evt_10s,
            desc="Engine RPM stats per 10s window",
        )
    )

    my_report.determine_report()

    rows = my_report.event_dfs["TIME_WINDOW_EVENT"]["changed"].collect()
    names = {r.event_id for r in rows}
    # Two distinct events (distinct event_ids) share the shared fact table.
    assert names == {evt_10s.get_id(), evt_30s.get_id()}

    # The 10s event produces strictly more windows than the 30s event.
    count_10s = sum(1 for r in rows if r.event_id == evt_10s.get_id())
    count_30s = sum(1 for r in rows if r.event_id == evt_30s.get_id())
    assert count_10s > count_30s > 0

    dim_rows = my_report.event_metadata_dfs["TIME_WINDOW_EVENT"].collect()
    assert {d.event_name for d in dim_rows} == {"ten_sec", "thirty_sec"}
