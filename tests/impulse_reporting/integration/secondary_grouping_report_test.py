"""Integration tests for the optional secondary grouping key in reporting.

Verifies that a secondary grouping key configured on the solver flows through the
aggregation transforms into the gold fact tables as a real output dimension
(rows per ``(container_id, secondary_grouping_key)``), becomes part of the fact
merge identity, and is persisted; and that leaving it unconfigured is
byte-for-byte backward compatible.
"""

import os
from unittest.mock import create_autospec

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient

from impulse_query_engine.analyze.query.solvers.solver_config import SecondaryGroupingConfig
from impulse_reporting.aggregations.aggregation_types import AggregationType
from impulse_reporting.aggregations.histogram import HistogramDuration
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
from tests.impulse_reporting.integration.test_helpers import (
    clone_silver_with_shrunk_container,
)

CONFIG = ("tests", "data", "config", "config.json")

_STATS_FACT = "spark_catalog.gold.evaluation_stats_aggregator_fact"
_STATS_IDENTITY = [
    "container_id",
    "secondary_grouping_key",
    "visual_id",
    "aggregation_label",
    "event_instance_id",
    "channel_name",
    "statistic_value",
]


def _config_path() -> str:
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    return os.path.join(base_path, *CONFIG)


def _build_report(spark, name: str) -> Report:
    return Report(
        name=name,
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config_path=_config_path(),
    )


# A time-localized key derived from the channel-data table: partition each
# container's samples by parity of their start timestamp. Column-returning
# derivers run on the driver, so a lambda is fine here.
def _parity_deriver(df):
    return (F.col("tstart") % F.lit(2)).cast("long")


def test_secondary_grouping_subdivides_aggregation_facts(spark):
    """Stats + histogram facts gain a secondary_grouping_key dimension per partition."""
    report = _build_report(spark, "sgk_stats_report")
    report.solver.config.secondary_grouping = SecondaryGroupingConfig(deriver=_parity_deriver)

    query = report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    rpm_event = BasicEvent(name="rpm_event", expr=c1 > 0, desc="Engine RPM > 0")
    report.add_event(rpm_event)

    page = Page(page_number=1)
    report.add_page(page)
    page.add_aggregation(
        StatsAggregator(
            name="rpm_stats",
            input_expressions=[c1],
            channel_names=["Engine RPM"],
            statistics=["min", "max", "mean"],
            event=rpm_event,
            desc="Engine RPM statistics",
        )
    )
    page.add_aggregation(
        HistogramDuration(
            "rpm_histogram", base_expr=c1, bins=[float(i) for i in range(0, 8000, 250)]
        )
    )

    report.determine_report()

    stats_df = report.aggregation_dfs["STATS_AGGREGATOR"]["changed"]
    hist_df = report.aggregation_dfs["HISTOGRAM"]["changed"]

    # The key is present on both fact frames as a genuine output dimension.
    assert "secondary_grouping_key" in stats_df.columns
    assert "secondary_grouping_key" in hist_df.columns

    # It actually took more than one value (real subdivision, not a constant).
    distinct_keys = {
        r.secondary_grouping_key for r in stats_df.select("secondary_grouping_key").collect()
    }
    assert len(distinct_keys) >= 2, distinct_keys

    # Real values survive per partition: a positive histogram mass per key.
    per_key_mass = (
        hist_df.groupBy("secondary_grouping_key").agg(F.sum("hist_value").alias("m")).collect()
    )
    assert per_key_mass and all(r.m > 0 for r in per_key_mass), per_key_mass

    # The merge identity includes the key so upserts target the right row.
    keys = report._get_aggregation_merge_keys(AggregationType.STATS_AGGREGATOR)
    assert "secondary_grouping_key" in keys


def test_secondary_grouping_persists_to_gold(spark):
    """The key column lands in the persisted gold fact table."""
    report = _build_report(spark, "sgk_persist_report")
    report.solver.config.secondary_grouping = SecondaryGroupingConfig(deriver=_parity_deriver)

    query = report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    event = BasicEvent(name="rpm_event", expr=c1 > 0, desc="Engine RPM > 0")
    report.add_event(event)
    page = Page(page_number=1)
    report.add_page(page)
    page.add_aggregation(
        StatsAggregator(
            name="rpm_stats",
            input_expressions=[c1],
            channel_names=["Engine RPM"],
            statistics=["min", "max", "mean"],
            event=event,
            desc="Engine RPM statistics",
        )
    )

    report.determine_report()
    report.persist_results()

    fact = spark.read.table("spark_catalog.gold.evaluation_stats_aggregator_fact")
    assert "secondary_grouping_key" in fact.columns
    assert fact.filter(F.col("secondary_grouping_key").isNotNull()).count() > 0
    # More than one partition made it to gold.
    assert fact.select("secondary_grouping_key").distinct().count() >= 2


def test_no_secondary_grouping_is_backward_compatible(spark):
    """Without configuration, facts carry no secondary_grouping_key column."""
    report = _build_report(spark, "sgk_off_report")
    query = report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    event = BasicEvent(name="rpm_event", expr=c1 > 0, desc="Engine RPM > 0")
    report.add_event(event)
    page = Page(page_number=1)
    report.add_page(page)
    page.add_aggregation(
        StatsAggregator(
            name="rpm_stats",
            input_expressions=[c1],
            channel_names=["Engine RPM"],
            statistics=["min", "max", "mean"],
            event=event,
            desc="Engine RPM statistics",
        )
    )

    report.determine_report()
    stats_df = report.aggregation_dfs["STATS_AGGREGATOR"]["changed"]
    assert "secondary_grouping_key" not in stats_df.columns
    assert stats_df.count() > 0
    assert "secondary_grouping_key" not in report._get_aggregation_merge_keys(
        AggregationType.STATS_AGGREGATOR
    )


def _sgk_config(cm_table: str, channels_table: str, incremental_enabled: bool) -> ImpulseConfig:
    return ImpulseConfig(
        source=Source(
            container_metrics_table=cm_table,
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri=channels_table,
        ),
        unity_sink=UnitySink(catalog="spark_catalog", schema="gold", table_prefix="evaluation"),
        incremental=IncrementalConfig(
            enabled=incremental_enabled,
            silver_last_modified_column="timestamp",
            gold_last_modified_column="_created_at",
        ),
    )


def _sgk_report(spark, name, cm_table, channels_table, incremental_enabled) -> Report:
    report = Report(
        name=name,
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_sgk_config(cm_table, channels_table, incremental_enabled)),
    )
    report.solver.config.secondary_grouping = SecondaryGroupingConfig(deriver=_parity_deriver)
    return report


def _add_rpm_stats(report):
    query = report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    event = BasicEvent(name="rpm_event", expr=c1 > 1000, desc="Engine RPM > 1000")
    report.add_event(event)
    page = Page(page_number=1)
    report.add_page(page)
    page.add_aggregation(
        StatsAggregator(
            name="rpm_stats",
            input_expressions=[c1],
            channel_names=["Engine RPM"],
            statistics=["min", "max"],
            event=event,
            desc="Engine RPM statistics",
        )
    )


def _combined_stats_df(report):
    """Union the changed + unchanged STATS_AGGREGATOR frames a report produced."""
    entry = report.aggregation_dfs["STATS_AGGREGATOR"]
    parts = [entry[k] for k in ("changed", "unchanged") if entry.get(k) is not None]
    df = parts[0]
    for part in parts[1:]:
        df = df.unionByName(part)
    return df


def _stats_identity_rows(df, container_id: int) -> set:
    rows = df.filter(F.col("container_id") == container_id).select(*_STATS_IDENTITY).collect()
    return {
        (
            r.container_id,
            r.secondary_grouping_key,
            r.visual_id,
            r.aggregation_label,
            r.event_instance_id,
            r.channel_name,
            round(r.statistic_value, 6),
        )
        for r in rows
    }


def test_incremental_secondary_grouping_prunes_stale_partitions(spark):
    """Incremental + SGK equals a full recompute of the reprocessed container.

    A modified container is reprocessed under an unchanged definition; the gold
    fact rows for it must exactly match a full (non-incremental) recompute on the
    same silver — proving no stale ``(container, secondary_grouping_key)`` rows
    survive and none are lost (process-and-correct via the SGK-keyed MERGE).
    """
    # Run 1 (full): seed gold from the base silver (all containers).
    r1 = _sgk_report(
        spark,
        "sgk_inc_report",
        "spark_catalog.silver.container_metrics",
        "spark_catalog.silver.channels",
        False,
    )
    _add_rpm_stats(r1)
    r1.determine_report()
    r1.persist_results()
    gold_c1_run1 = _stats_identity_rows(spark.read.table(_STATS_FACT), 1)
    assert gold_c1_run1, "expected container-1 stats rows after the seed run"

    shrink_cm = "spark_catalog.silver.sgk_container_metrics_shrink"
    shrink_channels = "spark_catalog.silver.sgk_channels_shrink"
    try:
        # Modified silver: container 1 updated (bumped timestamp), Engine RPM (ch 5)
        # truncated to its first 5 samples; other containers keep the old timestamp.
        clone_silver_with_shrunk_container(
            spark,
            updated_container_id=1,
            shrink_channel_ids=[5],
            keep_n=5,
            cm_table=shrink_cm,
            channels_table=shrink_channels,
        )

        # Run 2 (incremental): unchanged definition → container 1 reprocessed.
        r2 = _sgk_report(spark, "sgk_inc_report", shrink_cm, shrink_channels, True)
        _add_rpm_stats(r2)
        r2.determine_report()
        r2.persist_results()
        gold_c1_incremental = _stats_identity_rows(spark.read.table(_STATS_FACT), 1)

        # Ground truth: a full recompute on the SAME shrunk silver.
        r3 = _sgk_report(spark, "sgk_gt_report", shrink_cm, shrink_channels, False)
        _add_rpm_stats(r3)
        r3.determine_report()
        full_recompute_c1 = _stats_identity_rows(_combined_stats_df(r3), 1)

        # Incremental gold for the reprocessed container == full recompute: no stale
        # partitions left behind, none missing.
        assert gold_c1_incremental == full_recompute_c1
        # And it actually changed vs the seed run (the container was reprocessed).
        assert gold_c1_incremental != gold_c1_run1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {shrink_cm}")
        spark.sql(f"DROP TABLE IF EXISTS {shrink_channels}")
