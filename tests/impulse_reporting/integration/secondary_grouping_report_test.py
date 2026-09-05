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

CONFIG = ("tests", "data", "config", "config.json")

_STATS_FACT = "spark_catalog.gold.evaluation_stats_aggregator_fact"


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


# A key derived from the channel-data table: partition each container's samples by
# parity of their start timestamp. Guarantees >= 2 partitions without knowing the
# tstart scale — used by the (non-incremental) subdivide/persist tests. Column-
# returning derivers run on the driver, so a lambda is fine here.
def _parity_deriver(df):
    return (F.col("tstart") % F.lit(2)).cast("long")


# A monotonic, time-localized key: the day bucket of a microsecond-epoch tstart.
# Used by the incremental tests, where the contract is a time-localized key so the
# "latest partition" heuristic tracks the open partition.
_MICROS_PER_DAY = 86_400_000_000


def _day_deriver(df):
    return F.floor(F.col("tstart") / F.lit(_MICROS_PER_DAY)).cast("long")


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


def _sgk_report(
    spark, name, cm_table, channels_table, incremental_enabled, deriver=None
) -> Report:
    report = Report(
        name=name,
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_sgk_config(cm_table, channels_table, incremental_enabled)),
    )
    report.solver.config.secondary_grouping = SecondaryGroupingConfig(
        deriver=deriver or _parity_deriver
    )
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


def _clone_cm_with_bumped_container(spark, cm_table, updated_container_id=1):
    """Clone silver container_metrics, bumping only *updated_container_id*'s timestamp."""
    spark.read.table("spark_catalog.silver.container_metrics").withColumn(
        "timestamp",
        F.when(F.col("container_id") == updated_container_id, F.current_timestamp()).otherwise(
            F.lit("2020-01-01 00:00:00").cast("timestamp")
        ),
    ).write.format("delta").mode("overwrite").saveAsTable(cm_table)


def _clone_channels_with_appended_day(spark, channels_table, container_id=1, channel_id=5):
    """Clone silver channels and append a new, later day-bucket for one channel.

    Returns the new day-bucket value. The appended samples sit ~30 days past the
    container's current max ``tstart`` so they form a brand-new (and latest) day
    partition, with values > 1000 so the ``rpm > 1000`` event fires there.
    """
    base = spark.read.table("spark_catalog.silver.channels")
    max_tstart = (
        base.filter(F.col("container_id") == container_id)
        .agg(F.max("tstart").alias("m"))
        .collect()[0]["m"]
    )
    start = int(max_tstart) + 30 * _MICROS_PER_DAY
    step = 10_000_000  # 10s
    new_rows = [
        (container_id, channel_id, start + i * step, start + (i + 1) * step, 1500 + i)
        for i in range(4)
    ]
    # Match the silver channels schema exactly (value is an integer column).
    new_df = spark.createDataFrame(new_rows, schema=base.schema)
    base.unionByName(new_df).write.format("delta").mode("overwrite").saveAsTable(channels_table)
    return start // _MICROS_PER_DAY


def test_incremental_secondary_grouping_appends_new_partition_only(spark):
    """Appending a new day partition reprocesses only it; settled days are untouched.

    Endless-stream scenario: run 1 seeds several day partitions; run 2 appends a
    brand-new later day. Only the new/latest partition is recomputed — a sentinel
    written into a settled partition survives — while the new partition lands in
    gold with correct values.
    """
    # Run 1 (full): seed gold from base silver using the monotonic day key.
    r1 = _sgk_report(
        spark,
        "sgk_append_report",
        "spark_catalog.silver.container_metrics",
        "spark_catalog.silver.channels",
        False,
        deriver=_day_deriver,
    )
    _add_rpm_stats(r1)
    r1.determine_report()
    r1.persist_results()

    days = sorted(
        row.secondary_grouping_key
        for row in spark.read.table(_STATS_FACT)
        .filter(F.col("container_id") == 1)
        .select("secondary_grouping_key")
        .distinct()
        .collect()
    )
    assert days, days
    # Any run-1 day becomes settled once a strictly-later day is appended below.
    settled_day = days[0]

    # Tamper a settled day's gold value; if it were reprocessed it'd be overwritten.
    spark.sql(
        f"UPDATE {_STATS_FACT} SET statistic_value = -999.0 "
        f"WHERE container_id = 1 AND secondary_grouping_key = {settled_day} "
        f"AND aggregation_label = 'min'"
    )

    cm_table = "spark_catalog.silver.sgk_append_cm"
    channels_table = "spark_catalog.silver.sgk_append_channels"
    try:
        _clone_cm_with_bumped_container(spark, cm_table, updated_container_id=1)
        new_day = _clone_channels_with_appended_day(spark, channels_table, container_id=1)
        assert new_day not in days, (new_day, days)

        # Run 2 (incremental): only the new/latest day partition should be processed.
        r2 = _sgk_report(spark, "sgk_append_report", cm_table, channels_table, True, _day_deriver)
        _add_rpm_stats(r2)
        r2.determine_report()
        r2.persist_results()

        fact = spark.read.table(_STATS_FACT).filter(F.col("container_id") == 1)

        # The new day partition landed in gold with real (non-sentinel) values.
        new_rows = fact.filter(F.col("secondary_grouping_key") == new_day).collect()
        assert new_rows, "the appended day partition must be present in gold"
        assert all(r.statistic_value != -999.0 for r in new_rows)

        # The settled day was NOT reprocessed → its sentinel survived.
        settled_min = fact.filter(
            (F.col("secondary_grouping_key") == settled_day)
            & (F.col("aggregation_label") == "min")
        ).collect()
        assert settled_min and all(r.statistic_value == -999.0 for r in settled_min), settled_min
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {cm_table}")
        spark.sql(f"DROP TABLE IF EXISTS {channels_table}")


def test_incremental_secondary_grouping_skips_settled_partitions(spark):
    """Settled (non-latest, already-in-gold) partitions are NOT reprocessed.

    A container is flagged as updated but its silver data is unchanged. We tamper a
    settled partition's gold value; after the incremental run it must survive (the
    partition was neither re-read nor recomputed), while the latest partition is
    still refreshed. This is the point of key-level incremental: an endless stream
    never reprocesses its whole history.
    """
    # Seed gold (full) from base silver. Parity guarantees >= 2 partitions so one
    # is settled (non-latest) regardless of the container's time span.
    r1 = _sgk_report(
        spark,
        "sgk_skip_report",
        "spark_catalog.silver.container_metrics",
        "spark_catalog.silver.channels",
        False,
        deriver=_parity_deriver,
    )
    _add_rpm_stats(r1)
    r1.determine_report()
    r1.persist_results()

    # Container 1 must have >= 2 partitions so one is settled (non-latest).
    keys = sorted(
        row.secondary_grouping_key
        for row in spark.read.table(_STATS_FACT)
        .filter(F.col("container_id") == 1)
        .select("secondary_grouping_key")
        .distinct()
        .collect()
    )
    assert len(keys) >= 2, keys
    settled_key, latest_key = keys[0], keys[-1]

    # Tamper a settled-partition gold value with a sentinel. If that partition were
    # reprocessed, the recompute would overwrite it.
    spark.sql(
        f"UPDATE {_STATS_FACT} SET statistic_value = -999.0 "
        f"WHERE container_id = 1 AND secondary_grouping_key = {settled_key} "
        f"AND aggregation_label = 'min'"
    )

    cm_table = "spark_catalog.silver.sgk_skip_cm"
    channels_table = "spark_catalog.silver.sgk_skip_channels"
    try:
        # Container 1 flagged updated (bumped timestamp), but silver data unchanged.
        _clone_cm_with_bumped_container(spark, cm_table, updated_container_id=1)
        spark.read.table("spark_catalog.silver.channels").write.format("delta").mode(
            "overwrite"
        ).saveAsTable(channels_table)
        r2 = _sgk_report(spark, "sgk_skip_report", cm_table, channels_table, True, _parity_deriver)
        _add_rpm_stats(r2)
        r2.determine_report()
        r2.persist_results()

        fact = spark.read.table(_STATS_FACT).filter(F.col("container_id") == 1)
        # Settled partition's sentinel survived → it was not reprocessed.
        settled_min = (
            fact.filter(
                (F.col("secondary_grouping_key") == settled_key)
                & (F.col("aggregation_label") == "min")
            )
            .select("statistic_value")
            .collect()
        )
        assert settled_min, "settled partition rows must still exist"
        assert all(r.statistic_value == -999.0 for r in settled_min), settled_min
        # Latest partition was refreshed → its min is the real (non-sentinel) value.
        latest_min = (
            fact.filter(
                (F.col("secondary_grouping_key") == latest_key)
                & (F.col("aggregation_label") == "min")
            )
            .select("statistic_value")
            .collect()
        )
        assert latest_min and all(r.statistic_value != -999.0 for r in latest_min)
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {cm_table}")
        spark.sql(f"DROP TABLE IF EXISTS {channels_table}")
