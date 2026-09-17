"""Integration tests for ``query_engine.max_containers_per_batch``.

The cap bounds upserted containers per incremental run; committed containers drop out of
the next run's detection, so repeated runs iterate the population and the final gold matches
an uncapped run. The cap is incremental-only: the FIRST run of a capped config treats every
container as "new" and caps-and-defers even before gold exists, so successive runs drain the
full silver table (containers 1, 2, 3).

These tests use lightweight aggregations (few histogram bins) and share a single uncapped
baseline (computed once) so each determine+persist cycle stays cheap.
"""

from unittest.mock import create_autospec

import pyspark.sql.functions as F
import pytest
from databricks.sdk import WorkspaceClient

from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.config.config_parser import (
    IncrementalConfig,
    ImpulseConfig,
    QueryEngine,
    Source,
    UnitySink,
)
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.events.container_event import ContainerEvent
from tests.conftest import spark


def _add_light_aggs(report, *, rpm_bins=None):
    """Register a small event/aggregation set on the report.

    Mirrors the entity mix used across the reporting tests (two histograms, a basic event, an
    event-scoped stats aggregator, a container event + its stats aggregator) but with tiny
    histogram bins so each determine+persist cycle stays cheap. ``rpm_bins`` lets a caller flip
    the rpm-histogram definition (its hash) to exercise the changed-definition path.
    """
    rpm_bins = rpm_bins if rpm_bins is not None else [float(i) for i in range(0, 8000, 2000)]
    query = report.get_db().query
    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")
    page = Page(page_number=1)
    report.add_page(page)
    page.add_aggregation(HistogramDuration("rpm_hist_p1", base_expr=c1, bins=rpm_bins))
    page.add_aggregation(
        HistogramDuration(
            "speed_hist_p1", base_expr=c2, bins=[float(i) for i in range(0, 300, 100)]
        )
    )
    rpm_event = BasicEvent(name="rpm_event", expr=c1 > 0, desc="engine speed > 0 rpm")
    report.add_event(rpm_event)
    page.add_aggregation(
        StatsAggregator(
            name="stats_agg",
            input_expressions=[c1],
            channel_names=["Engine RPM"],
            event=rpm_event,
            statistics=["start", "end", "mean"],
        )
    )
    container_event = ContainerEvent("Measurement Event")
    report.add_event(container_event)
    page.add_aggregation(
        StatsAggregator(
            name="stats_agg_container",
            input_expressions=[c1],
            channel_names=["Engine RPM"],
            event=container_event,
            statistics=["start", "end", "mean"],
        )
    )


def _add_light_aggs_changed(report):
    """Same as :func:`_add_light_aggs` but with different rpm bins (a changed definition hash)."""
    _add_light_aggs(report, rpm_bins=[float(i) for i in range(0, 8000, 1000)])


def _config(silver_table, prefix, *, max_containers_per_batch=None):
    return ImpulseConfig(
        source=Source(
            container_metrics_table=f"spark_catalog.silver.{silver_table}",
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri="spark_catalog.silver.channels",
        ),
        unity_sink=UnitySink(catalog="spark_catalog", schema="gold", table_prefix=prefix),
        incremental=IncrementalConfig(
            enabled=True,
            silver_last_modified_column="timestamp",
            gold_last_modified_column="_created_at",
        ),
        query_engine=QueryEngine(max_containers_per_batch=max_containers_per_batch),
    )


def _make_report(
    spark, silver_table, prefix, *, max_containers_per_batch=None, add_aggs=_add_light_aggs
):
    report = Report(
        name="cap_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(
            _config(silver_table, prefix, max_containers_per_batch=max_containers_per_batch)
        ),
    )
    add_aggs(report)
    return report


def _run(spark, silver_table, prefix, *, max_containers_per_batch=None, add_aggs=_add_light_aggs):
    """Run ONE determine+persist pass (single batch) — the granular API, no run() loop."""
    report = _make_report(
        spark,
        silver_table,
        prefix,
        max_containers_per_batch=max_containers_per_batch,
        add_aggs=add_aggs,
    )
    report.determine_report()
    report.persist_results()


def _container_ids(spark, prefix):
    return sorted(
        r.container_id
        for r in spark.read.table(f"spark_catalog.gold.{prefix}_measurement_dimension").collect()
    )


def _hist_container_ids(spark, prefix):
    return {
        r.container_id
        for r in spark.read.table(f"spark_catalog.gold.{prefix}_histogram_fact")
        .select("container_id")
        .distinct()
        .collect()
    }


def _rows_without_meta(df):
    """Deterministic row set, dropping run-dependent columns.

    Excludes ``_``-prefixed meta (e.g. ``_created_at``) and ``config_hash`` (a hash of the
    full config, which intentionally differs between the capped/uncapped configs).
    """
    cols = [c for c in df.columns if not c.startswith("_") and c != "config_hash"]
    return sorted(tuple(r) for r in df.select(*cols).collect())


_BASELINE_TABLES = ("histogram_fact", "stats_aggregator_fact", "measurement_dimension")


@pytest.fixture(scope="module")
def uncapped_baseline(spark):
    """Uncapped gold for the light aggs, computed once and reused as snapshots.

    Returns ``{"plain": {table: rows}, "changed": {table: rows}}`` where rows are the
    ``_rows_without_meta`` snapshots of each gold fact table from a single uncapped full run.
    Collected eagerly so the snapshots survive the per-test ``cleanup_gold`` teardown.
    """
    baselines = {}
    for key, add_aggs, prefix in (
        ("plain", _add_light_aggs, "plainbase"),
        ("changed", _add_light_aggs_changed, "changedbase"),
    ):
        _run(spark, "container_metrics", prefix, add_aggs=add_aggs)
        baselines[key] = {
            t: _rows_without_meta(spark.read.table(f"spark_catalog.gold.{prefix}_{t}"))
            for t in _BASELINE_TABLES
        }
    return baselines


def test_bootstrap_capped_defers_beyond_cap_and_matches_uncapped(spark, uncapped_baseline):
    """Bootstrap (empty gold): the first capped run treats every container as new and caps it."""
    # Capped bootstrap (no gold): the first run processes only the lowest container.
    _run(spark, "container_metrics", "bootcap", max_containers_per_batch=1)
    assert _container_ids(spark, "bootcap") == [1], "bootstrap must cap the first run to one"

    # Successive runs drain the population (committed containers drop out of detection).
    _run(spark, "container_metrics", "bootcap", max_containers_per_batch=1)
    assert _container_ids(spark, "bootcap") == [1, 2]
    _run(spark, "container_metrics", "bootcap", max_containers_per_batch=1)
    assert _container_ids(spark, "bootcap") == [1, 2, 3]

    for t in _BASELINE_TABLES:
        got = _rows_without_meta(spark.read.table(f"spark_catalog.gold.bootcap_{t}"))
        assert got == uncapped_baseline["plain"][t], f"{t}: bootstrap drain must match uncapped"

    # Real-value sanity check: the histogram carries positive accumulated duration.
    total = (
        spark.read.table("spark_catalog.gold.bootcap_histogram_fact")
        .agg(F.sum("hist_value").alias("s"))
        .collect()[0]["s"]
    )
    assert total is not None and total > 0


def test_bootstrap_capped_drains_in_one_run_call(spark, uncapped_baseline):
    """A single run() call drains the whole population from an empty gold."""
    _make_report(spark, "container_metrics", "bootrun", max_containers_per_batch=1).run()
    assert _container_ids(spark, "bootrun") == [1, 2, 3]
    for t in _BASELINE_TABLES:
        got = _rows_without_meta(spark.read.table(f"spark_catalog.gold.bootrun_{t}"))
        assert got == uncapped_baseline["plain"][t], f"{t}: run() drain must match uncapped"


def test_full_mode_container_chunked_solve_matches_uncapped(spark, uncapped_baseline):
    """Full mode (no incremental) + cap: the solve is chunked by the cap in a single pass.

    Chunking is a memory split, not a content change, so gold must equal the uncapped run.
    """
    config = ImpulseConfig(
        source=Source(
            container_metrics_table="spark_catalog.silver.container_metrics",
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri="spark_catalog.silver.channels",
        ),
        unity_sink=UnitySink(catalog="spark_catalog", schema="gold", table_prefix="fullcap"),
        # No incremental config -> full mode; cap chunks the single full pass.
        query_engine=QueryEngine(max_containers_per_batch=1),
    )
    report = Report(
        name="cap_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(config),
    )
    _add_light_aggs(report)
    report.determine_report()
    report.persist_results()

    assert _container_ids(spark, "fullcap") == [
        1,
        2,
        3,
    ], "full-mode solve processes all containers"
    for t in _BASELINE_TABLES:
        got = _rows_without_meta(spark.read.table(f"spark_catalog.gold.fullcap_{t}"))
        assert got == uncapped_baseline["plain"][t], f"{t}: chunked full solve must match uncapped"


def test_capped_run_does_not_prune_out_of_batch_updated_container(spark):
    """A capped run must not prune gold rows for updated containers outside the cap."""
    # Seed gold with containers 1 and 2 (initial full load).
    _run(spark, "container_metrics_inc_1_2", "prune")
    assert _container_ids(spark, "prune") == [1, 2]

    hist_pre = spark.read.table("spark_catalog.gold.prune_histogram_fact")
    container2_hist_pre = (
        hist_pre.where(F.col("container_id") == 2).orderBy("visual_id", "bin_id").collect()
    )
    assert container2_hist_pre, "container 2 must have histogram rows after the initial load"
    meas_pre = spark.read.table("spark_catalog.gold.prune_measurement_dimension")
    container2_created_at_pre = (
        meas_pre.where(F.col("container_id") == 2).select("_created_at").collect()[0][0]
    )

    # Mark BOTH containers as updated in silver (timestamp newer than gold _created_at).
    modified = spark.read.table("spark_catalog.silver.container_metrics_inc_1_2").withColumn(
        "timestamp", F.current_timestamp()
    )
    modified.write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver.container_metrics_prune_modified"
    )

    # Incremental run with cap=1: only container 1 (lowest id) is reprocessed;
    # container 2 is updated but OUTSIDE the cap.
    _run(spark, "container_metrics_prune_modified", "prune", max_containers_per_batch=1)

    assert _container_ids(spark, "prune") == [1, 2], "no container may be dropped"

    hist_post = spark.read.table("spark_catalog.gold.prune_histogram_fact")
    container2_hist_post = (
        hist_post.where(F.col("container_id") == 2).orderBy("visual_id", "bin_id").collect()
    )
    # Container 2 was NOT in the cap -> its gold rows must be untouched (not pruned).
    assert container2_hist_post == container2_hist_pre
    meas_post = spark.read.table("spark_catalog.gold.prune_measurement_dimension")
    container2_created_at_post = (
        meas_post.where(F.col("container_id") == 2).select("_created_at").collect()[0][0]
    )
    assert container2_created_at_pre == container2_created_at_post, "container 2 must be untouched"


def test_changed_entity_capped_defers_beyond_cap_new(spark, uncapped_baseline):
    """A changed definition recomputes historical + capped-new; new-beyond-cap is deferred."""
    # Seed gold with container 1 under definition D1.
    _run(spark, "container_metrics_inc_1", "chg")
    assert _container_ids(spark, "chg") == [1]

    # Change the definition (D2) and add containers 2, 3; incremental, cap=1.
    # Run 1: changed entity computed for historical {1} + capped new {2}; container 3 deferred.
    _run(
        spark,
        "container_metrics",
        "chg",
        max_containers_per_batch=1,
        add_aggs=_add_light_aggs_changed,
    )
    assert _container_ids(spark, "chg") == [1, 2], "beyond-cap new container 3 must be deferred"
    assert _hist_container_ids(spark, "chg") == {1, 2}, "no facts for the deferred container"

    # Run 2 advances to container 3 (definition now unchanged -> unchanged path).
    _run(
        spark,
        "container_metrics",
        "chg",
        max_containers_per_batch=1,
        add_aggs=_add_light_aggs_changed,
    )
    assert _container_ids(spark, "chg") == [1, 2, 3]

    for t in ("histogram_fact", "stats_aggregator_fact"):
        got = _rows_without_meta(spark.read.table(f"spark_catalog.gold.chg_{t}"))
        assert (
            got == uncapped_baseline["changed"][t]
        ), f"{t}: changed iteration must match uncapped"


def test_run_loop_with_changed_definition(spark, uncapped_baseline):
    """run() drains batches when a definition changed: iter 1 recomputes, the rest are unchanged."""
    _run(spark, "container_metrics_inc_1", "loopchg")  # D1 on container 1

    # Changed definition (D2) + new {2, 3}, cap=1, single run() call drains everything.
    _make_report(
        spark,
        "container_metrics",
        "loopchg",
        max_containers_per_batch=1,
        add_aggs=_add_light_aggs_changed,
    ).run()
    assert _container_ids(spark, "loopchg") == [1, 2, 3]

    for t in ("histogram_fact", "stats_aggregator_fact"):
        got = _rows_without_meta(spark.read.table(f"spark_catalog.gold.loopchg_{t}"))
        assert got == uncapped_baseline["changed"][t], t


def test_run_without_persist_does_not_loop(spark):
    """run(persist_results=False) runs determine once and does not iterate."""
    _run(spark, "container_metrics_inc_1", "nopersist")
    report = _make_report(spark, "container_metrics", "nopersist", max_containers_per_batch=1)
    report.run(persist_results=False)
    # Nothing was persisted beyond the seed, so gold still holds only container 1.
    assert _container_ids(spark, "nopersist") == [1]


def test_run_stops_when_drain_makes_no_progress(spark):
    """A stalled drain must stop with a warning instead of looping forever.

    Every container's freshness ``timestamp`` is set far in the future, so a persisted
    container is always re-detected as updated and never drops out. With cap=1 the lowest
    container (1) is selected every iteration and blocks the drain. run() must detect the
    repeated batch, warn, and return rather than hang.
    """
    spark.read.table("spark_catalog.silver.container_metrics").withColumn(
        "timestamp", F.lit("2999-01-01 00:00:00").cast("timestamp")
    ).write.format("delta").mode("overwrite").saveAsTable(
        "spark_catalog.silver.container_metrics_future"
    )
    try:
        report = _make_report(
            spark, "container_metrics_future", "stuck", max_containers_per_batch=1
        )
        with pytest.warns(UserWarning, match="no forward progress"):
            report.run()
        # Only the first batch (container 1) was ever committed; the drain stalled on it,
        # so the remaining containers were left unprocessed rather than looped on.
        assert _container_ids(spark, "stuck") == [1]
    finally:
        spark.sql("DROP TABLE IF EXISTS spark_catalog.silver.container_metrics_future")
