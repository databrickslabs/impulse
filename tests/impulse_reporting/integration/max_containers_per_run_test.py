"""Integration tests for ``query_engine.max_containers_per_run``.

The cap bounds upserted containers per incremental run; committed containers drop out of
the next run's detection, so repeated runs iterate the population and the final gold
matches an uncapped run. The cap needs gold to exist, so the tests seed gold with a
subset first, then run capped passes over the full silver table (containers 1, 2, 3).
"""

from unittest.mock import create_autospec

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient

from impulse_reporting.config.config_parser import (
    IncrementalConfig,
    ImpulseConfig,
    QueryEngine,
    Source,
    UnitySink,
)
from impulse_reporting.core.report import Report
from tests.conftest import spark
from tests.impulse_reporting.integration.incremental_report_test import (
    add_aggs_to_report,
    add_aggs_to_report_changed_bins,
)


def _config(silver_table, prefix, *, max_containers_per_run=None):
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
        query_engine=QueryEngine(max_containers_per_run=max_containers_per_run),
    )


def _make_report(
    spark, silver_table, prefix, *, max_containers_per_run=None, add_aggs=add_aggs_to_report
):
    report = Report(
        name="cap_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config(silver_table, prefix, max_containers_per_run=max_containers_per_run)),
    )
    add_aggs(report)
    return report


def _run(spark, silver_table, prefix, *, max_containers_per_run=None, add_aggs=add_aggs_to_report):
    """Run ONE determine+persist pass (single batch) — the granular API, no run() loop."""
    report = _make_report(
        spark,
        silver_table,
        prefix,
        max_containers_per_run=max_containers_per_run,
        add_aggs=add_aggs,
    )
    report.determine_report()
    report.persist_results()


def _container_ids(spark, prefix):
    return sorted(
        r.container_id
        for r in spark.read.table(f"spark_catalog.gold.{prefix}_measurement_dimension").collect()
    )


def _rows_without_meta(df):
    """Deterministic row set, dropping run-dependent columns.

    Excludes ``_``-prefixed meta (e.g. ``_created_at``) and ``config_hash`` (a hash of
    the full config, which intentionally differs between the capped/uncapped configs).
    """
    cols = [c for c in df.columns if not c.startswith("_") and c != "config_hash"]
    return sorted(tuple(r) for r in df.select(*cols).collect())


def test_max_containers_per_run_iterates_and_matches_uncapped(spark):
    # --- Uncapped baseline (prefix "uncapped") ---------------------------------
    # Seed gold with container 1 (initial full load), then one uncapped incremental
    # run over the full silver table processes the remaining new containers {2, 3}.
    _run(spark, "container_metrics_inc_1", "uncapped")
    assert _container_ids(spark, "uncapped") == [1]
    _run(spark, "container_metrics", "uncapped")  # cap=None
    assert _container_ids(spark, "uncapped") == [1, 2, 3]

    # --- Capped runs (prefix "capped", N=1) ------------------------------------
    _run(spark, "container_metrics_inc_1", "capped")  # initial full load -> {1}
    assert _container_ids(spark, "capped") == [1]

    # First capped incremental run: {2, 3} are new; cap=1 processes the lowest (2).
    _run(spark, "container_metrics", "capped", max_containers_per_run=1)
    assert _container_ids(spark, "capped") == [1, 2], "cap must limit the run to one new container"

    # Second capped run advances to container 3 (2 already committed, drops out).
    _run(spark, "container_metrics", "capped", max_containers_per_run=1)
    assert _container_ids(spark, "capped") == [1, 2, 3]

    # Third capped run is a no-op: everything is already processed.
    _run(spark, "container_metrics", "capped", max_containers_per_run=1)
    assert _container_ids(spark, "capped") == [1, 2, 3]

    # --- Gold from the capped iteration matches the uncapped run ----------------
    for table in ("histogram_fact", "stats_aggregator_fact", "measurement_dimension"):
        capped = spark.read.table(f"spark_catalog.gold.capped_{table}")
        uncapped = spark.read.table(f"spark_catalog.gold.uncapped_{table}")
        assert _rows_without_meta(capped) == _rows_without_meta(
            uncapped
        ), f"{table}: capped iteration must reproduce the uncapped gold"

    # Real-value sanity check: the histogram carries positive accumulated duration.
    total = (
        spark.read.table("spark_catalog.gold.capped_histogram_fact")
        .agg(F.sum("hist_value").alias("s"))
        .collect()[0]["s"]
    )
    assert total is not None and total > 0


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
    _run(spark, "container_metrics_prune_modified", "prune", max_containers_per_run=1)

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


def _hist_container_ids(spark, prefix):
    return {
        r.container_id
        for r in spark.read.table(f"spark_catalog.gold.{prefix}_histogram_fact")
        .select("container_id")
        .distinct()
        .collect()
    }


def test_full_mode_container_chunked_solve_matches_uncapped(spark):
    """Full-mode solve chunked by the cap (pre_filter=None) yields identical gold."""
    # Full run over all 3 containers, uncapped vs cap=1 (solve chunked into 3 batches).
    _run(spark, "container_metrics", "fullbase")
    _run(spark, "container_metrics", "fullcap", max_containers_per_run=1)

    assert _container_ids(spark, "fullbase") == [1, 2, 3]
    assert _container_ids(spark, "fullcap") == [1, 2, 3]
    for table in ("histogram_fact", "stats_aggregator_fact", "measurement_dimension"):
        capped = spark.read.table(f"spark_catalog.gold.fullcap_{table}")
        uncapped = spark.read.table(f"spark_catalog.gold.fullbase_{table}")
        assert _rows_without_meta(capped) == _rows_without_meta(
            uncapped
        ), f"{table}: chunked full-mode solve must match the uncapped solve"


def test_changed_entity_capped_defers_beyond_cap_new_and_matches_uncapped(spark):
    """A changed definition recomputes historical + capped-new; new-beyond-cap deferred."""
    # Seed gold with container 1 under definition D1.
    _run(spark, "container_metrics_inc_1", "chg")
    assert _container_ids(spark, "chg") == [1]

    # Change the definition (D2) and add containers 2,3; incremental, cap=1.
    # Run 1: changed entity computed for historical {1} + capped new {2}; 3 deferred.
    _run(
        spark,
        "container_metrics",
        "chg",
        max_containers_per_run=1,
        add_aggs=add_aggs_to_report_changed_bins,
    )
    assert _container_ids(spark, "chg") == [1, 2], "beyond-cap new container 3 must be deferred"
    assert _hist_container_ids(spark, "chg") == {1, 2}, "no facts for the deferred container"

    # Run 2 advances to container 3 (definition now unchanged -> unchanged path).
    _run(
        spark,
        "container_metrics",
        "chg",
        max_containers_per_run=1,
        add_aggs=add_aggs_to_report_changed_bins,
    )
    assert _container_ids(spark, "chg") == [1, 2, 3]

    # Uncapped reference: D1 on container 1, then D2 over all containers in one run.
    _run(spark, "container_metrics_inc_1", "chgbase")
    _run(spark, "container_metrics", "chgbase", add_aggs=add_aggs_to_report_changed_bins)

    for table in ("histogram_fact", "stats_aggregator_fact"):
        capped = spark.read.table(f"spark_catalog.gold.chg_{table}")
        uncapped = spark.read.table(f"spark_catalog.gold.chgbase_{table}")
        assert _rows_without_meta(capped) == _rows_without_meta(
            uncapped
        ), f"{table}: capped changed-entity iteration must match the uncapped run"


def test_run_drains_all_batches_in_one_call(spark):
    """A single run() call loops determine+persist until the population is drained."""
    # Seed gold with container 1.
    _run(spark, "container_metrics_inc_1", "loop")
    assert _container_ids(spark, "loop") == [1]

    # New containers {2, 3}; cap=1. One run() call loops: {2} (more pending) then {3}.
    _make_report(spark, "container_metrics", "loop", max_containers_per_run=1).run()
    assert _container_ids(spark, "loop") == [1, 2, 3], "run() must drain every batch"

    # Matches an uncapped single incremental run.
    _run(spark, "container_metrics_inc_1", "loopbase")
    _run(spark, "container_metrics", "loopbase")
    for table in ("histogram_fact", "stats_aggregator_fact", "measurement_dimension"):
        looped = spark.read.table(f"spark_catalog.gold.loop_{table}")
        uncapped = spark.read.table(f"spark_catalog.gold.loopbase_{table}")
        assert _rows_without_meta(looped) == _rows_without_meta(uncapped), table


def test_run_loop_with_changed_definition(spark):
    """run() drains batches when a definition changed: iter 1 recomputes, rest unchanged."""
    _run(spark, "container_metrics_inc_1", "loopchg")  # D1 on container 1

    # Changed definition (D2) + new {2,3}, cap=1, single run() call.
    _make_report(
        spark,
        "container_metrics",
        "loopchg",
        max_containers_per_run=1,
        add_aggs=add_aggs_to_report_changed_bins,
    ).run()
    assert _container_ids(spark, "loopchg") == [1, 2, 3]

    # Uncapped D2 reference.
    _run(spark, "container_metrics_inc_1", "loopchgbase")
    _run(spark, "container_metrics", "loopchgbase", add_aggs=add_aggs_to_report_changed_bins)
    for table in ("histogram_fact", "stats_aggregator_fact"):
        looped = spark.read.table(f"spark_catalog.gold.loopchg_{table}")
        uncapped = spark.read.table(f"spark_catalog.gold.loopchgbase_{table}")
        assert _rows_without_meta(looped) == _rows_without_meta(uncapped), table


def test_run_without_persist_does_not_loop(spark):
    """run(persist_results=False) runs determine once and does not iterate."""
    _run(spark, "container_metrics_inc_1", "nopersist")
    report = _make_report(spark, "container_metrics", "nopersist", max_containers_per_run=1)
    report.run(persist_results=False)
    # Nothing was persisted beyond the seed, so gold still holds only container 1.
    assert _container_ids(spark, "nopersist") == [1]
