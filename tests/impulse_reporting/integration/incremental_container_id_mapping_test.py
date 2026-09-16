"""Regression test for issue #99: incremental detection must apply ``column_name_mapping``.

A silver ``container_metrics`` table whose physical container-id column is named
``measurement_id`` (mapped to internal ``container_id`` via
``solver_config.container_metrics.column_name_mapping``) must work in incremental mode.
Before the fix, container detection read the raw table and joined on a literal
``container_id``, raising ``AnalysisException``; detection now reads through the solver's
scoped read, so the mapping is applied first.
"""

from unittest.mock import create_autospec

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient

from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig, TableConfig
from impulse_reporting.config.config_parser import (
    ImpulseConfig,
    IncrementalConfig,
    QueryEngine,
    Source,
    UnitySink,
)
from impulse_reporting.core.report import Report
from tests.conftest import spark  # noqa: F401  (pytest fixture)
from tests.impulse_reporting.integration.incremental_report_test import add_aggs_to_report

_MAPPED_SCHEMA = "spark_catalog.silver_mapped_cid"


def _clone_with_renamed_cid(spark, source_table, target_table):  # noqa: F811
    """Clone a silver container_metrics table, renaming ``container_id`` -> ``measurement_id``."""
    spark.read.table(source_table).withColumnRenamed(
        "container_id", "measurement_id"
    ).write.format("delta").mode("overwrite").saveAsTable(target_table)


def _config(container_metrics_table, *, is_enabled):
    return dict(
        ImpulseConfig(
            source=Source(
                container_metrics_table=container_metrics_table,
                channel_metrics_table="spark_catalog.silver.channel_metrics",
                channels_uri="spark_catalog.silver.channels",
            ),
            unity_sink=UnitySink(
                catalog="spark_catalog", schema="gold", table_prefix="mapped_cid"
            ),
            incremental=IncrementalConfig(
                enabled=is_enabled,
                silver_last_modified_column="timestamp",
                gold_last_modified_column="_created_at",
            ),
            query_engine=QueryEngine(
                solver_config=SolverConfig(
                    container_metrics=TableConfig(
                        column_name_mapping={"measurement_id": "container_id"}
                    ),
                ),
            ),
            measurement_dimensions=["container_id"],
        )
    )


def _run(spark, container_metrics_table, *, is_enabled):  # noqa: F811
    report = Report(
        name="mapped_cid_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=_config(container_metrics_table, is_enabled=is_enabled),
    )
    add_aggs_to_report(report)
    report.determine_report()
    report.persist_results()


def _container_ids(spark):  # noqa: F811
    return sorted(
        r.container_id
        for r in spark.read.table("spark_catalog.gold.mapped_cid_measurement_dimension").collect()
    )


def test_incremental_detection_applies_container_id_mapping(spark):  # noqa: F811
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_MAPPED_SCHEMA}")
    seed_table = f"{_MAPPED_SCHEMA}.container_metrics_1_2"
    full_table = f"{_MAPPED_SCHEMA}.container_metrics_all"
    try:
        _clone_with_renamed_cid(
            spark, "spark_catalog.silver.container_metrics_inc_1_2", seed_table
        )
        _clone_with_renamed_cid(spark, "spark_catalog.silver.container_metrics", full_table)

        # Run 1 (full): seed gold with containers 1 and 2. Full mode goes through the solver,
        # which already applied the mapping, so this succeeds even before the fix.
        _run(spark, seed_table, is_enabled=False)
        assert _container_ids(spark) == [1, 2]

        # Run 2 (incremental): detection reads container_metrics through the solver's scoped
        # read, applies the mapping, joins on the internal container_id (no AnalysisException),
        # and detects the new container 3 while leaving 1 and 2 in place. This exercises both
        # _detect_upserted_containers and _detect_updated_containers via the shared read.
        _run(spark, full_table, is_enabled=True)
        assert _container_ids(spark) == [1, 2, 3], "new container must be detected under mapping"

        # Real-value sanity check: the histogram carries positive accumulated duration.
        total = (
            spark.read.table("spark_catalog.gold.mapped_cid_histogram_fact")
            .agg(F.sum("hist_value").alias("s"))
            .collect()[0]["s"]
        )
        assert total is not None and total > 0
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {_MAPPED_SCHEMA} CASCADE")
