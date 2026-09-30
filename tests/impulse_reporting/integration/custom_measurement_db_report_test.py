# pylint: disable=missing-function-docstring
"""End-to-end: a registered custom read seam selected via ``query_engine.measurement_db``.

The custom DB reads a "raw" copy of ``container_metrics`` whose id column is ``session_id`` and
renames it back, so the solve *and* incremental container detection only work if every read goes
through the DB.
"""

from unittest.mock import create_autospec

import pyspark.sql.functions as F
import pytest
from databricks.sdk import WorkspaceClient

import impulse_query_engine.measurement_db_registry as registry
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from impulse_query_engine.measurement_db_registry import register_measurement_db
from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.config.config_parser import (
    IncrementalConfig,
    ImpulseConfig,
    QueryEngine,
    Source,
    UnitySink,
)
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report

_RAW_CONTAINER_METRICS = "spark_catalog.silver.custom_db_container_metrics_raw"


class SessionKeyedDB(MeasurementDB):
    """Reshapes the raw container read: ``session_id`` -> ``container_id``."""

    def container_metrics(self, spark):
        return super().container_metrics(spark).withColumnRenamed("session_id", "container_id")


@pytest.fixture(autouse=True)
def custom_db(spark, monkeypatch):
    monkeypatch.setattr(registry, "_REGISTRY", dict(registry._REGISTRY))
    register_measurement_db("SessionKeyedDB", MeasurementDBConfig)(SessionKeyedDB)
    spark.read.table("spark_catalog.silver.container_metrics").withColumnRenamed(
        "container_id", "session_id"
    ).write.format("delta").mode("overwrite").saveAsTable(_RAW_CONTAINER_METRICS)
    yield
    spark.sql(f"DROP TABLE IF EXISTS {_RAW_CONTAINER_METRICS}")


def _report(spark, *, incremental: bool) -> Report:
    config = ImpulseConfig(
        source=Source(
            container_metrics_table=_RAW_CONTAINER_METRICS,
            channel_metrics_table="spark_catalog.silver.channel_metrics",
            channels_uri="spark_catalog.silver.channels",
        ),
        unity_sink=UnitySink(catalog="spark_catalog", schema="gold", table_prefix="evaluation"),
        query_engine=QueryEngine(measurement_db="SessionKeyedDB"),
        incremental=IncrementalConfig(enabled=incremental),
    )
    report = Report(
        name="custom_db_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(config),
    )
    rpm = report.get_db().query.channel(channel_name="Engine RPM")
    page = Page(page_number=1)
    report.add_page(page)
    page.add_aggregation(
        HistogramDuration("rpm_hist", base_expr=rpm, bins=[float(b) for b in range(0, 8000, 500)])
    )
    return report


def test_report_runs_through_custom_db(spark):
    report = _report(spark, incremental=False)
    assert type(report.get_db()) is SessionKeyedDB

    report.determine_report()

    hist = report.aggregation_dfs["HISTOGRAM"]["changed"]
    assert hist.filter(F.col("hist_value") > 0).count() > 0


def test_incremental_second_run_processes_no_containers(spark):
    first = _report(spark, incremental=False)
    first.determine_report()
    first.persist_results()
    gold = "spark_catalog.gold.evaluation_measurement_dimension"
    before = sorted(spark.read.table(gold).collect())

    second = _report(spark, incremental=True)
    second.determine_report()
    second.persist_results()

    # Detection reads the reshaped container_metrics through the DB, so it matches gold.
    assert second._has_processed_containers is False
    assert sorted(spark.read.table(gold).collect()) == before
