# pylint: disable=missing-function-docstring
"""End-to-end: a customer-provided MeasurementDB implementation, selected from the report config.

The custom DB reports only the containers listed in an extra allow-list table. Its config class
requires that table and declares it for Delta version pinning. Each test covers one promise of the
extension point: the selected DB serves all reads, incremental detection reads through it, and the
declared extra table is pinned.
"""

from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from pyspark.sql import Row

import impulse_query_engine.measurement_db_registry as registry
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from impulse_query_engine.measurement_db_registry import register_measurement_db
from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report

_ALLOWED = "spark_catalog.silver.e2e_allowed_containers"
_GOLD_MEASUREMENTS = "spark_catalog.gold.e2e_measurement_dimension"


class AllowListConfig(MeasurementDBConfig):
    def __init__(self, *, allowed_containers_table: str, **base_kwargs):
        super().__init__(**base_kwargs)
        self.allowed_containers_table = allowed_containers_table

    def configured_table_uris(self) -> list[str]:
        return super().configured_table_uris() + [self.allowed_containers_table]


class AllowListDB(MeasurementDB):
    """Reports only the containers listed in ``allowed_containers_table``."""

    def container_metrics(self, spark):
        allowed = self._read_table(spark, self.config.allowed_containers_table)
        return super().container_metrics(spark).join(allowed, "container_id", "left_semi")


@pytest.fixture(autouse=True)
def allow_list_db(monkeypatch):
    monkeypatch.setattr(registry, "_REGISTRY", dict(registry._REGISTRY))
    register_measurement_db("AllowListDB", AllowListConfig)(AllowListDB)


@pytest.fixture
def set_allowed_container_ids(spark):
    """Overwrites the allow-list table with the given container ids."""

    def write(*container_ids: int):
        spark.createDataFrame([(i,) for i in container_ids], "container_id int").write.format(
            "delta"
        ).mode("overwrite").saveAsTable(_ALLOWED)

    yield write
    spark.sql(f"DROP TABLE IF EXISTS {_ALLOWED}")


def _create_report(spark, *, incremental: bool = False) -> Report:
    config = {
        "source": {
            "container_metrics_table": "spark_catalog.silver.container_metrics",
            "channel_metrics_table": "spark_catalog.silver.channel_metrics",
            "channels_uri": "spark_catalog.silver.channels",
        },
        "unity_sink": {"catalog": "spark_catalog", "schema": "gold", "table_prefix": "e2e"},
        "query_engine": {
            "measurement_db": "AllowListDB",
            "measurement_db_config": {"allowed_containers_table": _ALLOWED},
        },
        "incremental": {"enabled": incremental},
    }
    report = Report(
        name="e2e_custom_db",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=config,
    )
    rpm = report.get_db().query.channel(channel_name="Engine RPM")
    page = Page(page_number=1)
    report.add_page(page)
    page.add_aggregation(
        HistogramDuration("rpm_hist", base_expr=rpm, bins=[float(b) for b in range(0, 8000, 500)])
    )
    return report


def _gold_created_at_by_container_id(spark) -> dict[int, object]:
    rows = spark.read.table(_GOLD_MEASUREMENTS).select("container_id", "_created_at").collect()
    return {row.container_id: row._created_at for row in rows}


def test_report_reads_through_selected_db(spark, set_allowed_container_ids):
    set_allowed_container_ids(1, 2)
    report = _create_report(spark)
    report.determine_report()

    assert type(report.get_db()) is AllowListDB
    # determine_report splits results into {"changed", "unchanged"} definition buckets; exactly
    # one holds this run's histogram. Pull it out and check which containers it was computed for.
    [histogram] = [df for df in report.aggregation_dfs["HISTOGRAM"].values() if df is not None]
    assert histogram.select("container_id").distinct().orderBy("container_id").collect() == [
        Row(container_id=1),
        Row(container_id=2),
    ]


def test_report_restricts_to_single_allowed_container(spark, set_allowed_container_ids):
    set_allowed_container_ids(1)
    report = _create_report(spark)

    # The measurement_db_config sticks: the allow-list table reaches the built DB's config...
    assert report.get_db().config.allowed_containers_table == _ALLOWED

    # ...and with only container 1 allowed, that is the only container the histogram covers.
    report.determine_report()
    [histogram] = [df for df in report.aggregation_dfs["HISTOGRAM"].values() if df is not None]
    assert histogram.select("container_id").distinct().collect() == [Row(container_id=1)]


def test_incremental_detection_reads_through_custom_db(spark, set_allowed_container_ids):
    set_allowed_container_ids(1)
    first = _create_report(spark)
    first.determine_report()
    first.persist_results()
    before = _gold_created_at_by_container_id(spark)

    set_allowed_container_ids(1, 2)
    second = _create_report(spark, incremental=True)
    second.determine_report()
    second.persist_results()
    after = _gold_created_at_by_container_id(spark)

    assert set(before.keys()) == {1}
    # The second run computes only the newly allowed container 2 (unchanged definition bucket).
    [histogram] = [df for df in second.aggregation_dfs["HISTOGRAM"].values() if df is not None]
    assert histogram.select("container_id").distinct().collect() == [Row(container_id=2)]
    assert after[1] == before[1]


def test_declared_extra_table_is_pinned(spark, set_allowed_container_ids):
    set_allowed_container_ids(1)
    db = _create_report(spark).get_db()
    db.pin_versions(spark)

    set_allowed_container_ids(1, 2, 3)

    assert {row.container_id for row in db.container_metrics(spark).collect()} == {1}
