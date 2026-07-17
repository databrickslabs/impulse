# pylint: disable=missing-function-docstring
"""Integration tests: Report orchestration of calculated channels.

Builds a Report against the autouse ``spark_catalog.silver`` fixtures, adds a
calculated channel, runs determine_report + persist_results, and asserts real
values land in the gold fact/dimension tables — then verifies incremental
re-run behavior (idempotent unchanged upsert; changed-definition replace).
"""

from unittest.mock import create_autospec

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from databricks.sdk import WorkspaceClient

from impulse_reporting.channels.calculated_channel import CalculatedChannel
from impulse_reporting.config.config_parser import (
    DataType,
    ImpulseConfig,
    IncrementalConfig,
    QueryEngine,
    Source,
    UnitySink,
)
from impulse_reporting.core.report import Report
from tests.conftest import setup_raw_channels_db, spark  # noqa: F401  (pytest fixtures)

_FACT = "spark_catalog.gold.evaluation_calculated_channel_fact"
_DIM = "spark_catalog.gold.evaluation_calculated_channel_dimension"


def _config(silver_table="container_metrics", is_enabled=False):
    return ImpulseConfig(
        source=Source(
            container_metrics_table=f"spark_catalog.silver.{silver_table}",
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


def _add_channel(report, factor=3.6, name="speed_kmh", identity=None):
    q = report.get_db().query
    ch = CalculatedChannel(
        name=name,
        expr=q.channel(channel_name="Vehicle Speed Sensor") * factor,
        identity=identity or {"channel_name": name, "data_key": "CALC"},
    )
    report.add_calculated_channel(ch)
    return ch


def test_persist_calculated_channel_full(spark):
    report = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config(is_enabled=False)),
    )
    ch = _add_channel(report, factor=3.6)

    report.determine_report()
    report.persist_results()

    assert spark.catalog.tableExists(_FACT)
    assert spark.catalog.tableExists(_DIM)

    fact = spark.read.table(_FACT)
    assert fact.count() > 0
    # channel_id in the fact matches the reporting entity id.
    ids = {r["channel_id"] for r in fact.select("channel_id").distinct().collect()}
    assert ids == {ch.get_id()}
    # identity is a single VARIANT column carrying the whole identity dict.
    assert fact.schema["identity"].dataType == T.VariantType()
    id_names = {
        r["cn"]
        for r in fact.select(
            F.variant_get(F.col("identity"), "$.channel_name", "string").alias("cn")
        )
        .distinct()
        .collect()
    }
    id_keys = {
        r["dk"]
        for r in fact.select(F.variant_get(F.col("identity"), "$.data_key", "string").alias("dk"))
        .distinct()
        .collect()
    }
    assert id_names == {"speed_kmh"}
    assert id_keys == {"CALC"}

    # Values are the derived signal: compare against the raw source scaled by 3.6.
    raw = (
        report.get_db()
        .channels(spark)
        .join(
            report.get_db()
            .channel_metrics(spark)
            .filter(F.col("channel_name") == "Vehicle Speed Sensor")
            .select("container_id", "channel_id"),
            on=["container_id", "channel_id"],
        )
    )
    raw_sum = raw.select(F.sum("value")).first()[0]
    calc_sum = fact.select(F.sum("value")).first()[0]
    assert calc_sum == pytest.approx(raw_sum * 3.6)

    dim = spark.read.table(_DIM)
    assert (
        dim.filter(
            F.variant_get(F.col("identity"), "$.channel_name", "string") == "speed_kmh"
        ).count()
        == 1
    )
    assert dim.filter(F.col("definition_hash").isNotNull()).count() == 1


def test_incremental_unchanged_is_idempotent(spark):
    # Seed gold (full), then re-run incrementally with the same definition/data.
    r1 = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config(is_enabled=False)),
    )
    _add_channel(r1, factor=3.6)
    r1.determine_report()
    r1.persist_results()
    count_before = spark.read.table(_FACT).count()

    r2 = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config(is_enabled=True)),
    )
    _add_channel(r2, factor=3.6)
    r2.determine_report()
    r2.persist_results()

    # Idempotent: unchanged definition + unchanged silver → row count stable.
    assert spark.read.table(_FACT).count() == count_before


def test_incremental_changed_definition_replaces(spark):
    # Seed gold with factor 3.6.
    r1 = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config(is_enabled=False)),
    )
    ch1 = _add_channel(r1, factor=3.6)
    r1.determine_report()
    r1.persist_results()
    hash_before = (
        spark.read.table(_DIM)
        .filter(F.col("channel_id") == ch1.get_id())
        .select("definition_hash")
        .first()[0]
    )

    # Re-run incrementally with a changed factor → replace_by_ids rewrites the rows.
    r2 = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config(is_enabled=True)),
    )
    ch2 = _add_channel(r2, factor=3.7)
    assert ch2.get_id() == ch1.get_id()  # same identity → same entity id
    r2.determine_report()
    r2.persist_results()

    dim = spark.read.table(_DIM)
    hash_after = (
        dim.filter(F.col("channel_id") == ch2.get_id()).select("definition_hash").first()[0]
    )
    assert hash_after != hash_before
    # A single dimension row per channel_id (upsert, not append).
    assert dim.filter(F.col("channel_id") == ch2.get_id()).count() == 1


def test_incremental_identity_reorder_does_not_reprocess(spark):
    # Seed gold with identity keys in one order.
    r1 = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config(is_enabled=False)),
    )
    ch1 = _add_channel(r1, factor=3.6, identity={"channel_name": "speed_kmh", "data_key": "CALC"})
    r1.determine_report()
    r1.persist_results()
    count_before = spark.read.table(_FACT).count()
    hash_before = (
        spark.read.table(_DIM)
        .filter(F.col("channel_id") == ch1.get_id())
        .select("definition_hash")
        .first()[0]
    )

    # Re-run incrementally with the SAME identity but reversed key insertion order
    # and the same expression. This must NOT be seen as a definition change.
    r2 = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config(is_enabled=True)),
    )
    ch2 = _add_channel(r2, factor=3.6, identity={"data_key": "CALC", "channel_name": "speed_kmh"})
    assert ch2.get_id() == ch1.get_id()
    r2.determine_report()
    r2.persist_results()

    # Stable hash → classified unchanged → idempotent upsert, no row growth.
    hash_after = (
        spark.read.table(_DIM)
        .filter(F.col("channel_id") == ch2.get_id())
        .select("definition_hash")
        .first()[0]
    )
    assert hash_after == hash_before
    assert spark.read.table(_FACT).count() == count_before


def test_calculated_channel_raw_mode(spark, setup_raw_channels_db):
    """A calculated channel solves over RAW (point-sample) silver data.

    In RAW mode the solver run-length/interval-encodes the point samples before
    the calc-channel grouped-map UDF runs — exercising the ``is_raw_data`` branch
    of ``DefaultSolver.solve_calculated_channels``.
    """
    config = ImpulseConfig(
        source=Source(
            container_metrics_table="spark_catalog.silver_raw.container_metrics",
            channel_metrics_table="spark_catalog.silver_raw.channel_metrics",
            channels_uri="spark_catalog.silver_raw.channels",
        ),
        query_engine=QueryEngine(data_type=DataType.RAW),
    )
    report = Report(
        name="calc_channel_raw_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(config),
    )
    q = report.get_db().query
    ch = CalculatedChannel(
        name="rpm_x2",
        expr=q.channel(channel_name="Engine RPM") * 2,
        identity={"channel_name": "rpm_x2", "data_key": "CALC"},
    )
    report.add_calculated_channel(ch)

    # This is the branch that regressed: RAW-mode solve must not raise.
    df = CalculatedChannel.determine_calculated_channels(
        spark, [ch], query=q, solver=report.get_solver()
    )
    assert df.count() > 0
    assert df.schema["identity"].dataType == T.VariantType()
    ids = {r["channel_id"] for r in df.select("channel_id").distinct().collect()}
    assert ids == {ch.get_id()}
