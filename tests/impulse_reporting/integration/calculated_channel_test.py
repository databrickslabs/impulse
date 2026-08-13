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

from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from impulse_reporting.channels.calculated_channel import CalculatedChannel
from impulse_reporting.config.config_parser import (
    CalculatedChannels,
    DataType,
    ImpulseConfig,
    IncrementalConfig,
    QueryEngine,
    Source,
    UnitySink,
)
from impulse_reporting.core.report import Report
from tests.conftest import setup_raw_channels_db, spark  # noqa: F401  (pytest fixtures)
from tests.impulse_reporting.integration.test_helpers import (
    clone_silver_with_shrunk_container,
)

_FACT = "spark_catalog.gold.evaluation_calculated_channel_fact"
_DIM = "spark_catalog.gold.evaluation_calculated_channel_dimension"
_METRICS = "spark_catalog.gold.evaluation_calculated_channel_metrics"


def _config(silver_table="container_metrics", is_enabled=False, calculated_channels=None):
    kwargs = dict(
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
    if calculated_channels is not None:
        kwargs["calculated_channels"] = calculated_channels
    return ImpulseConfig(**kwargs)


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
    # A *1.0 passthrough channel materializes the physical signal unchanged, so its
    # gold fact rows must be identical to the silver channel rows (see below).
    ch_pass = _add_channel(report, factor=1.0, name="speed_raw")

    report.determine_report()
    report.persist_results()

    assert spark.catalog.tableExists(_FACT)
    assert spark.catalog.tableExists(_DIM)

    fact = spark.read.table(_FACT)
    assert fact.count() > 0
    # Both channels' ids appear in the fact and match their reporting entity ids.
    ids = {r["channel_id"] for r in fact.select("channel_id").distinct().collect()}
    assert ids == {ch.get_id(), ch_pass.get_id()}
    # Identity is NOT on the fact — it lives on the dimension, joined via channel_id.
    assert "identity" not in fact.columns
    assert fact.columns == ["container_id", "channel_id", "tstart", "tend", "value", "_created_at"]

    # The silver source rows for the physical channel this derived signal is built from.
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
    calc_sum = fact.filter(F.col("channel_id") == ch.get_id()).select(F.sum("value")).first()[0]
    assert calc_sum == pytest.approx(raw_sum * 3.6)

    # End-to-end passthrough invariant: a *1.0 calculated channel reproduces the
    # silver signal exactly. Compare the gold fact rows to the silver channel rows
    # on (container_id, tstart, tend, value). channel_id is intentionally excluded
    # (the derived channel has its own identity-derived id); value is compared
    # numerically since silver stores int and the derived signal is float64.
    gold_pass = {
        (r["container_id"], r["tstart"], r["tend"], float(r["value"]))
        for r in fact.filter(F.col("channel_id") == ch_pass.get_id())
        .select("container_id", "tstart", "tend", "value")
        .collect()
    }
    silver_rows = {
        (r["container_id"], r["tstart"], r["tend"], float(r["value"]))
        for r in raw.select("container_id", "tstart", "tend", "value").collect()
    }
    assert gold_pass == silver_rows

    # The dimension carries the identity (a map), keyed by channel_id — the join
    # target for the fact. Confirm the fact's channel_id resolves to the identity.
    dim = spark.read.table(_DIM)
    assert dim.schema["identity"].dataType == T.MapType(T.StringType(), T.StringType())
    dim_row = (
        dim.filter(F.col("channel_id") == ch.get_id())
        .select(
            F.col("identity").getItem("channel_name").alias("cn"),
            F.col("identity").getItem("data_key").alias("dk"),
            "definition_hash",
        )
        .collect()
    )
    assert len(dim_row) == 1
    assert dim_row[0]["cn"] == "speed_kmh"
    assert dim_row[0]["dk"] == "CALC"
    assert dim_row[0]["definition_hash"] is not None


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


def test_incremental_modified_container_fewer_rows_deletes_stale(spark):
    """Regression: a modified container producing FEWER rows under an UNCHANGED
    definition must not leave stale gold fact rows behind.

    Calculated-channel facts are 1:1 with the silver signal intervals, so cutting
    container 1's source rows deterministically shrinks its gold facts. Container 1
    is flagged updated (newer silver ``timestamp``); containers 2 & 3 keep an old
    timestamp so they are not reprocessed and must stay byte-identical.
    """
    # Run 1 (full): seed gold from the base silver tables (container 1 has 50
    # source intervals for the Vehicle Speed Sensor → 50 calc-channel fact rows).
    r1 = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config(is_enabled=False)),
    )
    ch = _add_channel(r1, factor=3.6)
    r1.determine_report()
    r1.persist_results()

    fact_pre = spark.read.table(_FACT).filter(F.col("channel_id") == ch.get_id())
    c1_before = fact_pre.filter(F.col("container_id") == 1).count()
    others_before = {
        (r["container_id"], r["tstart"], r["value"])
        for r in fact_pre.filter(F.col("container_id") != 1)
        .select("container_id", "tstart", "value")
        .collect()
    }
    assert c1_before > 5  # there is real shrink headroom

    shrink_cm = "spark_catalog.silver.container_metrics_shrink"
    shrink_channels = "spark_catalog.silver.channels_shrink"
    try:
        # Modified silver: bump container 1's timestamp (→ detected as updated),
        # keep 2 & 3 old (→ skipped). Vehicle Speed Sensor is channel_id 7; keep
        # only the first 5 of container 1's intervals for it, everything else intact.
        clone_silver_with_shrunk_container(
            spark,
            updated_container_id=1,
            shrink_channel_ids=[7],
            keep_n=5,
            cm_table=shrink_cm,
            channels_table=shrink_channels,
        )

        # Run 2 (incremental): unchanged definition, container 1 now yields 5 rows.
        cfg = ImpulseConfig(
            source=Source(
                container_metrics_table=shrink_cm,
                channel_metrics_table="spark_catalog.silver.channel_metrics",
                channels_uri=shrink_channels,
            ),
            unity_sink=UnitySink(
                catalog="spark_catalog", schema="gold", table_prefix="evaluation"
            ),
            incremental=IncrementalConfig(
                enabled=True,
                silver_last_modified_column="timestamp",
                gold_last_modified_column="_created_at",
            ),
        )
        r2 = Report(
            name="calc_channel_report",
            spark=spark,
            workspace_client=create_autospec(WorkspaceClient),
            config=dict(cfg),
        )
        ch2 = _add_channel(r2, factor=3.6)
        assert ch2.get_id() == ch.get_id()  # unchanged definition
        r2.determine_report()
        r2.persist_results()

        fact_post = spark.read.table(_FACT).filter(F.col("channel_id") == ch.get_id())
        # Container 1 shrank to exactly 5 rows — no stale orphans survive.
        assert fact_post.filter(F.col("container_id") == 1).count() == 5
        # Containers 2 & 3 were not reprocessed → untouched.
        others_after = {
            (r["container_id"], r["tstart"], r["value"])
            for r in fact_post.filter(F.col("container_id") != 1)
            .select("container_id", "tstart", "value")
            .collect()
        }
        assert others_after == others_before
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {shrink_cm}")
        spark.sql(f"DROP TABLE IF EXISTS {shrink_channels}")


def test_incremental_changed_definition_replaces(spark):
    # Metrics enabled so this also covers a changed-definition refresh of the
    # metrics table (no extra Report runs / no added suite time).
    cc = CalculatedChannels(emit_channel_metrics=True)

    # Seed gold with factor 3.6.
    r1 = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config(is_enabled=False, calculated_channels=cc)),
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
    # Metric mean under the 3.6 definition (container 1), captured before the change.
    mean_before = (
        spark.read.table(_METRICS)
        .filter((F.col("channel_id") == ch1.get_id()) & (F.col("container_id") == 1))
        .first()["mean"]
    )

    # Re-run incrementally with a changed factor → the changed-definition path
    # rewrites the rows in the unified MERGE (scoped by channel_id).
    r2 = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config(is_enabled=True, calculated_channels=cc)),
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

    # The metrics row was recomputed for the changed definition: the signal is
    # `Vehicle Speed Sensor * factor`, so its duration-weighted mean scales
    # linearly with the factor (3.6 → 3.7).
    mean_after = (
        spark.read.table(_METRICS)
        .filter((F.col("channel_id") == ch2.get_id()) & (F.col("container_id") == 1))
        .first()["mean"]
    )
    assert mean_after == pytest.approx(mean_before * 3.7 / 3.6)


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
    # Identity is dimension-only; the fact projection carries just the silver columns.
    assert "identity" not in df.columns
    ids = {r["channel_id"] for r in df.select("channel_id").distinct().collect()}
    assert ids == {ch.get_id()}


def test_channel_metrics_not_emitted_by_default(spark):
    # Flag off (default) → no calculated_channel_metrics table is written even
    # when channels carry attributes.
    report = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config(is_enabled=False)),
    )
    _add_channel(report, factor=3.6)
    report.determine_report()
    report.persist_results()

    assert spark.catalog.tableExists(_FACT)
    assert not spark.catalog.tableExists(_METRICS)


def test_channel_metrics_emitted_and_usable_as_impulse_source(spark):
    # Opt in to the metrics table, with `unit` surfaced as an attribute column.
    report = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(
            _config(
                is_enabled=False,
                calculated_channels=CalculatedChannels(
                    emit_channel_metrics=True, attribute_columns=["unit"]
                ),
            )
        ),
    )
    q = report.get_db().query
    ch = CalculatedChannel(
        name="speed_kmh",
        expr=q.channel(channel_name="Vehicle Speed Sensor") * 3.6,
        identity={"channel_name": "speed_kmh", "data_key": "CALC"},
        attributes={"unit": "km/h"},
    )
    report.add_calculated_channel(ch)

    report.determine_report()
    report.persist_results()

    assert spark.catalog.tableExists(_METRICS)
    metrics = spark.read.table(_METRICS)
    # Dynamic identity columns (channel_name, data_key) + configured attribute (unit)
    # + fixed metric columns; identity keys always present, attribute opt-in.
    for col in [
        "container_id",
        "channel_id",
        "channel_name",
        "data_key",
        "unit",
        "value_type",
        "duration",
        "min",
        "max",
        "mean",
    ]:
        assert col in metrics.columns, col

    row = metrics.filter(F.col("channel_id") == ch.get_id()).first()
    assert row["value_type"] == "double"
    assert row["channel_name"] == "speed_kmh"
    assert row["data_key"] == "CALC"
    assert row["unit"] == "km/h"
    # One metrics row per (container, channel).
    fact = spark.read.table(_FACT)
    n_pairs = fact.select("container_id", "channel_id").distinct().count()
    assert metrics.count() == n_pairs

    # Numeric KPIs match a hand-computed, duration-weighted aggregate of the gold
    # fact rows (container 1). Independently reproduces the production semantics so
    # a regression in the KPI math is caught end to end.
    fact_rows = (
        fact.filter((F.col("channel_id") == ch.get_id()) & (F.col("container_id") == 1))
        .select("tstart", "tend", "value")
        .collect()
    )
    durations = [r["tend"] - r["tstart"] for r in fact_rows]
    values = [r["value"] for r in fact_rows]
    exp_duration = max(r["tend"] for r in fact_rows) - min(r["tstart"] for r in fact_rows)
    exp_mean = sum(v * d for v, d in zip(values, durations, strict=True)) / sum(durations)
    m_row = metrics.filter(
        (F.col("channel_id") == ch.get_id()) & (F.col("container_id") == 1)
    ).first()
    assert m_row["duration"] == exp_duration
    assert m_row["min"] == pytest.approx(min(values))
    assert m_row["max"] == pytest.approx(max(values))
    assert m_row["mean"] == pytest.approx(exp_mean)

    # Round-trip: the fact + metrics pair is a valid Impulse silver source. Feed
    # them back as `channels` + `channel_metrics` (wide model: channel_name is a
    # column on channel_metrics) and resolve the channel by name. A minimal
    # `container_metrics` (one row per container) satisfies the filter pipeline.
    fact_as_channels = spark.read.table(_FACT).select(
        "container_id", "channel_id", "tstart", "tend", "value"
    )
    container_metrics = fact_as_channels.select("container_id").distinct()
    db = MeasurementDB(
        MeasurementDBConfig.for_debug(
            {
                "channels": fact_as_channels,
                "channel_metrics": metrics,
                "container_metrics": container_metrics,
            }
        ),
        ws=report.ws,
    )
    solved = (
        db.query.channel(channel_name="speed_kmh").alias("v").solve(spark, report.get_solver())
    )
    assert solved.count() > 0


def test_channel_metrics_incremental_is_idempotent(spark):
    # Seed gold (full) with the metrics table, then re-run incrementally with the
    # same definition/data: the metrics upsert on (container_id, channel_id) must
    # keep the row count stable (one row per container/channel).
    cc = CalculatedChannels(emit_channel_metrics=True)

    r1 = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config(is_enabled=False, calculated_channels=cc)),
    )
    _add_channel(r1, factor=3.6)
    r1.determine_report()
    r1.persist_results()
    count_before = spark.read.table(_METRICS).count()
    assert count_before > 0

    r2 = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(_config(is_enabled=True, calculated_channels=cc)),
    )
    _add_channel(r2, factor=3.6)
    r2.determine_report()
    r2.persist_results()

    assert spark.read.table(_METRICS).count() == count_before


def test_channel_metrics_custom_kpis(spark):
    # A non-default KPI selection controls which KPI columns are emitted.
    report = Report(
        name="calc_channel_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(
            _config(
                is_enabled=False,
                calculated_channels=CalculatedChannels(
                    emit_channel_metrics=True, kpis=["min", "max"]
                ),
            )
        ),
    )
    _add_channel(report, factor=3.6)
    report.determine_report()
    report.persist_results()

    metrics = spark.read.table(_METRICS)
    # Only the selected KPIs are present; the dropped defaults are absent.
    assert "min" in metrics.columns
    assert "max" in metrics.columns
    assert "mean" not in metrics.columns
    assert "duration" not in metrics.columns
