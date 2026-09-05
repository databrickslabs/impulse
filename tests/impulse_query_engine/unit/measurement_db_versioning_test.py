# pylint: disable=missing-function-docstring
"""Tests for per-run Delta version pinning on ``MeasurementDB`` (issue #87).

``pin_versions`` resolves each configured silver table's current Delta version
once at run start; ``_read_table`` then reads with ``versionAsOf`` so every
lazy op in the run observes the same snapshot even if the table changes
mid-run. Debug mode is exempt; non-Delta / unresolvable tables are skipped
with a warning and continue to read the latest version.
"""

from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import spark  # noqa: F401  (pytest fixture)

_PIN_SCHEMA = "spark_catalog.silver_pin_test"


def _db(cfg: MeasurementDBConfig) -> MeasurementDB:
    return MeasurementDB(cfg, ws=create_autospec(WorkspaceClient))


@pytest.fixture
def pin_schema(spark):  # noqa: F811
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_PIN_SCHEMA}")
    yield _PIN_SCHEMA
    spark.sql(f"DROP SCHEMA IF EXISTS {_PIN_SCHEMA} CASCADE")


def test_pin_versions_freezes_snapshot(spark, pin_schema):  # noqa: F811
    table = f"{pin_schema}.container_metrics"
    spark.range(3).toDF("container_id").write.format("delta").mode("overwrite").saveAsTable(table)

    cfg = MeasurementDBConfig(container_metrics_table=table, table_locations="unity_catalog")
    db = _db(cfg)
    db.pin_versions(spark)
    assert cfg.pinned_versions == {table: 0}

    # Mutate the table AFTER pinning but BEFORE the (lazy) read materializes.
    spark.range(3, 10).toDF("container_id").write.format("delta").mode("append").saveAsTable(table)

    # The read still reflects the pinned snapshot, not the 10-row mutated table.
    assert db.container_metrics(spark).count() == 3

    # Without a pin, the same read sees the latest snapshot.
    cfg.pinned_versions = {}
    assert db.container_metrics(spark).count() == 10


def test_pin_versions_freezes_snapshot_path_mode(spark, tmp_path):  # noqa: F811
    # Path mode (``external_locations``): reads and version resolution go through
    # the filesystem path rather than a catalog name.
    path = str(tmp_path / "container_metrics")
    spark.range(3).toDF("container_id").write.format("delta").mode("overwrite").save(path)

    cfg = MeasurementDBConfig(container_metrics_table=path, table_locations="external_locations")
    db = _db(cfg)
    db.pin_versions(spark)
    assert cfg.pinned_versions == {path: 0}

    # Mutate after pinning; the pinned read must still see the original snapshot.
    spark.range(3, 10).toDF("container_id").write.format("delta").mode("append").save(path)
    assert db.container_metrics(spark).count() == 3

    cfg.pinned_versions = {}
    assert db.container_metrics(spark).count() == 10


def test_pin_versions_skips_debug_mode(spark):  # noqa: F811
    cfg = MeasurementDBConfig.for_debug({"container_metrics": spark.range(2).toDF("container_id")})
    db = _db(cfg)
    db.pin_versions(spark)
    # Debug mode is exempt: nothing pinned, and reads return the in-memory df.
    assert cfg.pinned_versions == {}
    assert db.container_metrics(spark).count() == 2


def test_pin_versions_skips_non_delta_and_warns(spark):  # noqa: F811
    cfg = MeasurementDBConfig(
        container_metrics_table="spark_catalog.silver_pin_test.does_not_exist",
        table_locations="unity_catalog",
    )
    db = _db(cfg)
    with pytest.warns(UserWarning, match="Could not pin Delta version"):
        db.pin_versions(spark)
    # Unresolvable table is skipped, leaving the pin map empty (reads latest).
    assert cfg.pinned_versions == {}
