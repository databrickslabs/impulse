# pylint: disable=missing-function-docstring
"""End-to-end integration test for a customer-registered custom solver.

Demonstrates the pluggable-solver contract via a column-redaction example: a
customer subclasses ``DefaultSolver``, registers it with a ``SolverConfig``
subclass carrying an extra ``redact_columns`` field, builds it via
``from_config``, and runs the same solve flow as ``default_solver_test``.

Asserts that the pipeline still resolves all containers and that the configured
columns are masked in the result.
"""

import pyspark.sql.functions as F
import pytest
from pyspark.sql import DataFrame, SparkSession

from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.registry import register_solver
from impulse_query_engine.analyze.query.solvers.solver_config import (
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.analyze.query.solvers.solver_context import SolverBuildContext
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig


class RedactConfig(SolverConfig):
    """SolverConfig subclass adding a column-redaction switch."""

    redact_columns: list[str] = []


@register_solver("RedactingSolver", RedactConfig)
class RedactingSolver(DefaultSolver):
    """DefaultSolver that nulls out configured columns in the solve output."""

    def solve(self, query, channels_df, selections, dtypes) -> DataFrame:  # noqa: D102
        df = super().solve(query, channels_df, selections, dtypes)
        for col in self.config.redact_columns:
            if col in df.columns:
                df = df.withColumn(col, F.lit(None).cast(df.schema[col].dataType))
        return df


def _redact_cfg(redact_columns: list[str]) -> RedactConfig:
    """RedactConfig wired for the KVS test data (mirrors default_solver_test._kvs_cfg)."""
    return RedactConfig(
        project_id="SAMPLE_PROJECT",
        container_tags=TableConfig(column_name_mapping={"element_id": "key"}),
        container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
        redact_columns=redact_columns,
    )


class TestCustomSolverIntegration:
    def test_registered_custom_solver_built_via_from_config_runs(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """A custom solver built through the from_config hook runs the full pipeline."""
        cfg = _redact_cfg(redact_columns=[])
        solver = RedactingSolver.from_config(SolverBuildContext(spark=spark, solver_config=cfg))
        assert isinstance(solver, RedactingSolver)

        query = key_value_store_db.query
        eng_rpm = query.channel(channel_name="Engine RPM")
        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark=spark, solver=solver)

        assert {row.container_id for row in result.collect()} == {1, 2, 3}

    def test_redaction_masks_configured_column(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """The configured column is nulled while container_id still resolves."""
        cfg = _redact_cfg(redact_columns=["rpm_mean"])
        solver = RedactingSolver.from_config(SolverBuildContext(spark=spark, solver_config=cfg))

        query = key_value_store_db.query
        eng_rpm = query.channel(channel_name="Engine RPM")
        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark=spark, solver=solver)

        rows = result.collect()
        assert {row.container_id for row in rows} == {1, 2, 3}
        # Every rpm_mean value has been redacted to NULL.
        assert all(row.rpm_mean is None for row in rows)

    def test_without_redaction_column_has_values(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """Sanity check: without redaction the same column carries real values."""
        cfg = _redact_cfg(redact_columns=[])
        solver = RedactingSolver.from_config(SolverBuildContext(spark=spark, solver_config=cfg))

        query = key_value_store_db.query
        eng_rpm = query.channel(channel_name="Engine RPM")
        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark=spark, solver=solver)

        rows = result.collect()
        assert any(row.rpm_mean is not None for row in rows)


# ---------------------------------------------------------------------------
# Custom solver that UNIONs two channel-data sources via the load_channels seam
# ---------------------------------------------------------------------------

_UNION_SCHEMA = "spark_catalog.silver_union_channels"


class UnionChannelsConfig(SolverConfig):
    """SolverConfig subclass naming a SECOND channel-data table to union in.

    ``extra_channels_table`` is validated at config-parse time like any other
    field on the subclass.
    """

    extra_channels_table: str


@register_solver("UnionChannelsSolver", UnionChannelsConfig)
class UnionChannelsSolver(DefaultSolver):
    """DefaultSolver that loads channel data from TWO configured tables.

    Overrides only :meth:`load_channels`: instead of reading the single
    ``channels`` table, it ``unionByName``s the primary table with a second
    table named in the solver config. Everything downstream (column mapping,
    encoding, the solve join) is inherited unchanged.
    """

    def load_channels(self, db, spark) -> DataFrame:
        primary = db.channels(spark)
        extra = spark.read.table(self.config.extra_channels_table)
        return primary.unionByName(extra)


@pytest.fixture
def union_channels_db(spark: SparkSession, mock_workspace_client) -> MeasurementDB:
    """Seed a DB whose channel data for a channel is split across two tables.

    Container 1 / channel 5 has its intervals split: the primary ``channels``
    table holds the first half, ``channels_extra`` holds the second half. A
    solver reading only the primary table sees fewer samples than one that
    unions both.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_UNION_SCHEMA}")
    for table in spark.sql(f"SHOW TABLES IN {_UNION_SCHEMA}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {_UNION_SCHEMA}.{table.tableName} PURGE")

    container_metrics = spark.createDataFrame(
        [(1, "SAMPLE_PROJECT")],
        schema="container_id long, project_id string",
    )
    channel_metrics = spark.createDataFrame(
        [(1, 5, "Engine RPM")],
        schema="container_id long, channel_id int, channel_name string",
    )
    channels_schema = "container_id long, channel_id int, tstart long, tend long, value double"
    # Primary source: two low-value intervals.
    channels_primary = spark.createDataFrame(
        [
            (1, 5, 0, 1_000_000, 500.0),
            (1, 5, 1_000_000, 2_000_000, 700.0),
        ],
        schema=channels_schema,
    )
    # Second source: two high-value intervals continuing the same channel.
    channels_extra = spark.createDataFrame(
        [
            (1, 5, 2_000_000, 3_000_000, 6000.0),
            (1, 5, 3_000_000, 4_000_000, 7000.0),
        ],
        schema=channels_schema,
    )

    container_metrics.write.format("delta").mode("overwrite").saveAsTable(
        f"{_UNION_SCHEMA}.container_metrics"
    )
    channel_metrics.write.format("delta").mode("overwrite").saveAsTable(
        f"{_UNION_SCHEMA}.channel_metrics"
    )
    channels_primary.write.format("delta").mode("overwrite").saveAsTable(
        f"{_UNION_SCHEMA}.channels"
    )
    channels_extra.write.format("delta").mode("overwrite").saveAsTable(
        f"{_UNION_SCHEMA}.channels_extra"
    )

    tables = {
        "container_metrics": spark.read.table(f"{_UNION_SCHEMA}.container_metrics"),
        "channel_metrics": spark.read.table(f"{_UNION_SCHEMA}.channel_metrics"),
        "channels": spark.read.table(f"{_UNION_SCHEMA}.channels"),
    }
    cfg = MeasurementDBConfig.for_debug(tables)
    db = MeasurementDB(cfg, ws=mock_workspace_client)
    yield db
    spark.sql(f"DROP SCHEMA IF EXISTS {_UNION_SCHEMA} CASCADE")


class TestUnionChannelsSolver:
    """A custom solver can union two configured channel-data sources."""

    def _run(self, spark, db, solver) -> dict:
        query = db.query
        rpm = query.channel(channel_name="Engine RPM")
        result = query.select(
            rpm.min().alias("rpm_min"),
            rpm.max().alias("rpm_max"),
        ).solve(spark=spark, solver=solver)
        return {row.container_id: row for row in result.collect()}[1]

    def test_baseline_default_solver_sees_only_primary_table(
        self, spark: SparkSession, union_channels_db: MeasurementDB
    ):
        """The stock DefaultSolver reads only the primary channels table."""
        solver = DefaultSolver(spark, config=SolverConfig())
        row = self._run(spark, union_channels_db, solver)
        # Only the primary source's low values are present.
        assert row.rpm_min == 500.0
        assert row.rpm_max == 700.0

    def test_union_solver_combines_both_channel_sources(
        self, spark: SparkSession, union_channels_db: MeasurementDB
    ):
        """The custom solver unions both tables, so the high values appear too."""
        cfg = UnionChannelsConfig(extra_channels_table=f"{_UNION_SCHEMA}.channels_extra")
        solver = UnionChannelsSolver.from_config(
            SolverBuildContext(spark=spark, solver_config=cfg)
        )
        assert isinstance(solver.config, UnionChannelsConfig)

        row = self._run(spark, union_channels_db, solver)
        # min from the primary source, max from the second source — both tables
        # contributed to the solved series.
        assert row.rpm_min == 500.0
        assert row.rpm_max == 7000.0


# ---------------------------------------------------------------------------
# The seam generalizes: the SAME override pattern works for container_metrics
# ---------------------------------------------------------------------------


class UnionContainersConfig(SolverConfig):
    """Names a second container_metrics table to union in."""

    extra_container_metrics_table: str


@register_solver("UnionContainersSolver", UnionContainersConfig)
class UnionContainersSolver(DefaultSolver):
    """DefaultSolver that unions two container_metrics tables via the seam."""

    def load_container_metrics(self, db, spark) -> DataFrame:
        primary = db.container_metrics(spark)
        extra = spark.read.table(self.config.extra_container_metrics_table)
        return primary.unionByName(extra)


class TestUnionContainersSolver:
    """The load_* seam approach is not channels-specific — container_metrics too."""

    def test_union_solver_adds_containers_from_second_table(
        self, spark: SparkSession, union_channels_db: MeasurementDB
    ):
        """Unioning a second container table brings container 2 into the result."""
        # A second container_metrics table introducing container 2 (with its
        # own channel data already present in the primary channels table).
        extra_containers = spark.createDataFrame(
            [(2, "SAMPLE_PROJECT")],
            schema="container_id long, project_id string",
        )
        extra_containers.write.format("delta").mode("overwrite").saveAsTable(
            f"{_UNION_SCHEMA}.container_metrics_extra"
        )
        # Give container 2 a channel + channel data so it solves to a row.
        spark.createDataFrame(
            [(2, 5, "Engine RPM")],
            schema="container_id long, channel_id int, channel_name string",
        ).write.format("delta").mode("append").saveAsTable(f"{_UNION_SCHEMA}.channel_metrics")
        spark.createDataFrame(
            [(2, 5, 0, 1_000_000, 900.0)],
            schema="container_id long, channel_id int, tstart long, tend long, value double",
        ).write.format("delta").mode("append").saveAsTable(f"{_UNION_SCHEMA}.channels")

        # Rebuild the DB handle so it sees the appended channel rows.
        tables = {
            "container_metrics": spark.read.table(f"{_UNION_SCHEMA}.container_metrics"),
            "channel_metrics": spark.read.table(f"{_UNION_SCHEMA}.channel_metrics"),
            "channels": spark.read.table(f"{_UNION_SCHEMA}.channels"),
        }
        db = MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=union_channels_db.ws)

        cfg = UnionContainersConfig(
            extra_container_metrics_table=f"{_UNION_SCHEMA}.container_metrics_extra"
        )
        solver = UnionContainersSolver.from_config(
            SolverBuildContext(spark=spark, solver_config=cfg)
        )

        query = db.query
        rpm = query.channel(channel_name="Engine RPM")
        result = query.select(rpm.mean().alias("rpm_mean")).solve(spark=spark, solver=solver)

        # Baseline primary container_metrics has only container 1; the union
        # brings in container 2 from the second table.
        assert {row.container_id for row in result.collect()} == {1, 2}
