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
from pyspark.sql import DataFrame, SparkSession

from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.registry import register_solver
from impulse_query_engine.analyze.query.solvers.solver_config import (
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.analyze.query.solvers.solver_context import SolverBuildContext
from impulse_query_engine.measurement_db import MeasurementDB


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
