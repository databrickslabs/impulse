# pylint: disable=missing-function-docstring
"""Report-factory integration for a customer-registered custom solver.

Covers the reporting-layer seam that the query-engine tests don't reach:

  report config (``query_engine.solver`` = a registered name) →
  ``Report.create_solver`` → registry ``resolve_registration`` →
  ``SolverBuildContext`` → ``from_config`` → the custom solver instance,
  with its ``SolverConfig`` subclass validated and typed.

Uses a ``solve()``-override custom solver (no ``load_*`` seams — those were
removed as too invasive); the override is exercised directly by the query-engine
``custom_solver_test``. Here we assert the *report factory* routes a custom name
through the registry and builds the right, fully-wired solver, and that solving
through it actually changes the output relative to ``DefaultSolver``.

The custom solver registers at run time via the ``_register_report_custom_solver``
fixture (which depends on the shared ``registry_isolation`` fixture in
``conftest.py``), not via a module-level ``@register_solver`` decorator: a
decorator would mutate the process-global registry at import time, before any
snapshot is taken, and leak into other tests.
"""

from unittest.mock import create_autospec

import pyspark.sql.functions as F
import pytest
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame, SparkSession

from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.registry import register_solver
from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig
from impulse_reporting.config.config_parser import (
    DataType,
    ImpulseConfig,
    QueryEngine,
    Source,
    UnitySink,
)
from impulse_reporting.core.report import Report


class ReportCustomConfig(SolverConfig):
    """SolverConfig subclass carrying an extra, report-config-validated field."""

    redact_columns: list[str] = []


class ReportCustomSolver(DefaultSolver):
    """DefaultSolver variant selected by name from a report config.

    Overrides only ``solve`` (the supported, non-seam extension point): it defers
    to the inherited pipeline via ``super()`` and nulls out configured columns.
    """

    def solve(self, query, channels_df, selections, dtypes) -> DataFrame:  # noqa: D102
        df = super().solve(query, channels_df, selections, dtypes)
        for col in self.config.redact_columns:
            if col in df.columns:
                df = df.withColumn(col, F.lit(None).cast(df.schema[col].dataType))
        return df


@pytest.fixture
def _register_report_custom_solver(registry_isolation):
    """Register ``ReportCustomSolver`` for the duration of a test.

    Registration happens here (run time), not via a module-level decorator, so
    the shared ``registry_isolation`` fixture can restore the registry afterwards.
    """
    register_solver("ReportCustomSolver", ReportCustomConfig)(ReportCustomSolver)
    yield


def _config(solver_name: str, solver_config: dict | None = None) -> ImpulseConfig:
    """Minimal ImpulseConfig pointing at the ``silver_narrow_db`` fixture tables."""
    schema = "spark_catalog.silver_narrow_db"
    query_engine = QueryEngine(solver=solver_name, data_type=DataType.RLE)
    if solver_config is not None:
        query_engine = QueryEngine(
            solver=solver_name, data_type=DataType.RLE, solver_config=solver_config
        )
    return ImpulseConfig(
        source=Source(
            container_tags_table=f"{schema}.container_tags",
            channel_tags_table=f"{schema}.channel_tags",
            container_metrics_table=f"{schema}.container_metrics",
            channel_metrics_table=f"{schema}.channel_metrics",
            channels_uri=f"{schema}.channels",
        ),
        unity_sink=UnitySink(catalog="spark_catalog", schema="gold", table_prefix="custom_solver"),
        measurement_dimensions=["container_id"],
        query_engine=query_engine,
    )


def _report(spark: SparkSession, config: ImpulseConfig) -> Report:
    return Report(
        name="custom_solver_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(config),
    )


def _run_query_through_configured_solver(report: Report) -> list:
    """Run the same single-channel query through ``report`` and collect the rows.

    The ``silver_narrow_db`` fixture has one container (id=1) whose ``seed``
    channel carries values 1..10, so ``DefaultSolver`` yields ``seed_mean=5.5``.
    """
    query = report.get_db().query
    seed = query.channel(seed="0")
    result = query.select(seed.mean().alias("seed_mean")).solve(
        spark=report.spark, solver=report.get_solver()
    )
    return result.collect()


class TestCustomSolverReportFactory:
    """``Report.create_solver`` resolves custom names through the registry."""

    def test_report_builds_registered_custom_solver_with_typed_config(
        self, spark: SparkSession, setup_narrow_db, _register_report_custom_solver
    ):
        """A report config naming a custom solver builds that class via the registry,
        with its ``solver_config`` validated as the registered subclass."""
        config = _config(
            "ReportCustomSolver",
            solver_config={"redact_columns": ["rpm_mean"]},
        )

        report = _report(spark, config)

        solver = report.get_solver()
        # Routed through the registry to the custom class (not DefaultSolver)...
        assert isinstance(solver, ReportCustomSolver)
        # ...and its solver_config was validated/typed as the registered subclass,
        # preserving the extra field rather than dropping it to base SolverConfig.
        assert isinstance(solver.config, ReportCustomConfig)
        assert solver.config.redact_columns == ["rpm_mean"]

    def test_report_defaults_to_default_solver(
        self, spark: SparkSession, setup_narrow_db, _register_report_custom_solver
    ):
        """Sanity check: the built-in name still resolves to DefaultSolver via the
        same registry-backed factory (custom registration doesn't disturb it)."""
        report = _report(spark, _config("DefaultSolver"))

        solver = report.get_solver()
        assert isinstance(solver, DefaultSolver)
        assert not isinstance(solver, ReportCustomSolver)

    def test_custom_solver_changes_solve_output_vs_default(
        self, spark: SparkSession, setup_narrow_db, _register_report_custom_solver
    ):
        """The custom solver is *effective*: running the same query through it
        alters the result relative to ``DefaultSolver``.

        Both reports resolve the single fixture container (id=1). ``DefaultSolver``
        computes the real ``seed_mean`` (5.5); the custom solver, configured to
        redact ``seed_mean``, resolves the same container but nulls that column.
        The only difference between the two runs is the selected solver, so the
        changed output is attributable to the custom solver alone.
        """
        default_rows = _run_query_through_configured_solver(
            _report(spark, _config("DefaultSolver"))
        )
        custom_rows = _run_query_through_configured_solver(
            _report(
                spark,
                _config("ReportCustomSolver", solver_config={"redact_columns": ["seed_mean"]}),
            )
        )

        # Same container set resolved by both solvers.
        assert {row.container_id for row in default_rows} == {1}
        assert {row.container_id for row in custom_rows} == {1}

        # DefaultSolver produces the real aggregate; the custom solver redacts it.
        assert default_rows[0].seed_mean == 5.5
        assert custom_rows[0].seed_mean is None
