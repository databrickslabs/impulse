# pylint: disable=missing-function-docstring
"""End-to-end: a customer-provided solver, selected from the report config.

The custom solver drops the containers listed in its own config setting from the solve output.
Each test covers one promise of the extension point: the report builds the selected solver with its
typed config and solves through it, and registering a custom solver leaves the built-in
``DefaultSolver`` unchanged.
"""

from unittest.mock import create_autospec

import pyspark.sql.functions as F
import pytest
from databricks.sdk import WorkspaceClient
from pyspark.sql import Row

from impulse_query_engine.analyze.query.solvers import DefaultSolver, SolverConfig, register_solver
from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report


class ExcludeContainersConfig(SolverConfig):
    excluded_container_ids: list[int]


class ExcludeContainersSolver(DefaultSolver):
    """Drops the containers listed in ``excluded_container_ids`` from the solve output."""

    def solve(self, query, channels_df, selections, dtypes):
        solved = super().solve(query, channels_df, selections, dtypes)
        return solved.filter(
            ~F.col(self.config.container_id_col).isin(self.config.excluded_container_ids)
        )


@pytest.fixture(autouse=True)
def exclude_containers_solver(isolated_registries):
    register_solver("ExcludeContainersSolver", ExcludeContainersConfig)(ExcludeContainersSolver)


def _create_report(spark, query_engine: dict) -> Report:
    config = {
        "source": {
            "container_metrics_table": "spark_catalog.silver.container_metrics",
            "channel_metrics_table": "spark_catalog.silver.channel_metrics",
            "channels_uri": "spark_catalog.silver.channels",
        },
        "unity_sink": {"catalog": "spark_catalog", "schema": "gold", "table_prefix": "e2e_solver"},
        "query_engine": query_engine,
    }
    report = Report(
        name="e2e_custom_solver",
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


def test_report_solves_through_selected_solver(spark):
    report = _create_report(
        spark,
        {"solver": "ExcludeContainersSolver", "solver_config": {"excluded_container_ids": [3]}},
    )
    report.run(persist_results=False)

    # The report built the registered solver, and its typed config setting reached it...
    solver = report.get_solver()
    assert type(solver) is ExcludeContainersSolver
    assert solver.config.excluded_container_ids == [3]

    # ...and every solve went through it: container 3 is missing from the histogram.
    # The run splits results into {"changed", "unchanged"} definition buckets; exactly one
    # holds this run's histogram.
    [histogram] = [df for df in report.aggregation_dfs["HISTOGRAM"].values() if df is not None]
    assert histogram.select("container_id").distinct().orderBy("container_id").collect() == [
        Row(container_id=1),
        Row(container_id=2),
    ]


def test_default_solver_is_unchanged_by_the_registration(spark):
    report = _create_report(spark, {"solver": "DefaultSolver"})
    report.run(persist_results=False)

    assert type(report.get_solver()) is DefaultSolver
    [histogram] = [df for df in report.aggregation_dfs["HISTOGRAM"].values() if df is not None]
    assert histogram.select("container_id").distinct().orderBy("container_id").collect() == [
        Row(container_id=1),
        Row(container_id=2),
        Row(container_id=3),
    ]
