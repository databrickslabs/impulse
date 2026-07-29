# pylint: disable=missing-function-docstring
"""End-to-end report test driven by a custom solver selected via config.

Scenario (a split silver layer): container information is split
across TWO physical tables — a base ``container_metrics`` table and a separate
``vehicle_info`` table that carries the ``brand`` column. A customer ships a
solver that JOINs the two into the container silver frame and prefilters rows to
a configurable set of brands. Both the second table's location and the brand
allowlist live in the solver's own ``SolverConfig`` subclass, so no Impulse core
change is needed — the report config just names the solver and its extra config.

This exercises the full pluggable-solver path end to end:
  report config (JSON dict) → registry resolves "MultiTableBrandSolver" →
  extended config validated as MultiTableBrandConfig → solver built via
  from_config → Report.determine_report runs the pipeline → the brand prefilter
  is reflected in the container dimension and the aggregation results.

The custom solver is defined in this module; importing the module registers it
(the same mechanism a customer package would use).
"""

import datetime
from unittest.mock import create_autospec

import pyspark.sql.functions as F
import pytest
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame, SparkSession

from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.registry import register_solver
from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig
from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report

_SCHEMA = "spark_catalog.silver_custom_solver"


class MultiTableBrandConfig(SolverConfig):
    """SolverConfig subclass carrying the second table + brand prefilter.

    ``vehicle_info_table`` is required (validated at config-parse time);
    ``include_brands`` is an optional allowlist — empty means no brand filter.
    """

    vehicle_info_table: str
    include_brands: list[str] = []


@register_solver("MultiTableBrandSolver", MultiTableBrandConfig)
class MultiTableBrandSolver(DefaultSolver):
    """DefaultSolver that combines two container tables and prefilters by brand.

    Overrides only the ``load_container_metrics`` seam: it joins the base
    ``container_metrics`` table with a separate ``vehicle_info`` table (read
    from ``self.config.vehicle_info_table``) and applies the configurable brand
    allowlist. Because the seam is the single source of container rows, both the
    solve pipeline *and* incremental container detection read the same combined,
    brand-filtered set — the rest of the pipeline is inherited unchanged.
    """

    def load_container_metrics(self, db, spark) -> DataFrame:
        base = db.container_metrics(spark)
        vehicle_info = spark.read.table(self.config.vehicle_info_table)
        combined = base.join(vehicle_info, on=self.config.container_id_col, how="inner")

        if self.config.include_brands:
            combined = combined.where(F.col("brand").isin(self.config.include_brands))

        return combined


@pytest.fixture(scope="module")
def setup_custom_solver_db(spark: SparkSession):
    """Seed two container tables (base + vehicle_info) plus channel data.

    Four containers across three brands:
      1, 2 → Seat   3 → VW   4 → Audi
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_SCHEMA}")
    for table in spark.sql(f"SHOW TABLES IN {_SCHEMA}").collect():
        spark.sql(f"DROP TABLE IF EXISTS {_SCHEMA}.{table.tableName} PURGE")

    ts = datetime.datetime(2025, 7, 1, 12, 0, 0)

    # Table 1: base container_metrics (no brand column).
    base_container_metrics = spark.createDataFrame(
        [
            (1, "UUT_1", "SAMPLE_PROJECT", ts),
            (2, "UUT_2", "SAMPLE_PROJECT", ts),
            (3, "UUT_3", "SAMPLE_PROJECT", ts),
            (4, "UUT_4", "SAMPLE_PROJECT", ts),
        ],
        schema="container_id long, uut_id string, project_id string, start_dt timestamp",
    )

    # Table 2: vehicle_info — the second table combined in by the custom solver.
    vehicle_info = spark.createDataFrame(
        [
            (1, "Seat", "Leon"),
            (2, "Seat", "Ibiza"),
            (3, "VW", "Golf"),
            (4, "Audi", "A3"),
        ],
        schema="container_id long, brand string, model string",
    )

    # Wide channel_metrics: one selectable channel per container.
    channel_metrics = spark.createDataFrame(
        [(cid, 5, "Engine RPM") for cid in (1, 2, 3, 4)],
        schema="container_id long, channel_id int, channel_name string",
    )

    # RLE channel data: three intervals per container, values within the bins.
    channel_rows = []
    for cid in (1, 2, 3, 4):
        channel_rows += [
            (cid, 5, 0, 1_000_000, 800.0),
            (cid, 5, 1_000_000, 2_000_000, 1500.0),
            (cid, 5, 2_000_000, 3_000_000, 3000.0),
        ]
    channels = spark.createDataFrame(
        channel_rows,
        schema="container_id long, channel_id int, tstart long, tend long, value double",
    )

    base_container_metrics.write.format("delta").mode("overwrite").saveAsTable(
        f"{_SCHEMA}.base_container_metrics"
    )
    vehicle_info.write.format("delta").mode("overwrite").saveAsTable(f"{_SCHEMA}.vehicle_info")
    channel_metrics.write.format("delta").mode("overwrite").saveAsTable(
        f"{_SCHEMA}.channel_metrics"
    )
    channels.write.format("delta").mode("overwrite").saveAsTable(f"{_SCHEMA}.channels")

    yield

    spark.sql(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")


def _config(include_brands: list[str]) -> dict:
    """Build a report config dict selecting the custom solver by name."""
    return {
        "source": {
            "container_metrics_table": f"{_SCHEMA}.base_container_metrics",
            "channel_metrics_table": f"{_SCHEMA}.channel_metrics",
            "channels_uri": f"{_SCHEMA}.channels",
        },
        "unity_sink": {
            "catalog": "spark_catalog",
            "schema": "gold",
            "table_prefix": "custom_solver_eval",
        },
        "query_engine": {
            "solver": "MultiTableBrandSolver",
            "solver_config": {
                "vehicle_info_table": f"{_SCHEMA}.vehicle_info",
                "include_brands": include_brands,
            },
        },
        "measurement_dimensions": ["container_id", "brand"],
    }


def _build_report(spark: SparkSession, include_brands: list[str]) -> Report:
    report = Report(
        name="custom_solver_report",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=_config(include_brands),
    )
    query = report.get_db().query
    rpm = query.channel(channel_name="Engine RPM")

    page = Page(page_number=1)
    report.add_page(page)
    page.add_aggregation(
        HistogramDuration(
            name="rpm_hist",
            base_expr=rpm,
            bins=[float(i) for i in range(0, 8000, 250)],
        )
    )
    return report


class TestCustomSolverReportE2E:
    def test_config_selects_custom_solver_with_extended_config(
        self, spark: SparkSession, setup_custom_solver_db
    ):
        """The config-named solver is built and its extended config is validated/typed."""
        report = _build_report(spark, include_brands=["Seat"])

        solver = report.get_solver()
        assert isinstance(solver, MultiTableBrandSolver)
        assert isinstance(solver.config, MultiTableBrandConfig)
        assert solver.config.vehicle_info_table == f"{_SCHEMA}.vehicle_info"
        assert solver.config.include_brands == ["Seat"]

    def test_brand_prefilter_limits_containers_and_combines_tables(
        self, spark: SparkSession, setup_custom_solver_db
    ):
        """Combining vehicle_info + prefiltering to Seat yields only containers 1 and 2."""
        report = _build_report(spark, include_brands=["Seat"])

        report.determine_report()

        # container_dimension_df comes straight from the custom filter_container_metrics.
        dim_rows = report.container_dimension_df.collect()
        assert {r.container_id for r in dim_rows} == {1, 2}
        # The `brand` column proves the second table was joined in.
        assert {r.brand for r in dim_rows} == {"Seat"}

        # Aggregation ran and produced non-empty results for the surviving containers.
        assert (
            report.aggregation_dfs["HISTOGRAM"]["changed"].filter(F.col("hist_value") > 0).count()
            > 0
        )

    def test_different_brand_selection(self, spark: SparkSession, setup_custom_solver_db):
        """A different brand allowlist selects the corresponding containers."""
        report = _build_report(spark, include_brands=["VW", "Audi"])
        report.determine_report()

        dim_rows = report.container_dimension_df.collect()
        assert {r.container_id for r in dim_rows} == {3, 4}
        assert {r.brand for r in dim_rows} == {"VW", "Audi"}

    def test_empty_brand_list_keeps_all_containers(
        self, spark: SparkSession, setup_custom_solver_db
    ):
        """No brand allowlist means the prefilter is skipped — all four containers survive."""
        report = _build_report(spark, include_brands=[])
        report.determine_report()

        dim_rows = report.container_dimension_df.collect()
        assert {r.container_id for r in dim_rows} == {1, 2, 3, 4}

    def test_persist_writes_gold_tables(self, spark: SparkSession, setup_custom_solver_db):
        """The full determine + persist path writes gold facts for the filtered set."""
        report = _build_report(spark, include_brands=["Seat"])
        report.determine_report()
        report.persist_results()

        assert spark.catalog.tableExists("spark_catalog.gold.custom_solver_eval_histogram_fact")
        measurement_dim = spark.read.table(
            "spark_catalog.gold.custom_solver_eval_measurement_dimension"
        )
        assert {r.container_id for r in measurement_dim.collect()} == {1, 2}

    def test_incremental_detection_uses_custom_solver_container_source(
        self, spark: SparkSession, setup_custom_solver_db
    ):
        """Incremental container detection reads through the solver's seam.

        The detection input must reflect the custom solver's combined
        (base + vehicle_info) and brand-filtered container set — containers
        {1, 2} for Seat — not the raw base ``container_metrics`` table, which
        alone has all four containers.
        """
        report = _build_report(spark, include_brands=["Seat"])

        args = report._container_detection_args()
        assert args is not None
        _detector, silver_containers, *_ = args

        detected_ids = {r.container_id for r in silver_containers.collect()}
        assert detected_ids == {1, 2}
        # The joined-in `brand` column is present, proving the second table
        # flowed into detection via the seam.
        assert "brand" in silver_containers.columns
