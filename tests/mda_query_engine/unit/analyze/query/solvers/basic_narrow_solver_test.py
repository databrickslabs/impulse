# pylint: disable=missing-function-docstring
"""
Tests for BasicNarrowTimeSeriesCache, BasicNarrowSolver, and SolverConfig.

Covers:
- BasicNarrowTimeSeriesCache with default and custom column configs (via col_map)
- BasicNarrowSolver._solve_udf with col_map
- BasicNarrowSolver.filter_container_metrics / filter_channel_metrics with config
- BasicNarrowSolver.solve end-to-end with config (custom column names)
- SolverConfig entity_id_col field
- Backward compatibility: default config identical to previous hardcoded behaviour
"""

import pandas as pd
from pyspark.sql import SparkSession

from mda_query_engine.analyze.query.solvers.basic_narrow_solver import (
    BasicNarrowSolver,
    BasicNarrowTimeSeriesCache,
)
from mda_query_engine.analyze.query.solvers.solver_config import SolverConfig
from mda_query_engine.measurement_db import MeasurementDB
from tests.conftest import spark

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DEFAULT_COL_MAP = {
    "cid": "container_id",
    "ch": "channel_id",
    "ts": "tstart",
    "te": "tend",
    "val": "value",
}

CUSTOM_COL_MAP = {
    "cid": "meas_id",
    "ch": "sig_id",
    "ts": "t_start",
    "te": "t_stop",
    "val": "val",
}


def _make_channel_pdf(
    cid_col="container_id", ch_col="channel_id", ts_col="tstart", te_col="tend", val_col="value"
):
    """Return a tiny pandas DataFrame with the given column names."""
    return pd.DataFrame(
        {
            cid_col: [1, 1, 2, 2],
            ch_col: [10, 10, 20, 20],
            ts_col: [0, 100, 0, 200],
            te_col: [100, 200, 200, 400],
            val_col: [1.0, 2.0, 3.0, 4.0],
        }
    )


# ---------------------------------------------------------------------------
# TestBasicNarrowTimeSeriesCache
# ---------------------------------------------------------------------------


class TestBasicNarrowTimeSeriesCache:
    """Unit tests for BasicNarrowTimeSeriesCache."""

    def test_default_config_load_blob(self):
        """load_blob works with default column names."""
        pdf = _make_channel_pdf()
        cache = BasicNarrowTimeSeriesCache(pdf, col_map=DEFAULT_COL_MAP)
        series = cache.load_blob(1, 10)
        assert list(series.tstarts) == [0, 100]
        assert list(series.values) == [1.0, 2.0]

    def test_custom_config_load_blob(self):
        """load_blob works with custom column names when matching col_map is given."""
        pdf = _make_channel_pdf(
            cid_col="meas_id", ch_col="sig_id", ts_col="t_start", te_col="t_stop", val_col="val"
        )
        cache = BasicNarrowTimeSeriesCache(pdf, col_map=CUSTOM_COL_MAP)
        series = cache.load_blob(2, 20)
        assert list(series.tstarts) == [0, 200]
        assert list(series.values) == [3.0, 4.0]

    def test_mdf_drops_data_columns(self):
        """mdf should not contain tstart/tend/value columns."""
        pdf = _make_channel_pdf()
        cache = BasicNarrowTimeSeriesCache(pdf, col_map=DEFAULT_COL_MAP)
        assert "tstart" not in cache.mdf.columns
        assert "tend" not in cache.mdf.columns
        assert "value" not in cache.mdf.columns
        assert "container_id" in cache.mdf.columns
        assert "channel_id" in cache.mdf.columns

    def test_mdf_drops_data_columns_custom_names(self):
        """mdf drops custom-named data columns when col_map matches."""
        pdf = _make_channel_pdf(ts_col="t_start", te_col="t_stop", val_col="val")
        col_map = {
            "cid": "container_id",
            "ch": "channel_id",
            "ts": "t_start",
            "te": "t_stop",
            "val": "val",
        }
        cache = BasicNarrowTimeSeriesCache(pdf, col_map=col_map)
        assert "t_start" not in cache.mdf.columns
        assert "t_stop" not in cache.mdf.columns
        assert "val" not in cache.mdf.columns

    def test_pdf_sorted_correctly(self):
        """pdf should be sorted by (container_id, channel_id, tstart) within each group."""
        pdf = _make_channel_pdf()
        # Scramble order
        pdf = pdf.sample(frac=1, random_state=0).reset_index(drop=True)
        cache = BasicNarrowTimeSeriesCache(pdf, col_map=DEFAULT_COL_MAP)
        # Verify that for each (container_id, channel_id) group, tstarts are sorted
        for (cid, chid), group in cache.pdf.groupby([cache._cid_col, cache._ch_col]):
            ts_vals = list(group[cache._ts_col])
            assert ts_vals == sorted(
                ts_vals
            ), f"tstart not sorted for container_id={cid}, channel_id={chid}: {ts_vals}"


# ---------------------------------------------------------------------------
# TestBasicNarrowSolverUDF
# ---------------------------------------------------------------------------


class TestBasicNarrowSolverUDF:
    """Unit tests for BasicNarrowSolver._solve_udf with col_map."""

    def test_default_config_result_key(self):
        """UDF result DataFrame should have 'container_id' column with default col_map."""
        pdf = _make_channel_pdf()

        class _MockSelection:
            _alias = "mock_result"

            def build(self, cache):
                return _MockSerializable([42.0])

        class _MockSerializable:
            def __init__(self, v):
                self._v = v

            def serialize(self):
                return self._v

        result = BasicNarrowSolver._solve_udf(
            pdf, selections=[_MockSelection()], col_map=DEFAULT_COL_MAP
        )
        assert "container_id" in result.columns
        assert result["container_id"].iloc[0] == pdf["container_id"].iloc[0]

    def test_custom_config_result_key(self):
        """UDF result DataFrame should use col_map cid column name."""
        pdf = _make_channel_pdf(
            cid_col="meas_id", ch_col="sig_id", ts_col="t_start", te_col="t_stop", val_col="val"
        )

        class _MockSelection:
            _alias = "out"

            def build(self, cache):
                return _MockSerializable([1.0])

        class _MockSerializable:
            def __init__(self, v):
                self._v = v

            def serialize(self):
                return self._v

        result = BasicNarrowSolver._solve_udf(
            pdf, selections=[_MockSelection()], col_map=CUSTOM_COL_MAP
        )
        assert "meas_id" in result.columns
        assert "container_id" not in result.columns
        assert result["meas_id"].iloc[0] == 1


# ---------------------------------------------------------------------------
# TestBasicNarrowSolverFilterMethods
# ---------------------------------------------------------------------------


class TestBasicNarrowSolverFilterMethods:
    """Unit tests for BasicNarrowSolver filter stage methods."""

    def test_filter_container_metrics_uses_config_col(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """filter_container_metrics should return a column named per config."""
        solver = BasicNarrowSolver(spark)
        query = basic_narrow_db.query
        result = solver.filter_container_metrics(spark, query, None)
        # Default config → column must be "container_id"
        assert "container_id" in result.columns
        assert result.count() > 0

    def test_filter_container_tags_returns_empty_df(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """BasicNarrowSolver.filter_container_tags always returns an empty DataFrame."""
        solver = BasicNarrowSolver(spark)
        query = basic_narrow_db.query
        result = solver.filter_container_tags(spark, query)
        assert result.count() == 0

    def test_filter_channel_metrics_uses_config_cols(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """filter_channel_metrics result should contain config column names."""
        solver = BasicNarrowSolver(spark)
        query = basic_narrow_db.query
        # build a container_df with the right column name
        container_df = solver.filter_container_metrics(spark, query, None)
        result = solver.filter_channel_metrics(spark, query, container_df)
        assert "container_id" in result.columns
        assert "channel_id" in result.columns


# ---------------------------------------------------------------------------
# TestBasicNarrowSolverEndToEnd
# ---------------------------------------------------------------------------


class TestBasicNarrowSolverEndToEnd:
    """End-to-end tests: run a full query.solve() through BasicNarrowSolver."""

    def test_default_config_solve_produces_results(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """
        Full solve() via query_builder.solve() with default config produces results.
        Uses a TimeSeriesSelector (channel expression) as the selection — this is
        what the query engine's select() accepts.
        """
        solver = BasicNarrowSolver(spark)
        query = basic_narrow_db.query

        # channel() returns a TimeSeriesSelector which has build() — the correct type.
        # Only filter on channel_name; basic_narrow test data has no data_key column.
        ch_expr = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")
        query.select(ch_expr)
        result = query.solve(spark, solver=solver)
        assert result is not None
        assert "container_id" in result.columns
        assert result.count() > 0

    def test_backward_compat_no_config_arg(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """BasicNarrowSolver() without config arg works identically to before."""
        solver = BasicNarrowSolver(spark)
        assert solver.config.container_id_col == "container_id"
        assert solver.config.tstart_col == "tstart"

    def test_config_properties_accessible(self, spark: SparkSession):
        """BasicNarrowSolver exposes column names via self.config properties."""
        cfg = SolverConfig(
            container_id_col="meas_id",
            channel_id_cols=["meas_id", "sig_id"],
            channel_data_mapping={"tstart": "t_start", "tend": "t_stop", "value": "val"},
        )
        solver = BasicNarrowSolver(spark, config=cfg)
        assert solver.config.container_id_col == "meas_id"
        assert solver.config.channel_id_col == "sig_id"
        assert solver.config.tstart_col == "t_start"
        assert solver.config.tend_col == "t_stop"
        assert solver.config.value_col == "val"

    def test_no_redundant_instance_attrs(self, spark: SparkSession):
        """BasicNarrowSolver should NOT have cid_col/ch_col/ts_col/te_col/val_col attributes."""
        solver = BasicNarrowSolver(spark)
        assert not hasattr(solver, "cid_col")
        assert not hasattr(solver, "ch_col")
        assert not hasattr(solver, "ts_col")
        assert not hasattr(solver, "te_col")
        assert not hasattr(solver, "val_col")

    def test_col_map_from_config(self, spark: SparkSession):
        """col_map property on config returns correct mapping."""
        cfg = SolverConfig(
            container_id_col="meas_id",
            channel_id_cols=["meas_id", "sig_id"],
            channel_data_mapping={"tstart": "t_start", "tend": "t_stop", "value": "val"},
        )
        solver = BasicNarrowSolver(spark, config=cfg)
        col_map = solver.config.col_map
        assert col_map == {
            "cid": "meas_id",
            "ch": "sig_id",
            "ts": "t_start",
            "te": "t_stop",
            "val": "val",
        }
