# pylint: disable=missing-function-docstring
"""
Tests for KVSTimeSeriesCache, KeyValueStoreSolver._solve_udf, and
KeyValueStoreSolver's wide-only data model (no container_tags_table).

Covers:
- KVSTimeSeriesCache with default and custom column configs (via col_map)
- KeyValueStoreSolver._solve_udf with col_map
- KeyValueStoreSolver.filter_channel_metrics / solve end-to-end with
  the wide-only data model via the basic_narrow_db fixture
- SolverConfig col_map and property invariants
"""

import pandas as pd
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.solvers.key_value_store_solver import (
    KeyValueStoreSolver,
    KVSTimeSeriesCache,
)
from impulse_query_engine.analyze.query.solvers.solver_config import (
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB
from tests.conftest import basic_narrow_db, spark

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
# TestKVSTimeSeriesCache
# ---------------------------------------------------------------------------


class TestKVSTimeSeriesCache:
    """Unit tests for KVSTimeSeriesCache."""

    def test_default_config_load_blob(self):
        """load_blob works with default column names."""
        pdf = _make_channel_pdf()
        cache = KVSTimeSeriesCache(pdf, col_map=DEFAULT_COL_MAP)
        series = cache.load_blob(1, 10)
        assert list(series.tstarts) == [0, 100]
        assert list(series.values) == [1.0, 2.0]

    def test_custom_config_load_blob(self):
        """load_blob works with custom column names when matching col_map is given."""
        pdf = _make_channel_pdf(
            cid_col="meas_id", ch_col="sig_id", ts_col="t_start", te_col="t_stop", val_col="val"
        )
        cache = KVSTimeSeriesCache(pdf, col_map=CUSTOM_COL_MAP)
        series = cache.load_blob(2, 20)
        assert list(series.tstarts) == [0, 200]
        assert list(series.values) == [3.0, 4.0]

    def test_mdf_drops_data_columns(self):
        """mdf should not contain tstart/tend/value columns."""
        pdf = _make_channel_pdf()
        cache = KVSTimeSeriesCache(pdf, col_map=DEFAULT_COL_MAP)
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
        cache = KVSTimeSeriesCache(pdf, col_map=col_map)
        assert "t_start" not in cache.mdf.columns
        assert "t_stop" not in cache.mdf.columns
        assert "val" not in cache.mdf.columns

    def test_pdf_sorted_correctly(self):
        """pdf should be sorted by (container_id, channel_id, tstart) within each group."""
        pdf = _make_channel_pdf()
        # Scramble order
        pdf = pdf.sample(frac=1, random_state=0).reset_index(drop=True)
        cache = KVSTimeSeriesCache(pdf, col_map=DEFAULT_COL_MAP)
        # Verify that for each (container_id, channel_id) group, tstarts are sorted
        for (cid, chid), group in cache.pdf.groupby([cache._cid_col, cache._ch_col]):
            ts_vals = list(group[cache._ts_col])
            assert ts_vals == sorted(
                ts_vals
            ), f"tstart not sorted for container_id={cid}, channel_id={chid}: {ts_vals}"


# ---------------------------------------------------------------------------
# TestKeyValueStoreSolverUDF
# ---------------------------------------------------------------------------


class TestKeyValueStoreSolverUDF:
    """Unit tests for KeyValueStoreSolver._solve_udf with col_map."""

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

        result = KeyValueStoreSolver._solve_udf(
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

        result = KeyValueStoreSolver._solve_udf(
            pdf, selections=[_MockSelection()], col_map=CUSTOM_COL_MAP
        )
        assert "meas_id" in result.columns
        assert "container_id" not in result.columns
        assert result["meas_id"].iloc[0] == 1


# ---------------------------------------------------------------------------
# TestKeyValueStoreSolverFilterMethods (wide-only data model)
# ---------------------------------------------------------------------------


class TestKeyValueStoreSolverFilterMethodsWideOnly:
    """Filter-stage tests against the wide-only fixture (no container_tags_table)."""

    def test_filter_channel_metrics_uses_config_cols(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """filter_channel_metrics result should contain (container_id, channel_id, selector_ids)."""
        solver = KeyValueStoreSolver(spark)
        query = basic_narrow_db.query.select(
            basic_narrow_db.query.channel(channel_name="Engine RPM")
        )
        tags_df = solver.filter_container_tags(spark, query)
        container_df = solver.filter_container_metrics(spark, query, tags_df)
        selectors = TimeSeriesExpression.collect_selectors(query.selections, uses_alias=False)
        result = solver.filter_channel_metrics(spark, basic_narrow_db, container_df, selectors)
        assert "container_id" in result.columns
        assert "channel_id" in result.columns
        assert "selector_ids" in result.columns


# ---------------------------------------------------------------------------
# TestKeyValueStoreSolverEndToEnd (wide-only data model)
# ---------------------------------------------------------------------------


class TestKeyValueStoreSolverEndToEndWideOnly:
    """End-to-end tests using the wide-only fixture (no container_tags_table)."""

    def test_default_config_solve_produces_results(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """Full solve() with default config produces results."""
        solver = KeyValueStoreSolver(spark)
        query = basic_narrow_db.query

        # channel() returns a TimeSeriesSelector which has build() — the correct type.
        ch_expr = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")
        query.select(ch_expr)
        result = query.solve(spark, solver=solver)
        assert result is not None
        assert "container_id" in result.columns
        assert result.count() > 0

    def test_backward_compat_no_config_arg(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """KeyValueStoreSolver(spark) without a config arg works."""
        solver = KeyValueStoreSolver(spark)
        assert solver.config.container_id_col == "container_id"
        assert solver.config.tstart_col == "tstart"

    def test_no_redundant_instance_attrs(self, spark: SparkSession):
        """KeyValueStoreSolver should NOT have cid_col/ch_col/ts_col/te_col/val_col attributes."""
        solver = KeyValueStoreSolver(spark)
        assert not hasattr(solver, "cid_col")
        assert not hasattr(solver, "ch_col")
        assert not hasattr(solver, "ts_col")
        assert not hasattr(solver, "te_col")
        assert not hasattr(solver, "val_col")

    def test_col_map_always_returns_internal_names(self, spark: SparkSession):
        """col_map always returns the fixed internal-name mapping."""
        cfg = SolverConfig(
            channels=TableConfig(
                column_name_mapping={
                    "meas_id": "container_id",
                    "sig_id": "channel_id",
                    "t_start": "tstart",
                    "t_stop": "tend",
                    "val": "value",
                }
            )
        )
        solver = KeyValueStoreSolver(spark, config=cfg)
        col_map = solver.config.col_map
        assert col_map == {
            "cid": "container_id",
            "ch": "channel_id",
            "ts": "tstart",
            "te": "tend",
            "val": "value",
            "conv": "conversion_factor",
        }

    def test_config_properties_return_internal_names(self, spark: SparkSession):
        """Properties always return fixed internal names regardless of mapping."""
        cfg = SolverConfig(
            channels=TableConfig(
                column_name_mapping={
                    "meas_id": "container_id",
                    "sig_id": "channel_id",
                    "t_start": "tstart",
                    "t_stop": "tend",
                    "val": "value",
                }
            )
        )
        solver = KeyValueStoreSolver(spark, config=cfg)
        assert solver.config.container_id_col == "container_id"
        assert solver.config.channel_id_col == "channel_id"
        assert solver.config.tstart_col == "tstart"
        assert solver.config.tend_col == "tend"
        assert solver.config.value_col == "value"
