# pylint: disable=missing-function-docstring
"""
Tests for TimeSeriesCache, DefaultSolver._solve_udf, and
DefaultSolver's wide-only data model (no container_tags_table).

Covers:
- TimeSeriesCache with default and custom column configs (via col_map)
- DefaultSolver._solve_udf with col_map
- DefaultSolver.filter_channel_metrics / solve end-to-end with
  the wide-only data model via the basic_narrow_db fixture
- SolverConfig col_map and property invariants
"""

import pandas as pd
import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.solvers.default_solver import (
    DefaultSolver,
    TimeSeriesCache,
)
from impulse_query_engine.analyze.query.solvers.solver_config import (
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
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
# TestTimeSeriesCache
# ---------------------------------------------------------------------------


class TestTimeSeriesCache:
    """Unit tests for TimeSeriesCache."""

    def test_default_config_load_blob(self):
        """load_blob works with default column names."""
        pdf = _make_channel_pdf()
        cache = TimeSeriesCache(pdf, col_map=DEFAULT_COL_MAP)
        series = cache.load_blob(1, 10)
        assert list(series.tstarts) == [0, 100]
        assert list(series.values) == [1.0, 2.0]

    def test_custom_config_load_blob(self):
        """load_blob works with custom column names when matching col_map is given."""
        pdf = _make_channel_pdf(
            cid_col="meas_id", ch_col="sig_id", ts_col="t_start", te_col="t_stop", val_col="val"
        )
        cache = TimeSeriesCache(pdf, col_map=CUSTOM_COL_MAP)
        series = cache.load_blob(2, 20)
        assert list(series.tstarts) == [0, 200]
        assert list(series.values) == [3.0, 4.0]

    def test_mdf_drops_data_columns(self):
        """mdf should not contain tstart/tend/value columns."""
        pdf = _make_channel_pdf()
        cache = TimeSeriesCache(pdf, col_map=DEFAULT_COL_MAP)
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
        cache = TimeSeriesCache(pdf, col_map=col_map)
        assert "t_start" not in cache.mdf.columns
        assert "t_stop" not in cache.mdf.columns
        assert "val" not in cache.mdf.columns

    def test_mdf_keeps_first_row_per_channel(self):
        """mdf keeps the first occurrence of per-channel metadata."""
        pdf = _make_channel_pdf()
        pdf["quality"] = ["good", "bad", "ok", "worse"]
        cache = TimeSeriesCache(pdf, col_map=DEFAULT_COL_MAP)
        assert len(cache.mdf) == 2
        quality = cache.mdf.set_index("channel_id")["quality"]
        assert quality[10] == "good"
        assert quality[20] == "ok"

    def test_load_blob_missing_channel_returns_empty(self):
        """Unknown (container_id, channel_id) yields an empty series."""
        cache = TimeSeriesCache(_make_channel_pdf(), col_map=DEFAULT_COL_MAP)
        assert len(cache.load_blob(1, 999)) == 0
        assert len(cache.load_blob(999, 10)) == 0

    def test_load_blob_applies_conversion_only_for_alias(self):
        """The per-channel factor multiplies values for aliased reads only."""
        pdf = _make_channel_pdf()
        pdf["conversion_factor"] = [2.0, 2.0, 0.5, 0.5]
        col_map = {**DEFAULT_COL_MAP, "conv": "conversion_factor"}
        cache = TimeSeriesCache(pdf, col_map=col_map)
        assert list(cache.load_blob(1, 10, uses_alias=True).values) == [2.0, 4.0]
        assert list(cache.load_blob(1, 10, uses_alias=False).values) == [1.0, 2.0]
        assert list(cache.load_blob(2, 20, uses_alias=True).values) == [1.5, 2.0]

    def test_load_blob_nan_conversion_factor_is_noop(self):
        """A null factor (no conversion resolved) leaves values unchanged."""
        pdf = _make_channel_pdf()
        pdf["conversion_factor"] = float("nan")
        col_map = {**DEFAULT_COL_MAP, "conv": "conversion_factor"}
        cache = TimeSeriesCache(pdf, col_map=col_map)
        assert list(cache.load_blob(1, 10, uses_alias=True).values) == [1.0, 2.0]

    def test_load_blob_preserves_input_order_for_duplicate_timestamps(self):
        """The (cid, ch, tstart) sort must stay stable for equal timestamps."""
        pdf = pd.DataFrame(
            {
                "container_id": [1, 1, 1],
                "channel_id": [10, 10, 10],
                "tstart": [0, 0, 100],
                "tend": [100, 100, 200],
                "value": [1.0, 2.0, 3.0],
            }
        )
        cache = TimeSeriesCache(pdf, col_map=DEFAULT_COL_MAP)
        assert list(cache.load_blob(1, 10).values) == [1.0, 2.0, 3.0]

    def test_cache_handles_non_default_index(self):
        """Duplicate / non-monotonic input index must not confuse mdf or load_blob."""
        pdf = _make_channel_pdf()
        pdf.index = [7, 3, 7, 5]
        cache = TimeSeriesCache(pdf, col_map=DEFAULT_COL_MAP)
        assert len(cache.mdf) == 2
        assert list(cache.load_blob(1, 10).values) == [1.0, 2.0]
        assert list(cache.load_blob(2, 20).values) == [3.0, 4.0]

    def test_pdf_sorted_correctly(self):
        """pdf should be sorted by (container_id, channel_id, tstart) within each group."""
        pdf = _make_channel_pdf()
        # Scramble order
        pdf = pdf.sample(frac=1, random_state=0).reset_index(drop=True)
        cache = TimeSeriesCache(pdf, col_map=DEFAULT_COL_MAP)
        # Verify that for each (container_id, channel_id) group, tstarts are sorted
        for (cid, chid), group in cache.pdf.groupby([cache._cid_col, cache._ch_col]):
            ts_vals = list(group[cache._ts_col])
            assert ts_vals == sorted(
                ts_vals
            ), f"tstart not sorted for container_id={cid}, channel_id={chid}: {ts_vals}"


# ---------------------------------------------------------------------------
# TestDefaultSolverUDF
# ---------------------------------------------------------------------------


class TestDefaultSolverUDF:
    """Unit tests for DefaultSolver._solve_udf with col_map."""

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

        result = DefaultSolver._solve_udf(
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

        result = DefaultSolver._solve_udf(
            pdf, selections=[_MockSelection()], col_map=CUSTOM_COL_MAP
        )
        assert "meas_id" in result.columns
        assert "container_id" not in result.columns
        assert result["meas_id"].iloc[0] == 1


# ---------------------------------------------------------------------------
# TestDefaultSolverFilterMethods (wide-only data model)
# ---------------------------------------------------------------------------


class TestDefaultSolverFilterMethodsWideOnly:
    """Filter-stage tests against the wide-only fixture (no container_tags_table)."""

    def test_filter_channel_metrics_uses_config_cols(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """filter_channel_metrics result should contain (container_id, channel_id, selector_ids)."""
        solver = DefaultSolver(spark)
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
# TestDefaultSolverEndToEnd (wide-only data model)
# ---------------------------------------------------------------------------


class TestDefaultSolverEndToEndWideOnly:
    """End-to-end tests using the wide-only fixture (no container_tags_table)."""

    def test_default_config_solve_produces_results(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """Full solve() with default config produces results."""
        solver = DefaultSolver(spark)
        query = basic_narrow_db.query

        # channel() returns a TimeSeriesSelector which has build() — the correct type.
        ch_expr = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")
        query.select(ch_expr)
        result = query.solve(spark, solver=solver)
        assert result is not None
        assert "container_id" in result.columns
        assert result.count() > 0

    def test_solve_ignores_extra_channels_columns(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """Extra physical columns on the channels table don't change solve() results.

        Guards the UDF-input projection in ``DefaultSolver.solve``: only the
        framework data columns may reach the grouped-map UDF, and results must
        be identical with or without extra columns on the silver table.
        """
        tables = {
            name: (df.withColumn("extra_col", F.lit("x")) if name == "channels" else df)
            for name, df in basic_narrow_db.config.debug_tables.items()
        }
        db_extra = MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=basic_narrow_db.ws)

        def _means(db):
            query = db.query
            result = query.select(
                query.channel(channel_name="Vehicle Speed Sensor").mean().alias("m")
            ).solve(spark, solver=DefaultSolver(spark))
            return {row.container_id: row.m for row in result.collect()}

        base = _means(basic_narrow_db)
        extra = _means(db_extra)
        assert base.keys() == extra.keys()
        for container_id in base:
            assert extra[container_id] == pytest.approx(base[container_id])
        assert any(m is not None and m != 0 for m in base.values()), base

    def test_solve_raw_point_data_with_extra_columns(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """is_raw_data=True works end-to-end with extra physical columns.

        Also guards the ordering of the UDF-input projection in
        ``DefaultSolver.solve``: it must run *after* the interval encoder,
        which consumes ``timestamp`` / ``is_plausible``.
        """
        tables = dict(basic_narrow_db.config.debug_tables)
        tables["channels"] = (
            tables["channels"]
            .select("container_id", "channel_id", F.col("tstart").alias("timestamp"), "value")
            .withColumn("extra_col", F.lit("x"))
            .withColumn("is_plausible", F.lit(True))
        )
        db_raw = MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=basic_narrow_db.ws)

        def _means(solver):
            query = db_raw.query
            result = query.select(
                query.channel(channel_name="Vehicle Speed Sensor").mean().alias("m")
            ).solve(spark, solver=solver)
            return {row.container_id: row.m for row in result.collect()}

        raw = _means(DefaultSolver(spark, is_raw_data=True))
        assert any(m is not None and m != 0 for m in raw.values()), raw

        # All rows are plausible, so dropping implausible rows must not
        # change the results (pins the is_plausible column surviving until
        # the encoder's filter and being projected away afterwards).
        dropped = _means(DefaultSolver(spark, is_raw_data=True, drop_implausible_data=True))
        assert raw.keys() == dropped.keys()
        for container_id in raw:
            assert dropped[container_id] == pytest.approx(raw[container_id])

    def test_solve_raw_point_data_with_mapped_timestamp_column(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """RAW mode reaches the timestamp column through the config vocabulary.

        The physical channels table carries ``ts_raw`` instead of
        ``timestamp``; the per-table ``column_name_mapping`` renames it to
        the internal name the IntervalEncoder retrieves from the
        SolverConfig (``timestamp_col``).
        """
        tables = dict(basic_narrow_db.config.debug_tables)
        tables["channels"] = tables["channels"].select(
            "container_id", "channel_id", F.col("tstart").alias("ts_raw"), "value"
        )
        db_raw = MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=basic_narrow_db.ws)

        cfg = SolverConfig(channels=TableConfig(column_name_mapping={"ts_raw": "timestamp"}))
        solver = DefaultSolver(spark, config=cfg, is_raw_data=True)

        query = db_raw.query
        result = query.select(
            query.channel(channel_name="Vehicle Speed Sensor").mean().alias("m")
        ).solve(spark, solver=solver)
        means = {row.container_id: row.m for row in result.collect()}
        assert any(m is not None and m != 0 for m in means.values()), means

    def test_backward_compat_no_config_arg(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """DefaultSolver(spark) without a config arg works."""
        solver = DefaultSolver(spark)
        assert solver.config.container_id_col == "container_id"
        assert solver.config.tstart_col == "tstart"

    def test_no_redundant_instance_attrs(self, spark: SparkSession):
        """DefaultSolver should NOT have cid_col/ch_col/ts_col/te_col/val_col attributes."""
        solver = DefaultSolver(spark)
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
        solver = DefaultSolver(spark, config=cfg)
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
        solver = DefaultSolver(spark, config=cfg)
        assert solver.config.container_id_col == "container_id"
        assert solver.config.channel_id_col == "channel_id"
        assert solver.config.tstart_col == "tstart"
        assert solver.config.tend_col == "tend"
        assert solver.config.value_col == "value"
