# pylint: disable=missing-function-docstring
"""
Tests verifying that SolverConfig column-name mappings are actually used
by DeltaSolver when processing DataFrames with non-default column names.

DeltaSolver reads five silver tables and applies `column_name_mapping`
to each one when reading.  These tests build DataFrames with custom
physical column names and verify the solver renames them to the internal
names so that the rest of the pipeline succeeds:

- container_tags  (narrow EAV)
- container_metrics  (wide)
- channel_tags  (narrow EAV at channel level)
- channel_metrics  (wide)
- channels  (RLE time-series)
"""

from unittest.mock import create_autospec

import pandas as pd
import pyspark.sql.types as T
import pytest
from databricks.sdk import WorkspaceClient
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.query.solvers.delta_solver import DeltaSolver
from impulse_query_engine.analyze.query.solvers.solver_config import (
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import spark

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _container_tags_df(spark, rows, cid_col="container_id", key_col="key", val_col="value"):
    """Narrow/EAV container_tags table with configurable column names."""
    pdf = pd.DataFrame(rows, columns=[cid_col, key_col, val_col])
    schema = T.StructType(
        [
            T.StructField(cid_col, T.LongType()),
            T.StructField(key_col, T.StringType()),
            T.StructField(val_col, T.StringType()),
        ]
    )
    return spark.createDataFrame(pdf, schema=schema)


def _container_metrics_df(
    spark, rows, cid_col="container_id", start_col="start_ts", stop_col="stop_ts"
):
    """Wide container_metrics table with configurable column names."""
    pdf = pd.DataFrame(rows, columns=[cid_col, start_col, stop_col])
    schema = T.StructType(
        [
            T.StructField(cid_col, T.LongType()),
            T.StructField(start_col, T.LongType()),
            T.StructField(stop_col, T.LongType()),
        ]
    )
    return spark.createDataFrame(pdf, schema=schema)


def _channel_tags_df(
    spark,
    rows,
    cid_col="container_id",
    ch_col="channel_id",
    key_col="key",
    val_col="value",
):
    """Narrow/EAV channel_tags table with configurable column names."""
    pdf = pd.DataFrame(rows, columns=[cid_col, ch_col, key_col, val_col])
    schema = T.StructType(
        [
            T.StructField(cid_col, T.LongType()),
            T.StructField(ch_col, T.IntegerType()),
            T.StructField(key_col, T.StringType()),
            T.StructField(val_col, T.StringType()),
        ]
    )
    return spark.createDataFrame(pdf, schema=schema)


def _channel_metrics_df(spark, rows, cid_col="container_id", ch_col="channel_id"):
    """Wide channel_metrics table with configurable container_id / channel_id columns."""
    pdf = pd.DataFrame(rows, columns=[cid_col, ch_col, "sample_count"])
    schema = T.StructType(
        [
            T.StructField(cid_col, T.LongType()),
            T.StructField(ch_col, T.IntegerType()),
            T.StructField("sample_count", T.IntegerType()),
        ]
    )
    return spark.createDataFrame(pdf, schema=schema)


def _channels_df(
    spark,
    rows,
    cid_col="container_id",
    ch_col="channel_id",
    ts_col="tstart",
    te_col="tend",
    val_col="value",
):
    """RLE channels (time-series) table with configurable column names."""
    pdf = pd.DataFrame(rows, columns=[cid_col, ch_col, ts_col, te_col, val_col])
    schema = T.StructType(
        [
            T.StructField(cid_col, T.LongType()),
            T.StructField(ch_col, T.IntegerType()),
            T.StructField(ts_col, T.LongType()),
            T.StructField(te_col, T.LongType()),
            T.StructField(val_col, T.DoubleType()),
        ]
    )
    return spark.createDataFrame(pdf, schema=schema)


def _make_db(tables: dict) -> MeasurementDB:
    cfg = MeasurementDBConfig.for_debug(tables)
    return MeasurementDB(cfg, ws=create_autospec(WorkspaceClient))


# ---------------------------------------------------------------------------
# Shared test data (default-name rows; helpers attach the custom names)
# ---------------------------------------------------------------------------

# (container_id, key, value)
_CONTAINER_TAGS_ROWS = [
    (1, "brand", "Seat"),
    (1, "model", "Leon"),
    (2, "brand", "Seat"),
    (2, "model", "Ibiza"),
    (3, "brand", "Seat"),
    (3, "model", "Ateca"),
]

# (container_id, start_ts, stop_ts)
_CONTAINER_METRICS_ROWS = [
    (1, 1000, 3000),
    (2, 1000, 3000),
    (3, 1000, 3000),
]

# (container_id, channel_id, key, value)
_CHANNEL_TAGS_ROWS = [
    (1, 1, "channel_name", "Engine RPM"),
    (1, 2, "channel_name", "Vehicle Speed"),
    (2, 1, "channel_name", "Engine RPM"),
    (3, 1, "channel_name", "Engine RPM"),
]

# (container_id, channel_id, sample_count)
_CHANNEL_METRICS_ROWS = [
    (1, 1, 100),
    (1, 2, 100),
    (2, 1, 100),
    (3, 1, 100),
]

# (container_id, channel_id, tstart, tend, value)
_CHANNELS_ROWS = [
    (1, 1, 1000, 2000, 1500.0),
    (1, 1, 2000, 3000, 1600.0),
    (1, 2, 1000, 2000, 50.0),
    (2, 1, 1000, 2000, 1400.0),
    (3, 1, 1000, 2000, 1800.0),
]


def _default_tables(spark) -> dict:
    """Default-named tables, no renames required to read them."""
    return {
        "container_tags": _container_tags_df(spark, _CONTAINER_TAGS_ROWS),
        "container_metrics": _container_metrics_df(spark, _CONTAINER_METRICS_ROWS),
        "channel_tags": _channel_tags_df(spark, _CHANNEL_TAGS_ROWS),
        "channel_metrics": _channel_metrics_df(spark, _CHANNEL_METRICS_ROWS),
        "channels": _channels_df(spark, _CHANNELS_ROWS),
    }


# ===================================================================
# TEST GROUP 1: container_tags column rename
# ===================================================================


class TestDeltaSolverContainerTagsMapping:
    """Verify filter_container_tags works when container_tags physical columns are renamed."""

    @pytest.fixture
    def db_custom_tags(self, spark):
        tables = _default_tables(spark)
        tables["container_tags"] = _container_tags_df(
            spark,
            _CONTAINER_TAGS_ROWS,
            cid_col="meas_id",
            key_col="element_id",
            val_col="attr_val",
        )
        return _make_db(tables)

    def test_no_filter_returns_all_containers(self, spark, db_custom_tags):
        cfg = SolverConfig(
            container_tags=TableConfig(
                column_name_mapping={
                    "meas_id": "container_id",
                    "element_id": "key",
                    "attr_val": "value",
                },
            ),
        )
        solver = DeltaSolver(spark, config=cfg)
        query = db_custom_tags.query
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_tag_filter_uses_renamed_columns(self, spark, db_custom_tags):
        cfg = SolverConfig(
            container_tags=TableConfig(
                column_name_mapping={
                    "meas_id": "container_id",
                    "element_id": "key",
                    "attr_val": "value",
                },
            ),
        )
        solver = DeltaSolver(spark, config=cfg)
        query = db_custom_tags.query
        query.where(TagSelector("model") == "Ateca")
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {3}

    def test_default_config_fails_on_renamed_data(self, spark, db_custom_tags):
        solver = DeltaSolver(spark)
        query = db_custom_tags.query
        with pytest.raises(AnalysisException):
            solver.filter_container_tags(spark, query).collect()


# ===================================================================
# TEST GROUP 2: container_metrics column rename
# ===================================================================


class TestDeltaSolverContainerMetricsMapping:
    """Verify filter_container_metrics works when container_metrics columns are renamed."""

    @pytest.fixture
    def db_custom_metrics(self, spark):
        tables = _default_tables(spark)
        tables["container_metrics"] = _container_metrics_df(
            spark, _CONTAINER_METRICS_ROWS, cid_col="run_id"
        )
        return _make_db(tables)

    def test_filter_container_metrics_joins_on_internal_name(self, spark, db_custom_metrics):
        cfg = SolverConfig(
            container_metrics=TableConfig(column_name_mapping={"run_id": "container_id"}),
        )
        solver = DeltaSolver(spark, config=cfg)
        query = db_custom_metrics.query
        tags_df = solver.filter_container_tags(spark, query)
        result = solver.filter_container_metrics(spark, query, tags_df)
        assert "container_id" in result.columns
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_default_config_fails_on_renamed_metrics(self, spark, db_custom_metrics):
        solver = DeltaSolver(spark)
        query = db_custom_metrics.query
        tags_df = solver.filter_container_tags(spark, query)
        with pytest.raises(AnalysisException):
            solver.filter_container_metrics(spark, query, tags_df).collect()


# ===================================================================
# TEST GROUP 3: channel_tags column rename
# ===================================================================


class TestDeltaSolverChannelTagsMapping:
    """Verify filter_channel_tags works when channel_tags columns are renamed."""

    @pytest.fixture
    def db_custom_channel_tags(self, spark):
        tables = _default_tables(spark)
        tables["channel_tags"] = _channel_tags_df(
            spark,
            _CHANNEL_TAGS_ROWS,
            cid_col="run_id",
            ch_col="signal_id",
            key_col="element_id",
            val_col="attr_val",
        )
        return _make_db(tables)

    def test_filter_channel_tags_uses_renames(self, spark, db_custom_channel_tags):
        cfg = SolverConfig(
            channel_tags=TableConfig(
                column_name_mapping={
                    "run_id": "container_id",
                    "signal_id": "channel_id",
                    "element_id": "key",
                    "attr_val": "value",
                },
            ),
        )
        solver = DeltaSolver(spark, config=cfg)
        query = db_custom_channel_tags.query
        query.select(query.channel(channel_name="Engine RPM"))
        tags_df = solver.filter_container_tags(spark, query)
        container_df = solver.filter_container_metrics(spark, query, tags_df)
        selectors = query._collect_time_series_selectors(uses_alias=False)
        result = solver.filter_channel_tags(spark, db_custom_channel_tags, container_df, selectors)
        assert {"container_id", "channel_id", "selector_id"}.issubset(set(result.columns))
        # Three channels named "Engine RPM" across containers 1, 2, 3
        pairs = {(row.container_id, row.channel_id) for row in result.collect()}
        assert pairs == {(1, 1), (2, 1), (3, 1)}

    def test_default_config_fails_on_renamed_channel_tags(self, spark, db_custom_channel_tags):
        solver = DeltaSolver(spark)
        query = db_custom_channel_tags.query
        query.select(query.channel(channel_name="Engine RPM"))
        tags_df = solver.filter_container_tags(spark, query)
        container_df = solver.filter_container_metrics(spark, query, tags_df)
        selectors = query._collect_time_series_selectors(uses_alias=False)
        with pytest.raises(AnalysisException):
            solver.filter_channel_tags(
                spark, db_custom_channel_tags, container_df, selectors
            ).collect()


# ===================================================================
# TEST GROUP 4: channel_metrics column rename
# ===================================================================


class TestDeltaSolverChannelMetricsMapping:
    """Verify filter_channel_metrics works when channel_metrics columns are renamed."""

    @pytest.fixture
    def db_custom_channel_metrics(self, spark):
        tables = _default_tables(spark)
        tables["channel_metrics"] = _channel_metrics_df(
            spark, _CHANNEL_METRICS_ROWS, cid_col="run_id", ch_col="signal_id"
        )
        return _make_db(tables)

    def test_filter_channel_metrics_uses_renames(self, spark, db_custom_channel_metrics):
        cfg = SolverConfig(
            channel_metrics=TableConfig(
                column_name_mapping={"run_id": "container_id", "signal_id": "channel_id"},
            ),
        )
        solver = DeltaSolver(spark, config=cfg)
        query = db_custom_channel_metrics.query
        query.select(query.channel(channel_name="Engine RPM"))
        tags_df = solver.filter_container_tags(spark, query)
        container_df = solver.filter_container_metrics(spark, query, tags_df)
        selectors = query._collect_time_series_selectors(uses_alias=False)
        ch_tags_df = solver.filter_channel_tags(
            spark, db_custom_channel_metrics, container_df, selectors
        )
        result = solver.filter_channel_metrics(
            spark, db_custom_channel_metrics, ch_tags_df, selectors
        )
        assert {"container_id", "channel_id", "selector_ids"}.issubset(set(result.columns))
        pairs = {(row.container_id, row.channel_id) for row in result.collect()}
        assert pairs == {(1, 1), (2, 1), (3, 1)}


# ===================================================================
# TEST GROUP 5: channels (RLE) column rename + end-to-end solve
# ===================================================================


class TestDeltaSolverChannelsMapping:
    """Verify solve() works when channels (RLE) physical columns are renamed."""

    @pytest.fixture
    def db_custom_channels(self, spark):
        tables = _default_tables(spark)
        tables["channels"] = _channels_df(
            spark,
            _CHANNELS_ROWS,
            cid_col="run_id",
            ch_col="signal_id",
            ts_col="t_begin",
            te_col="t_end",
            val_col="signal_val",
        )
        return _make_db(tables)

    def test_end_to_end_solve_with_renamed_channels(self, spark, db_custom_channels):
        cfg = SolverConfig(
            channels=TableConfig(
                column_name_mapping={
                    "run_id": "container_id",
                    "signal_id": "channel_id",
                    "t_begin": "tstart",
                    "t_end": "tend",
                    "signal_val": "value",
                },
            ),
        )
        solver = DeltaSolver(spark, config=cfg)
        query = db_custom_channels.query
        eng_rpm = query.channel(channel_name="Engine RPM")
        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark, solver=solver)
        rows = {row.container_id: row.rpm_mean for row in result.collect()}
        assert set(rows.keys()) == {1, 2, 3}
        # Container 1 has values 1500.0 and 1600.0 over equal-length intervals → mean 1550
        assert rows[1] == pytest.approx(1550.0)
        # Containers 2 and 3 have a single sample each
        assert rows[2] == pytest.approx(1400.0)
        assert rows[3] == pytest.approx(1800.0)


# ===================================================================
# TEST GROUP 6: every table renamed simultaneously (most realistic)
# ===================================================================


class TestDeltaSolverFullyCustomMapping:
    """Every silver table uses non-default physical column names."""

    @pytest.fixture
    def db_fully_custom(self, spark):
        return _make_db(
            {
                "container_tags": _container_tags_df(
                    spark,
                    _CONTAINER_TAGS_ROWS,
                    cid_col="meas_id",
                    key_col="element_id",
                    val_col="attr_val",
                ),
                "container_metrics": _container_metrics_df(
                    spark, _CONTAINER_METRICS_ROWS, cid_col="meas_id"
                ),
                "channel_tags": _channel_tags_df(
                    spark,
                    _CHANNEL_TAGS_ROWS,
                    cid_col="meas_id",
                    ch_col="signal_id",
                    key_col="element_id",
                    val_col="attr_val",
                ),
                "channel_metrics": _channel_metrics_df(
                    spark, _CHANNEL_METRICS_ROWS, cid_col="meas_id", ch_col="signal_id"
                ),
                "channels": _channels_df(
                    spark,
                    _CHANNELS_ROWS,
                    cid_col="meas_id",
                    ch_col="signal_id",
                    ts_col="t_begin",
                    te_col="t_end",
                    val_col="signal_val",
                ),
            }
        )

    @staticmethod
    def _cfg() -> SolverConfig:
        return SolverConfig(
            container_tags=TableConfig(
                column_name_mapping={
                    "meas_id": "container_id",
                    "element_id": "key",
                    "attr_val": "value",
                },
            ),
            container_metrics=TableConfig(column_name_mapping={"meas_id": "container_id"}),
            channel_tags=TableConfig(
                column_name_mapping={
                    "meas_id": "container_id",
                    "signal_id": "channel_id",
                    "element_id": "key",
                    "attr_val": "value",
                },
            ),
            channel_metrics=TableConfig(
                column_name_mapping={"meas_id": "container_id", "signal_id": "channel_id"},
            ),
            channels=TableConfig(
                column_name_mapping={
                    "meas_id": "container_id",
                    "signal_id": "channel_id",
                    "t_begin": "tstart",
                    "t_end": "tend",
                    "signal_val": "value",
                },
            ),
        )

    def test_end_to_end_solve_fully_custom(self, spark, db_fully_custom):
        solver = DeltaSolver(spark, config=self._cfg())
        query = db_fully_custom.query
        query.where(TagSelector("model") == "Ateca")
        eng_rpm = query.channel(channel_name="Engine RPM")
        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark, solver=solver)
        rows = {row.container_id: row.rpm_mean for row in result.collect()}
        assert set(rows.keys()) == {3}
        assert rows[3] == pytest.approx(1800.0)


# ===================================================================
# TEST GROUP 7: no config / default behaviour preserved
# ===================================================================


class TestDeltaSolverDefaultConfig:
    """When SolverConfig is None or has empty mappings, default column names still work."""

    @pytest.fixture
    def db_default(self, spark):
        return _make_db(_default_tables(spark))

    def test_solver_without_config_works_with_default_names(
        self, spark: SparkSession, db_default: MeasurementDB
    ):
        solver = DeltaSolver(spark)
        query = db_default.query
        eng_rpm = query.channel(channel_name="Engine RPM")
        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark, solver=solver)
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_solver_with_empty_config_works_with_default_names(
        self, spark: SparkSession, db_default: MeasurementDB
    ):
        solver = DeltaSolver(spark, config=SolverConfig())
        query = db_default.query
        eng_rpm = query.channel(channel_name="Engine RPM")
        result = query.select(eng_rpm.mean().alias("rpm_mean")).solve(spark, solver=solver)
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2, 3}
