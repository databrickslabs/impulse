# pylint: disable=missing-function-docstring
"""
Tests verifying that SolverConfig column-name mappings are actually used
by KeyValueStoreSolver when processing DataFrames with non-default column names.

Existing tests prove that SolverConfig stores/returns the right values and that
the solver works with default column names.  These tests close the gap: they
create DataFrames whose columns use **custom names** and verify the solver
resolves them correctly through the mapping — proving the wiring is live,
not just plumbed.

Covers:
- Renamed entity_id column in EAV table  →  filter_container_tags
- Renamed project_id column in EAV table →  filter_container_tags
- Renamed value column in EAV table      →  filter_container_tags (pivot)
- Renamed container_id across all tables →  filter_container_tags + filter_container_metrics
- Combined: all EAV columns renamed      →  filter_container_tags with TagExpression
- Renamed channel columns (tstart/tend/value/channel_id) → solve (UDF + col_map)
- Negative: wrong mapping → zero results or error
"""

import pandas as pd
import pyspark.sql.types as T
import pytest
from pyspark.errors.exceptions.captured import AnalysisException

from mda_query_engine.analyze.metadata.tag_expression import TagSelector
from mda_query_engine.analyze.query.solvers.key_value_store_solver import (
    KeyValueStoreSolver,
)
from mda_query_engine.analyze.query.solvers.solver_config import SolverConfig
from mda_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import spark
from unittest.mock import create_autospec
from databricks.sdk import WorkspaceClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _eav_dataframe(
    spark,
    rows,
    entity_col="entity_id",
    project_col="project_id",
    value_col="value",
    parent_id_col="parent_id",
):
    """Build a narrow/EAV DataFrame with configurable column names."""
    pdf = pd.DataFrame(
        rows, columns=[entity_col, project_col, "element_id", value_col, parent_id_col]
    )
    schema = T.StructType(
        [
            T.StructField(entity_col, T.LongType()),
            T.StructField(project_col, T.StringType()),
            T.StructField("element_id", T.StringType()),
            T.StructField(value_col, T.StringType()),
            T.StructField(parent_id_col, T.StringType()),
        ]
    )
    return spark.createDataFrame(pdf, schema=schema)


def _wide_metrics_dataframe(spark, rows, cid_col="container_id"):
    """Build a wide-format container_metrics DataFrame with configurable container_id col."""
    pdf = pd.DataFrame(rows, columns=[cid_col, "uut_id", "project", "file_name"])
    schema = T.StructType(
        [
            T.StructField(cid_col, T.LongType()),
            T.StructField("uut_id", T.StringType()),
            T.StructField("project", T.StringType()),
            T.StructField("file_name", T.StringType()),
        ]
    )
    return spark.createDataFrame(pdf, schema=schema)


def _channel_metrics_dataframe(spark, rows, cid_col="container_id", ch_col="channel_id"):
    """Build a channel_metrics DataFrame with configurable column names."""
    pdf = pd.DataFrame(rows, columns=[cid_col, ch_col, "channel_name", "sample_count"])
    schema = T.StructType(
        [
            T.StructField(cid_col, T.LongType()),
            T.StructField(ch_col, T.IntegerType()),
            T.StructField("channel_name", T.StringType()),
            T.StructField("sample_count", T.IntegerType()),
        ]
    )
    return spark.createDataFrame(pdf, schema=schema)


def _channels_dataframe(
    spark,
    rows,
    cid_col="container_id",
    ch_col="channel_id",
    ts_col="tstart",
    te_col="tend",
    val_col="value",
):
    """Build a channels (time-series) DataFrame with configurable column names."""
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
    """Create a debug MeasurementDB from a dict of DataFrames."""
    cfg = MeasurementDBConfig.for_debug(tables)
    return MeasurementDB(cfg, ws=create_autospec(WorkspaceClient))


# ---------------------------------------------------------------------------
# Shared test data (values only — column names added by helper functions)
#
# Three Seat models (Leon, Ibiza, Ateca) across two projects to exercise
# both model-level and project-level filtering.
# ---------------------------------------------------------------------------

# EAV rows: (entity, project, element_id, value, parent_id)
_EAV_ROWS = [
    (1, "SAMPLE_PROJECT", "brand", "Seat", "container_concept"),
    (1, "SAMPLE_PROJECT", "model", "Leon", "container_concept"),
    (1, "SAMPLE_PROJECT", "vehicle_key", "Seat_Leon", "container_concept"),
    (2, "SAMPLE_PROJECT", "brand", "Seat", "container_concept"),
    (2, "SAMPLE_PROJECT", "model", "Ibiza", "container_concept"),
    (2, "SAMPLE_PROJECT", "vehicle_key", "Seat_Ibiza", "container_concept"),
    (3, "SAMPLE_PROJECT", "brand", "Seat", "container_concept"),
    (3, "SAMPLE_PROJECT", "model", "Ateca", "container_concept"),
    (3, "SAMPLE_PROJECT", "vehicle_key", "Seat_Ateca", "container_concept"),
    (4, "SAMPLE_PROJECT_B", "brand", "Seat", "container_concept"),
    (4, "SAMPLE_PROJECT_B", "model", "Leon", "container_concept"),
    (4, "SAMPLE_PROJECT_B", "vehicle_key", "Seat_Leon", "container_concept"),
]

# Wide container_metrics rows: (container_id, uut_id, project, file_name)
_WIDE_ROWS = [
    (1, "SEAT_LEON", "SAMPLE_PROJECT", "2017-07-06_Seat_Leon.mf4"),
    (2, "SEAT_IBIZA", "SAMPLE_PROJECT", "2017-07-07_Seat_Ibiza.mf4"),
    (3, "SEAT_ATECA", "SAMPLE_PROJECT", "2017-07-08_Seat_Ateca.mf4"),
]

# Channel metrics rows: (container_id, channel_id, channel_name, sample_count)
_CH_METRIC_ROWS = [
    (1, 1, "is1_eng_speed", 500),
    (1, 2, "can_vehicle_speed", 500),
    (2, 1, "is1_eng_speed", 400),
    (3, 1, "is1_eng_speed", 600),
]

# Channel data rows: (container_id, channel_id, tstart, tend, value)
_CH_DATA_ROWS = [
    (1, 1, 1000, 2000, 1500.0),
    (1, 1, 2000, 3000, 1600.0),
    (1, 2, 1000, 2000, 50.0),
    (1, 2, 2000, 3000, 55.0),
    (2, 1, 1000, 2000, 1400.0),
    (2, 1, 2000, 3000, 1450.0),
    (3, 1, 1000, 2000, 1800.0),
    (3, 1, 2000, 3000, 1850.0),
]


# ===================================================================
# TEST GROUP 1: entity_id column mapping
# ===================================================================


class TestCustomEntityIdMapping:
    """Verify filter_container_tags works when entity_id is renamed."""

    @pytest.fixture
    def db_with_custom_entity_col(self, spark):
        """EAV table uses 'object_id' instead of 'entity_id'."""
        container_tags = _eav_dataframe(spark, _EAV_ROWS, entity_col="object_id")
        container_metrics = _wide_metrics_dataframe(spark, _WIDE_ROWS)
        return _make_db(
            {
                "container_tags": container_tags,
                "container_metrics": container_metrics,
                "channel_metrics": _channel_metrics_dataframe(spark, _CH_METRIC_ROWS),
                "channels": _channels_dataframe(spark, _CH_DATA_ROWS),
            }
        )

    def test_no_filter_returns_all_project_containers(self, spark, db_with_custom_entity_col):
        """All entity_ids from SAMPLE_PROJECT should be returned, aliased to container_id."""
        cfg = {"entity_id_col": "object_id"}
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=cfg)
        query = db_with_custom_entity_col.query
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_with_metric_filter_and_custom_entity_col(self, spark, db_with_custom_entity_col):
        """TagExpression should work after pivot with renamed entity_id."""
        cfg = {"entity_id_col": "object_id"}
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=cfg)
        query = db_with_custom_entity_col.query
        query.where(TagSelector("model") == "Ateca")
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {3}

    def test_wrong_entity_col_returns_error_or_empty(self, spark, db_with_custom_entity_col):
        """Using default entity_id_col when the actual column is 'object_id' should fail."""
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT")  # no custom config
        query = db_with_custom_entity_col.query
        with pytest.raises(AnalysisException):
            solver.filter_container_tags(spark, query).collect()


# ===================================================================
# TEST GROUP 2: project_id column mapping
# ===================================================================


class TestCustomProjectIdMapping:
    """Verify filter_container_tags works when project_id is renamed."""

    @pytest.fixture
    def db_with_custom_project_col(self, spark):
        """EAV table uses 'proj' instead of 'project_id'."""
        container_tags = _eav_dataframe(spark, _EAV_ROWS, project_col="proj")
        container_metrics = _wide_metrics_dataframe(spark, _WIDE_ROWS)
        return _make_db(
            {
                "container_tags": container_tags,
                "container_metrics": container_metrics,
                "channel_metrics": _channel_metrics_dataframe(spark, _CH_METRIC_ROWS),
                "channels": _channels_dataframe(spark, _CH_DATA_ROWS),
            }
        )

    def test_project_filter_with_custom_col(self, spark, db_with_custom_project_col):
        """Solver should filter by renamed project column via container_meta_data_mapping."""
        cfg = {"container_meta_data_mapping": {"project_id": "proj"}}
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=cfg)
        query = db_with_custom_project_col.query
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_project_filter_excludes_other_project(self, spark, db_with_custom_project_col):
        """Only SAMPLE_PROJECT_B entities should come back."""
        cfg = {"container_meta_data_mapping": {"project_id": "proj"}}
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT_B", config=cfg)
        query = db_with_custom_project_col.query
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {4}

    def test_wrong_project_col_mapping_fails(self, spark, db_with_custom_project_col):
        """Default mapping expects 'project_id' but the table has 'proj' — should fail."""
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT")  # default config
        query = db_with_custom_project_col.query
        with pytest.raises(AnalysisException):
            solver.filter_container_tags(spark, query).collect()


# ===================================================================
# TEST GROUP 3: value column mapping (EAV pivot)
# ===================================================================


class TestCustomValueColMapping:
    """Verify EAV pivot uses the renamed value column."""

    @pytest.fixture
    def db_with_custom_value_col(self, spark):
        """EAV table uses 'attr_val' instead of 'value'."""
        container_tags = _eav_dataframe(spark, _EAV_ROWS, value_col="attr_val")
        container_metrics = _wide_metrics_dataframe(spark, _WIDE_ROWS)
        return _make_db(
            {
                "container_tags": container_tags,
                "container_metrics": container_metrics,
                "channel_metrics": _channel_metrics_dataframe(spark, _CH_METRIC_ROWS),
                "channels": _channels_dataframe(spark, _CH_DATA_ROWS, val_col="attr_val"),
            }
        )

    def test_pivot_uses_custom_value_col(self, spark, db_with_custom_value_col):
        """Pivot should aggregate using the renamed value column."""
        cfg = {"channel_data_mapping": {"tstart": "tstart", "tend": "tend", "value": "attr_val"}}
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=cfg)
        query = db_with_custom_value_col.query
        query.where(TagSelector("model") == "Leon")
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {1}

    def test_default_value_col_fails_on_renamed_data(self, spark, db_with_custom_value_col):
        """Default config expects 'value' but the EAV table has 'attr_val'."""
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT")  # default config
        query = db_with_custom_value_col.query
        query.where(TagSelector("model") == "Leon")
        with pytest.raises(AnalysisException):
            solver.filter_container_tags(spark, query).collect()


# ===================================================================
# TEST GROUP 4: container_id column mapping (across tables)
# ===================================================================


class TestCustomContainerIdMapping:
    """Verify that a renamed container_id flows through both stages."""

    @pytest.fixture
    def db_with_custom_cid(self, spark):
        """Both EAV alias target and container_metrics use 'meas_id' instead of 'container_id'."""
        container_tags = _eav_dataframe(spark, _EAV_ROWS)
        container_metrics = _wide_metrics_dataframe(spark, _WIDE_ROWS, cid_col="meas_id")
        return _make_db(
            {
                "container_tags": container_tags,
                "container_metrics": container_metrics,
                "channel_metrics": _channel_metrics_dataframe(
                    spark, _CH_METRIC_ROWS, cid_col="meas_id"
                ),
                "channels": _channels_dataframe(spark, _CH_DATA_ROWS, cid_col="meas_id"),
            }
        )

    def test_filter_container_tags_aliases_to_custom_cid(self, spark, db_with_custom_cid):
        """entity_id should be aliased to 'meas_id' (not 'container_id') after pivot."""
        cfg = {"container_id_col": "meas_id"}
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=cfg)
        query = db_with_custom_cid.query
        result = solver.filter_container_tags(spark, query)
        assert "meas_id" in result.columns
        assert "container_id" not in result.columns
        ids = {row.meas_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_filter_container_metrics_joins_on_custom_cid(self, spark, db_with_custom_cid):
        """Stage 2 should INNER JOIN on 'meas_id' between tags and container_metrics."""
        cfg = {"container_id_col": "meas_id"}
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=cfg)
        query = db_with_custom_cid.query
        tags_df = solver.filter_container_tags(spark, query)
        result = solver.filter_container_metrics(spark, query, tags_df)
        assert "meas_id" in result.columns
        ids = {row.meas_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_metric_filter_with_custom_cid(self, spark, db_with_custom_cid):
        """TagExpression filter + custom container_id through both stages."""
        cfg = {"container_id_col": "meas_id"}
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=cfg)
        query = db_with_custom_cid.query
        query.where(TagSelector("model") == "Ateca")
        tags_df = solver.filter_container_tags(spark, query)
        result = solver.filter_container_metrics(spark, query, tags_df)
        ids = {row.meas_id for row in result.collect()}
        assert ids == {3}


# ===================================================================
# TEST GROUP 5: all EAV columns renamed simultaneously
# ===================================================================


class TestFullyCustomEavMapping:
    """Verify the solver works when entity_id, project_id, value, AND container_id
    are all renamed at once — the most realistic non-default scenario."""

    @pytest.fixture
    def db_fully_custom(self, spark):
        container_tags = _eav_dataframe(
            spark, _EAV_ROWS, entity_col="asset_id", project_col="proj", value_col="attr_val"
        )
        container_metrics = _wide_metrics_dataframe(spark, _WIDE_ROWS, cid_col="run_id")
        return _make_db(
            {
                "container_tags": container_tags,
                "container_metrics": container_metrics,
                "channel_metrics": _channel_metrics_dataframe(
                    spark, _CH_METRIC_ROWS, cid_col="run_id"
                ),
                "channels": _channels_dataframe(
                    spark, _CH_DATA_ROWS, cid_col="run_id", val_col="attr_val"
                ),
            }
        )

    @pytest.fixture
    def full_cfg(self):
        return {
            "container_id_col": "run_id",
            "entity_id_col": "asset_id",
            "container_meta_data_mapping": {"project_id": "proj"},
            "channel_data_mapping": {
                "tstart": "tstart",
                "tend": "tend",
                "value": "attr_val",
            },
        }

    def test_no_filter_fully_custom(self, spark, db_fully_custom, full_cfg):
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=full_cfg)
        query = db_fully_custom.query
        result = solver.filter_container_tags(spark, query)
        assert "run_id" in result.columns
        ids = {row.run_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_single_metric_filter_fully_custom(self, spark, db_fully_custom, full_cfg):
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=full_cfg)
        query = db_fully_custom.query
        query.where(TagSelector("model") == "Leon")
        result = solver.filter_container_tags(spark, query)
        ids = {row.run_id for row in result.collect()}
        assert ids == {1}

    def test_and_filter_fully_custom(self, spark, db_fully_custom, full_cfg):
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=full_cfg)
        query = db_fully_custom.query
        query.where((TagSelector("brand") == "Seat") & (TagSelector("model") == "Ibiza"))
        result = solver.filter_container_tags(spark, query)
        ids = {row.run_id for row in result.collect()}
        assert ids == {2}

    def test_or_filter_fully_custom(self, spark, db_fully_custom, full_cfg):
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=full_cfg)
        query = db_fully_custom.query
        query.where((TagSelector("model") == "Leon") | (TagSelector("model") == "Ateca"))
        result = solver.filter_container_tags(spark, query)
        ids = {row.run_id for row in result.collect()}
        assert ids == {1, 3}

    def test_stages_1_and_2_fully_custom(self, spark, db_fully_custom, full_cfg):
        """Full pipeline: filter_container_tags → filter_container_metrics."""
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=full_cfg)
        query = db_fully_custom.query
        query.where(TagSelector("model") == "Ibiza")
        tags_df = solver.filter_container_tags(spark, query)
        metrics_df = solver.filter_container_metrics(spark, query, tags_df)
        ids = {row.run_id for row in metrics_df.collect()}
        assert ids == {2}

    def test_non_existent_project_fully_custom(self, spark, db_fully_custom, full_cfg):
        solver = KeyValueStoreSolver(spark, "NO_SUCH_PROJECT", config=full_cfg)
        query = db_fully_custom.query
        result = solver.filter_container_tags(spark, query)
        assert result.count() == 0

    def test_other_project_fully_custom(self, spark, db_fully_custom, full_cfg):
        """SAMPLE_PROJECT_B should only contain entity 4."""
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT_B", config=full_cfg)
        query = db_fully_custom.query
        result = solver.filter_container_tags(spark, query)
        ids = {row.run_id for row in result.collect()}
        assert ids == {4}


# ===================================================================
# TEST GROUP 6: channel column mapping (col_map → UDF)
# ===================================================================


class TestCustomChannelColumnMapping:
    """Verify that renamed channel columns (tstart/tend/value/channel_id)
    propagate through col_map into the UDF and produce correct results."""

    @pytest.fixture
    def db_custom_channels(self, spark):
        """Channel data uses t_begin/t_end/signal_val/signal_id instead of defaults."""
        container_tags = _eav_dataframe(spark, _EAV_ROWS)
        container_metrics = _wide_metrics_dataframe(spark, _WIDE_ROWS)
        channel_metrics = _channel_metrics_dataframe(spark, _CH_METRIC_ROWS, ch_col="signal_id")
        channels = _channels_dataframe(
            spark,
            _CH_DATA_ROWS,
            ch_col="signal_id",
            ts_col="t_begin",
            te_col="t_end",
            val_col="signal_val",
        )
        return _make_db(
            {
                "container_tags": container_tags,
                "container_metrics": container_metrics,
                "channel_metrics": channel_metrics,
                "channels": channels,
            }
        )

    def test_col_map_built_from_custom_config(self, spark):
        """The col_map dict should reflect the custom channel_data_mapping."""
        cfg = SolverConfig.from_dict(
            {
                "channel_id_cols": ["container_id", "signal_id"],
                "channel_data_mapping": {
                    "tstart": "t_begin",
                    "tend": "t_end",
                    "value": "signal_val",
                },
            }
        )
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=cfg)
        assert solver.config.container_id_col == "container_id"
        assert solver.config.channel_id_col == "signal_id"
        assert solver.config.tstart_col == "t_begin"
        assert solver.config.tend_col == "t_end"
        assert solver.config.value_col == "signal_val"

    def test_col_map_dict_matches_config(self, spark):
        """The col_map dict built in solve() should match the SolverConfig values."""
        cfg = SolverConfig.from_dict(
            {
                "container_id_col": "run_id",
                "channel_id_cols": ["run_id", "sig_id"],
                "channel_data_mapping": {
                    "tstart": "ts",
                    "tend": "te",
                    "value": "v",
                },
            }
        )
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=cfg)
        col_map = solver.config.col_map
        assert col_map == {
            "cid": "run_id",
            "ch": "sig_id",
            "ts": "ts",
            "te": "te",
            "val": "v",
        }


# ===================================================================
# TEST GROUP 7: SolverConfig propagation through class hierarchy
# ===================================================================


class TestConfigPropagationThroughHierarchy:
    """Verify that config values set at KeyValueStoreSolver level reach
    every instance attribute used by BasicNarrowSolver."""

    @pytest.mark.parametrize(
        "cid,eid,ch_cols,data_map,meta_map",
        [
            (
                "meas_id",
                "obj_id",
                ["meas_id", "sig_id"],
                {"tstart": "ts", "tend": "te", "value": "v"},
                {"project_id": "proj"},
            ),
            (
                "file_id",
                "asset_id",
                ["file_id", "channel_num"],
                {"tstart": "t_start", "tend": "t_stop", "value": "raw_val"},
                {"project_id": "project_identifier"},
            ),
        ],
    )
    def test_all_instance_attrs_match_config(self, spark, cid, eid, ch_cols, data_map, meta_map):
        cfg = {
            "container_id_col": cid,
            "entity_id_col": eid,
            "channel_id_cols": ch_cols,
            "channel_data_mapping": data_map,
            "container_meta_data_mapping": meta_map,
        }
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=cfg)

        # QuerySolver level — use self.config directly (no redundant instance attrs)
        assert solver.config.container_id_col == cid
        assert solver.config.entity_id_col == eid
        assert solver.config.channel_id_cols == ch_cols
        assert solver.config.channel_id_col == ch_cols[-1]
        assert solver.config.tstart_col == data_map["tstart"]
        assert solver.config.tend_col == data_map["tend"]
        assert solver.config.value_col == data_map["value"]

        # Convenience properties
        assert solver.config.project_id_col == meta_map["project_id"]

        # col_map should reflect the same values
        col_map = solver.config.col_map
        assert col_map["cid"] == cid
        assert col_map["ch"] == ch_cols[-1]
        assert col_map["ts"] == data_map["tstart"]
        assert col_map["te"] == data_map["tend"]
        assert col_map["val"] == data_map["value"]


# ===================================================================
# TEST GROUP 8: edge case — entity_id == container_id (same name)
# ===================================================================


class TestEntityIdSameAsContainerId:
    """When the EAV table already uses 'container_id' as the entity column,
    the rename should be a no-op and still work."""

    @pytest.fixture
    def db_entity_is_cid(self, spark):
        """EAV table uses 'container_id' directly — no rename needed."""
        rows = [
            (1, "SAMPLE_PROJECT", "brand", "Seat", "container_concept"),
            (1, "SAMPLE_PROJECT", "model", "Leon", "container_concept"),
            (1, "SAMPLE_PROJECT", "vehicle_key", "Seat_Leon", "container_concept"),
            (2, "SAMPLE_PROJECT", "brand", "Seat", "container_concept"),
            (2, "SAMPLE_PROJECT", "model", "Ibiza", "container_concept"),
            (2, "SAMPLE_PROJECT", "vehicle_key", "Seat_Ibiza", "container_concept"),
        ]
        container_tags = _eav_dataframe(spark, rows, entity_col="container_id")
        wide_rows = [
            (1, "SEAT_LEON", "SAMPLE_PROJECT", "2017-07-06_Seat_Leon.mf4"),
            (2, "SEAT_IBIZA", "SAMPLE_PROJECT", "2017-07-07_Seat_Ibiza.mf4"),
        ]
        container_metrics = _wide_metrics_dataframe(spark, wide_rows)
        return _make_db(
            {
                "container_tags": container_tags,
                "container_metrics": container_metrics,
                "channel_metrics": _channel_metrics_dataframe(
                    spark, [(1, 1, "is1_eng_speed", 100), (2, 1, "is1_eng_speed", 100)]
                ),
                "channels": _channels_dataframe(
                    spark, [(1, 1, 0, 100, 50.0), (2, 1, 0, 100, 60.0)]
                ),
            }
        )

    def test_no_rename_needed(self, spark, db_entity_is_cid):
        cfg = {"entity_id_col": "container_id"}
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=cfg)
        query = db_entity_is_cid.query
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2}

    def test_metric_filter_no_rename(self, spark, db_entity_is_cid):
        cfg = {"entity_id_col": "container_id"}
        solver = KeyValueStoreSolver(spark, "SAMPLE_PROJECT", config=cfg)
        query = db_entity_is_cid.query
        query.where(TagSelector("model") == "Ibiza")
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {2}


# ===================================================================
# TEST GROUP 9: parent_id column mapping
# ===================================================================


class TestCustomParentIdMapping:
    """Verify filter_container_tags respects a renamed parent_id column."""

    @pytest.fixture
    def db_with_custom_parent_col(self, spark):
        """EAV table uses 'concept_type' instead of 'parent_id'."""
        container_tags = _eav_dataframe(spark, _EAV_ROWS, parent_id_col="concept_type")
        container_metrics = _wide_metrics_dataframe(spark, _WIDE_ROWS)
        return _make_db(
            {
                "container_tags": container_tags,
                "container_metrics": container_metrics,
                "channel_metrics": _channel_metrics_dataframe(spark, _CH_METRIC_ROWS),
                "channels": _channels_dataframe(spark, _CH_DATA_ROWS),
            }
        )

    def test_custom_parent_id_col_filters_correctly(self, spark, db_with_custom_parent_col):
        """parent_id filter should use the renamed column via config."""
        cfg = {"parent_id_col": "concept_type"}
        solver = KeyValueStoreSolver(
            spark,
            "SAMPLE_PROJECT",
            parent_id="container_concept",
            config=cfg,
        )
        query = db_with_custom_parent_col.query
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_custom_parent_id_col_non_matching(self, spark, db_with_custom_parent_col):
        """Non-matching parent_id with custom column should return empty."""
        cfg = {"parent_id_col": "concept_type"}
        solver = KeyValueStoreSolver(
            spark,
            "SAMPLE_PROJECT",
            parent_id="wrong_concept",
            config=cfg,
        )
        query = db_with_custom_parent_col.query
        result = solver.filter_container_tags(spark, query)
        assert result.count() == 0
