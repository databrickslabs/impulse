# pylint: disable=missing-function-docstring
"""
Tests verifying that SolverConfig column-name mappings are actually used
by KeyValueStoreSolver when processing DataFrames with non-default column names.

Existing tests prove that SolverConfig stores/returns the right values and that
the solver works with default column names.  These tests close the gap: they
create DataFrames whose columns use **custom physical names** and verify the
solver renames them to internal names via ``column_name_mapping`` — proving
the wiring is live, not just plumbed.

Covers:
- Renamed entity column in EAV table  →  filter_container_tags
- Renamed project_id column in EAV table →  filter_container_tags
- Renamed value column in EAV table      →  filter_container_tags (pivot)
- Renamed container_id across all tables →  filter_container_tags + filter_container_metrics
- Combined: all EAV columns renamed      →  filter_container_tags with TagExpression
- Renamed channel columns (tstart/tend/value/channel_id) → solve (UDF + col_map)
- Renamed parent_id column → per-table filters
- Negative: wrong mapping → zero results or error
"""

import pytest

import pandas as pd
import pyspark.sql.types as T
from pyspark.sql import SparkSession

from mda_query_engine.analyze.metadata.tag_expression import TagSelector
from mda_query_engine.analyze.query.solvers.key_value_store_solver import (
    KeyValueStoreSolver,
)
from mda_query_engine.analyze.query.solvers.solver_config import (
    SolverConfig,
    TableConfig,
)
from mda_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig

from tests.conftest import spark

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _eav_dataframe(
    spark,
    rows,
    entity_col="container_id",
    project_col="project_id",
    key_col="key",
    value_col="value",
    parent_id_col="parent_id",
):
    """Build a narrow/EAV DataFrame with configurable column names.

    The default column names match the framework-internal names so that
    tests not concerned with column renaming can use zero-config solvers.
    """
    pdf = pd.DataFrame(
        rows, columns=[entity_col, project_col, key_col, value_col, parent_id_col]
    )
    schema = T.StructType(
        [
            T.StructField(entity_col, T.LongType()),
            T.StructField(project_col, T.StringType()),
            T.StructField(key_col, T.StringType()),
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


def _channel_metrics_dataframe(
    spark, rows, cid_col="container_id", ch_col="channel_id"
):
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
    return MeasurementDB(cfg)


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
# TEST GROUP 1: entity column mapping
# ===================================================================

class TestCustomEntityIdMapping:
    """Verify filter_container_tags works when the entity column has a
    non-default physical name that needs renaming to 'container_id'."""

    @pytest.fixture
    def db_with_custom_entity_col(self, spark):
        """EAV table uses 'object_id' instead of 'container_id'."""
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
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_tags=TableConfig(
                    column_name_mapping={"object_id": "container_id"}
                ),
            ),
        )
        query = db_with_custom_entity_col.query
        result = solver.filter_container_tags(spark, query)
        assert "container_id" in result.columns
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_with_tag_filter_and_custom_entity_col(self, spark, db_with_custom_entity_col):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_tags=TableConfig(
                    column_name_mapping={"object_id": "container_id"}
                ),
            ),
        )
        query = db_with_custom_entity_col.query
        query.where(TagSelector("model") == "Ateca")
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {3}

    def test_wrong_entity_col_returns_error_or_empty(self, spark, db_with_custom_entity_col):
        """Using default config when the actual column is 'object_id' should fail."""
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(project_id="SAMPLE_PROJECT"),
        )
        query = db_with_custom_entity_col.query
        with pytest.raises(Exception):
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
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_tags=TableConfig(
                    column_name_mapping={"proj": "project_id"}
                ),
            ),
        )
        query = db_with_custom_project_col.query
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_project_filter_excludes_other_project(self, spark, db_with_custom_project_col):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT_B",
                container_tags=TableConfig(
                    column_name_mapping={"proj": "project_id"}
                ),
            ),
        )
        query = db_with_custom_project_col.query
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {4}

    def test_wrong_project_col_mapping_fails(self, spark, db_with_custom_project_col):
        """Default mapping expects 'project_id' but the table has 'proj' — should fail."""
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(project_id="SAMPLE_PROJECT"),
        )
        query = db_with_custom_project_col.query
        with pytest.raises(Exception):
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
                "channels": _channels_dataframe(spark, _CH_DATA_ROWS),
            }
        )

    def test_pivot_uses_custom_value_col(self, spark, db_with_custom_value_col):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_tags=TableConfig(
                    column_name_mapping={"attr_val": "value"}
                ),
            ),
        )
        query = db_with_custom_value_col.query
        query.where(TagSelector("model") == "Leon")
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {1}

    def test_default_value_col_fails_on_renamed_data(self, spark, db_with_custom_value_col):
        """Default config expects 'value' but the EAV table has 'attr_val'."""
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(project_id="SAMPLE_PROJECT"),
        )
        query = db_with_custom_value_col.query
        query.where(TagSelector("model") == "Leon")
        with pytest.raises(Exception):
            solver.filter_container_tags(spark, query).collect()


# ===================================================================
# TEST GROUP 4: container_id column mapping (across tables)
# ===================================================================

class TestCustomContainerIdMapping:
    """Verify that a renamed container_id flows through both stages.

    After column renaming, the result always uses the internal name
    ``container_id`` — not the physical name."""

    @pytest.fixture
    def db_with_custom_cid(self, spark):
        """container_metrics and channels use 'meas_id' instead of 'container_id'."""
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

    def _cfg(self):
        return SolverConfig(
            project_id="SAMPLE_PROJECT",
            container_metrics=TableConfig(
                column_name_mapping={"meas_id": "container_id"}
            ),
            channel_metrics=TableConfig(
                column_name_mapping={"meas_id": "container_id"}
            ),
            channels=TableConfig(
                column_name_mapping={"meas_id": "container_id"}
            ),
        )

    def test_filter_container_tags_returns_internal_name(self, spark, db_with_custom_cid):
        """EAV table uses default 'container_id'; result should also use it."""
        solver = KeyValueStoreSolver(spark, config=self._cfg())
        query = db_with_custom_cid.query
        result = solver.filter_container_tags(spark, query)
        assert "container_id" in result.columns
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_filter_container_metrics_joins_on_internal_name(self, spark, db_with_custom_cid):
        """Stage 2 should join on 'container_id' (internal name) after renaming."""
        solver = KeyValueStoreSolver(spark, config=self._cfg())
        query = db_with_custom_cid.query
        tags_df = solver.filter_container_tags(spark, query)
        result = solver.filter_container_metrics(spark, query, tags_df)
        assert "container_id" in result.columns
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_tag_filter_with_custom_cid(self, spark, db_with_custom_cid):
        """TagExpression filter + custom container_id through both stages."""
        solver = KeyValueStoreSolver(spark, config=self._cfg())
        query = db_with_custom_cid.query
        query.where(TagSelector("model") == "Ateca")
        tags_df = solver.filter_container_tags(spark, query)
        result = solver.filter_container_metrics(spark, query, tags_df)
        ids = {row.container_id for row in result.collect()}
        assert ids == {3}


# ===================================================================
# TEST GROUP 5: all EAV columns renamed simultaneously
# ===================================================================

class TestFullyCustomEavMapping:
    """Verify the solver works when entity_id, project_id, value, AND container_id
    are all renamed at once — the most realistic non-default scenario.

    After column renaming, results always use internal names."""

    @pytest.fixture
    def db_fully_custom(self, spark):
        container_tags = _eav_dataframe(
            spark,
            _EAV_ROWS,
            entity_col="asset_id",
            project_col="proj",
            value_col="attr_val",
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
        return SolverConfig(
            project_id="SAMPLE_PROJECT",
            container_tags=TableConfig(
                column_name_mapping={
                    "asset_id": "container_id",
                    "proj": "project_id",
                    "attr_val": "value",
                }
            ),
            container_metrics=TableConfig(
                column_name_mapping={"run_id": "container_id"}
            ),
            channel_metrics=TableConfig(
                column_name_mapping={"run_id": "container_id"}
            ),
            channels=TableConfig(
                column_name_mapping={
                    "run_id": "container_id",
                    "attr_val": "value",
                }
            ),
        )

    def test_no_filter_fully_custom(self, spark, db_fully_custom, full_cfg):
        solver = KeyValueStoreSolver(spark, config=full_cfg)
        query = db_fully_custom.query
        result = solver.filter_container_tags(spark, query)
        assert "container_id" in result.columns
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_single_tag_filter_fully_custom(self, spark, db_fully_custom, full_cfg):
        solver = KeyValueStoreSolver(spark, config=full_cfg)
        query = db_fully_custom.query
        query.where(TagSelector("model") == "Leon")
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {1}

    def test_and_filter_fully_custom(self, spark, db_fully_custom, full_cfg):
        solver = KeyValueStoreSolver(spark, config=full_cfg)
        query = db_fully_custom.query
        query.where((TagSelector("brand") == "Seat") & (TagSelector("model") == "Ibiza"))
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {2}

    def test_or_filter_fully_custom(self, spark, db_fully_custom, full_cfg):
        solver = KeyValueStoreSolver(spark, config=full_cfg)
        query = db_fully_custom.query
        query.where((TagSelector("model") == "Leon") | (TagSelector("model") == "Ateca"))
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 3}

    def test_stages_1_and_2_fully_custom(self, spark, db_fully_custom, full_cfg):
        """Full pipeline: filter_container_tags → filter_container_metrics."""
        solver = KeyValueStoreSolver(spark, config=full_cfg)
        query = db_fully_custom.query
        query.where(TagSelector("model") == "Ibiza")
        tags_df = solver.filter_container_tags(spark, query)
        metrics_df = solver.filter_container_metrics(spark, query, tags_df)
        ids = {row.container_id for row in metrics_df.collect()}
        assert ids == {2}

    def test_non_existent_project_fully_custom(self, spark, db_fully_custom, full_cfg):
        cfg = full_cfg.model_copy(update={"project_id": "NO_SUCH_PROJECT"})
        solver = KeyValueStoreSolver(spark, config=cfg)
        query = db_fully_custom.query
        result = solver.filter_container_tags(spark, query)
        assert result.count() == 0

    def test_other_project_fully_custom(self, spark, db_fully_custom, full_cfg):
        """SAMPLE_PROJECT_B should only contain entity 4."""
        cfg = full_cfg.model_copy(update={"project_id": "SAMPLE_PROJECT_B"})
        solver = KeyValueStoreSolver(spark, config=cfg)
        query = db_fully_custom.query
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {4}


# ===================================================================
# TEST GROUP 6: channel column mapping
# ===================================================================

class TestCustomChannelColumnMapping:
    """Verify that column_name_mapping entries are stored correctly and
    that properties always return the fixed internal names regardless
    of the mapping contents."""

    def test_properties_return_internal_names_with_custom_mapping(self, spark):
        cfg = SolverConfig(
            channels=TableConfig(
                column_name_mapping={
                    "signal_id": "channel_id",
                    "t_begin": "tstart",
                    "t_end": "tend",
                    "signal_val": "value",
                }
            )
        )
        solver = KeyValueStoreSolver(spark, config=cfg)
        assert solver.config.container_id_col == "container_id"
        assert solver.config.channel_id_col == "channel_id"
        assert solver.config.tstart_col == "tstart"
        assert solver.config.tend_col == "tend"
        assert solver.config.value_col == "value"

    def test_col_map_always_returns_internal_names(self, spark):
        cfg = SolverConfig(
            channels=TableConfig(
                column_name_mapping={
                    "run_id": "container_id",
                    "sig_id": "channel_id",
                    "ts": "tstart",
                    "te": "tend",
                    "v": "value",
                }
            )
        )
        solver = KeyValueStoreSolver(spark, config=cfg)
        assert solver.config.col_map == {
            "cid": "container_id",
            "ch": "channel_id",
            "ts": "tstart",
            "te": "tend",
            "val": "value",
        }

    def test_mapping_entries_stored_correctly(self, spark):
        mapping = {
            "signal_id": "channel_id",
            "t_begin": "tstart",
            "t_end": "tend",
            "signal_val": "value",
        }
        cfg = SolverConfig(channels=TableConfig(column_name_mapping=mapping))
        solver = KeyValueStoreSolver(spark, config=cfg)
        assert solver.config.channels.column_name_mapping == mapping


# ===================================================================
# TEST GROUP 7: edge case — entity column already named container_id
# ===================================================================

class TestEntityIdSameAsContainerId:
    """When the EAV table already uses 'container_id' as the entity column,
    no mapping is needed and everything should work with the default config."""

    @pytest.fixture
    def db_entity_is_cid(self, spark):
        rows = [
            (1, "SAMPLE_PROJECT", "brand", "Seat", "container_concept"),
            (1, "SAMPLE_PROJECT", "model", "Leon", "container_concept"),
            (1, "SAMPLE_PROJECT", "vehicle_key", "Seat_Leon", "container_concept"),
            (2, "SAMPLE_PROJECT", "brand", "Seat", "container_concept"),
            (2, "SAMPLE_PROJECT", "model", "Ibiza", "container_concept"),
            (2, "SAMPLE_PROJECT", "vehicle_key", "Seat_Ibiza", "container_concept"),
        ]
        container_tags = _eav_dataframe(spark, rows)
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

    def test_no_mapping_needed(self, spark, db_entity_is_cid):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(project_id="SAMPLE_PROJECT"),
        )
        query = db_entity_is_cid.query
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2}

    def test_tag_filter_no_mapping(self, spark, db_entity_is_cid):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(project_id="SAMPLE_PROJECT"),
        )
        query = db_entity_is_cid.query
        query.where(TagSelector("model") == "Ibiza")
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {2}


# ===================================================================
# TEST GROUP 8: parent_id column mapping (per-table filters)
# ===================================================================


class TestCustomParentIdMapping:
    """Verify filter_container_tags respects a renamed parent_id column.

    Per-table filters use **internal** column names (applied after
    column renaming), so the mapping must also be configured."""

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
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_tags=TableConfig(
                    column_name_mapping={"concept_type": "parent_id"},
                    filters={"parent_id": "container_concept"},
                ),
            ),
        )
        query = db_with_custom_parent_col.query
        result = solver.filter_container_tags(spark, query)
        ids = {row.container_id for row in result.collect()}
        assert ids == {1, 2, 3}

    def test_custom_parent_id_col_non_matching(self, spark, db_with_custom_parent_col):
        solver = KeyValueStoreSolver(
            spark,
            config=SolverConfig(
                project_id="SAMPLE_PROJECT",
                container_tags=TableConfig(
                    column_name_mapping={"concept_type": "parent_id"},
                    filters={"parent_id": "wrong_concept"},
                ),
            ),
        )
        query = db_with_custom_parent_col.query
        result = solver.filter_container_tags(spark, query)
        assert result.count() == 0
