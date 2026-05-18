# pylint: disable=missing-function-docstring
"""
Tests for SolverConfig driven by a single JSON fixture file.

All expected values are read from:
    tests/data/config/solver_config_test.json

The JSON describes a non-default configuration so every field is
exercised and every assertion is meaningful.
"""

import json
import pathlib

import pytest

from mda_query_engine.analyze.query.solvers.solver_config import (
    SolverConfig,
    TableConfig,
)

# ---------------------------------------------------------------------------
# Fixture path
# ---------------------------------------------------------------------------

_CONFIG_PATH = (
    pathlib.Path(__file__).parents[5] / "data" / "config" / "solver_config_test.json"  # …/tests/
)

# Internal column-name constants the new SolverConfig always returns,
# regardless of any per-table column_name_mapping.
_EXPECTED_COL_MAP = {
    "cid": "container_id",
    "ch": "channel_id",
    "ts": "tstart",
    "te": "tend",
    "val": "value",
}


# ---------------------------------------------------------------------------
# Shared fixture: load once, reuse across all tests in the module
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def cfg() -> SolverConfig:
    """SolverConfig loaded from solver_config_test.json via from_json."""
    return SolverConfig.from_json(str(_CONFIG_PATH))


@pytest.fixture(scope="module")
def raw_data() -> dict:
    """Raw dictionary parsed directly from solver_config_test.json."""
    with open(_CONFIG_PATH) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# TestSolverConfigFromJson – structural / field checks
# ---------------------------------------------------------------------------


class TestSolverConfigFromJson:
    """Verify that from_json populates every field exactly as the JSON states."""

    def test_json_file_exists(self):
        assert _CONFIG_PATH.exists(), f"Fixture not found: {_CONFIG_PATH}"

    def test_channel_mapping_filters(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.channel_mapping.filters == raw_data["channel_mapping"]["filters"]

    def test_channels_mapping(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.channels.column_name_mapping == raw_data["channels"]["column_name_mapping"]

    def test_container_tags_filters(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.container_tags.filters == raw_data["container_tags"]["filters"]

    def test_container_tags_mapping(self, cfg: SolverConfig, raw_data: dict):
        assert (
            cfg.container_tags.column_name_mapping
            == raw_data["container_tags"]["column_name_mapping"]
        )

    def test_project_id(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.project_id == raw_data["project_id"]

    def test_unconfigured_tables_use_defaults(self, cfg: SolverConfig):
        assert cfg.container_metrics == TableConfig()
        assert cfg.channel_tags == TableConfig()
        assert cfg.channel_metrics == TableConfig()


# ---------------------------------------------------------------------------
# TestSolverConfigProperties – convenience property checks
# ---------------------------------------------------------------------------


class TestSolverConfigProperties:
    """Verify derived properties match the JSON-backed field values."""

    def test_channel_id_col(self, cfg: SolverConfig):
        assert cfg.channel_id_col == "channel_id"

    def test_channel_id_cols(self, cfg: SolverConfig):
        assert cfg.channel_id_cols == ["container_id", "channel_id"]

    def test_container_id_col(self, cfg: SolverConfig):
        assert cfg.container_id_col == "container_id"

    def test_parent_id_col(self, cfg: SolverConfig):
        assert cfg.parent_id_col == "parent_id"

    def test_properties_same_for_default_config(self):
        default = SolverConfig()
        assert default.container_id_col == "container_id"
        assert default.channel_id_col == "channel_id"
        assert default.tstart_col == "tstart"
        assert default.tend_col == "tend"
        assert default.value_col == "value"
        assert default.project_id_col == "project_id"
        assert default.parent_id_col == "parent_id"


# ---------------------------------------------------------------------------
# TestFromDict – round-trip through dict matches JSON
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# TestColMap – col_map property (PR rework item #3)
# ---------------------------------------------------------------------------


class TestColMap:
    """Verify the col_map property returns the correct short-key mapping."""

    def test_col_map_keys(self, cfg: SolverConfig):
        """col_map should contain exactly the expected short keys."""
        assert set(cfg.col_map.keys()) == {"cid", "ch", "ts", "te", "val"}

    def test_col_map_default_config(self):
        """Default SolverConfig col_map should match hardcoded defaults."""
        default = SolverConfig()
        assert default.col_map == {
            "cid": "container_id",
            "ch": "channel_id",
            "ts": "tstart",
            "te": "tend",
            "val": "value",
        }

    def test_col_map_consistent_with_properties(self, cfg: SolverConfig):
        """col_map values must match the individual property accessors."""
        assert cfg.col_map["cid"] == cfg.container_id_col
        assert cfg.col_map["ch"] == cfg.channel_id_col
        assert cfg.col_map["ts"] == cfg.tstart_col
        assert cfg.col_map["te"] == cfg.tend_col
        assert cfg.col_map["val"] == cfg.value_col

    def test_col_map_values(self, cfg: SolverConfig):
        assert cfg.col_map == _EXPECTED_COL_MAP
