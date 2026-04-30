# pylint: disable=missing-function-docstring
"""
Tests for SolverConfig driven by a single JSON fixture file.

All expected values are read from:
    tests/unit/data/config/solver_config_test.json

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
    pathlib.Path(__file__).parents[5]  # …/tests/
    / "unit"
    / "data"
    / "config"
    / "solver_config_test.json"
)


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

    def test_project_id(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.project_id == raw_data["project_id"]

    def test_container_tags_mapping(self, cfg: SolverConfig, raw_data: dict):
        assert (
            cfg.container_tags.column_name_mapping
            == raw_data["container_tags"]["column_name_mapping"]
        )

    def test_container_tags_filters(self, cfg: SolverConfig, raw_data: dict):
        assert (
            cfg.container_tags.filters
            == raw_data["container_tags"]["filters"]
        )

    def test_channel_mapping_filters(self, cfg: SolverConfig, raw_data: dict):
        assert (
            cfg.channel_mapping.filters
            == raw_data["channel_mapping"]["filters"]
        )

    def test_channels_mapping(self, cfg: SolverConfig, raw_data: dict):
        assert (
            cfg.channels.column_name_mapping
            == raw_data["channels"]["column_name_mapping"]
        )

    def test_unconfigured_tables_use_defaults(self, cfg: SolverConfig):
        assert cfg.container_metrics == TableConfig()
        assert cfg.channel_tags == TableConfig()
        assert cfg.channel_metrics == TableConfig()


# ---------------------------------------------------------------------------
# TestSolverConfigProperties – convenience property checks
# ---------------------------------------------------------------------------


class TestSolverConfigProperties:
    """Properties always return the fixed internal column names."""

    def test_container_id_col(self, cfg: SolverConfig):
        assert cfg.container_id_col == "container_id"

    def test_channel_id_col(self, cfg: SolverConfig):
        assert cfg.channel_id_col == "channel_id"

    def test_channel_id_cols(self, cfg: SolverConfig):
        assert cfg.channel_id_cols == ["container_id", "channel_id"]

    def test_tstart_col(self, cfg: SolverConfig):
        assert cfg.tstart_col == "tstart"

    def test_tend_col(self, cfg: SolverConfig):
        assert cfg.tend_col == "tend"

    def test_value_col(self, cfg: SolverConfig):
        assert cfg.value_col == "value"

    def test_project_id_col(self, cfg: SolverConfig):
        assert cfg.project_id_col == "project_id"

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


class TestFromDict:
    """from_dict produces the same SolverConfig as from_json for the same data."""

    def test_round_trip_equals_from_json(self, cfg: SolverConfig, raw_data: dict):
        cfg_from_dict = SolverConfig.from_dict(raw_data)
        assert cfg_from_dict.project_id == cfg.project_id
        assert cfg_from_dict.container_tags == cfg.container_tags
        assert cfg_from_dict.channels == cfg.channels

    def test_missing_keys_use_defaults(self):
        """from_dict with an empty dict produces a default SolverConfig."""
        default = SolverConfig()
        from_empty = SolverConfig.from_dict({})
        assert from_empty.project_id == default.project_id
        assert from_empty.container_tags == default.container_tags
        assert from_empty.channels == default.channels
        assert from_empty.container_id_col == default.container_id_col


# ---------------------------------------------------------------------------
# TestColMap – col_map property
# ---------------------------------------------------------------------------

_EXPECTED_COL_MAP = {
    "cid": "container_id",
    "ch": "channel_id",
    "ts": "tstart",
    "te": "tend",
    "val": "value",
}


class TestColMap:
    """col_map always returns the fixed internal-name mapping."""

    def test_col_map_keys(self, cfg: SolverConfig):
        assert set(cfg.col_map.keys()) == {"cid", "ch", "ts", "te", "val"}

    def test_col_map_values(self, cfg: SolverConfig):
        assert cfg.col_map == _EXPECTED_COL_MAP

    def test_col_map_default_config(self):
        assert SolverConfig().col_map == _EXPECTED_COL_MAP

    def test_col_map_consistent_with_properties(self, cfg: SolverConfig):
        assert cfg.col_map["cid"] == cfg.container_id_col
        assert cfg.col_map["ch"] == cfg.channel_id_col
        assert cfg.col_map["ts"] == cfg.tstart_col
        assert cfg.col_map["te"] == cfg.tend_col
        assert cfg.col_map["val"] == cfg.value_col
