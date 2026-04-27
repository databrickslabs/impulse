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

from mda_query_engine.analyze.query.solvers.solver_config import SolverConfig

# ---------------------------------------------------------------------------
# Fixture path
# ---------------------------------------------------------------------------

_CONFIG_PATH = (
    pathlib.Path(__file__).parents[5] / "data" / "config" / "solver_config_test.json"  # …/tests/
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

    def test_container_id_col(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.container_id_col == raw_data["container_id_col"]

    def test_channel_id_cols(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.channel_id_cols == raw_data["channel_id_cols"]

    def test_channel_data_mapping(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.channel_data_mapping == raw_data["channel_data_mapping"]

    def test_container_meta_data_mapping(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.container_meta_data_mapping == raw_data["container_meta_data_mapping"]


# ---------------------------------------------------------------------------
# TestSolverConfigProperties – convenience property checks
# ---------------------------------------------------------------------------


class TestSolverConfigProperties:
    """Verify derived properties match the JSON-backed field values."""

    def test_channel_id_col_is_last_element(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.channel_id_col == raw_data["channel_id_cols"][-1]

    def test_tstart_col(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.tstart_col == raw_data["channel_data_mapping"]["tstart"]

    def test_tend_col(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.tend_col == raw_data["channel_data_mapping"]["tend"]

    def test_value_col(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.value_col == raw_data["channel_data_mapping"]["value"]

    def test_project_id_col(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.project_id_col == raw_data["container_meta_data_mapping"]["project_id"]


# ---------------------------------------------------------------------------
# TestFromDict – round-trip through dict matches JSON
# ---------------------------------------------------------------------------


class TestFromDict:
    """from_dict produces the same SolverConfig as from_json for the same data."""

    def test_round_trip_equals_from_json(self, cfg: SolverConfig, raw_data: dict):
        cfg_from_dict = SolverConfig.from_dict(raw_data)
        assert cfg_from_dict.container_id_col == cfg.container_id_col
        assert cfg_from_dict.channel_id_cols == cfg.channel_id_cols
        assert cfg_from_dict.channel_data_mapping == cfg.channel_data_mapping
        assert cfg_from_dict.container_meta_data_mapping == cfg.container_meta_data_mapping

    def test_missing_keys_use_defaults(self):
        """from_dict with an empty dict produces a default SolverConfig."""
        default = SolverConfig()
        from_empty = SolverConfig.from_dict({})
        assert from_empty.container_id_col == default.container_id_col
        assert from_empty.channel_id_cols == default.channel_id_cols
        assert from_empty.channel_data_mapping == default.channel_data_mapping
        assert from_empty.container_meta_data_mapping == default.container_meta_data_mapping


# ---------------------------------------------------------------------------
# TestColMap – col_map property (PR rework item #3)
# ---------------------------------------------------------------------------


class TestColMap:
    """Verify the col_map property returns the correct short-key mapping."""

    def test_col_map_keys(self, cfg: SolverConfig):
        """col_map should contain exactly the expected short keys."""
        assert set(cfg.col_map.keys()) == {"cid", "ch", "ts", "te", "val"}

    def test_col_map_cid(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.col_map["cid"] == raw_data["container_id_col"]

    def test_col_map_ch(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.col_map["ch"] == raw_data["channel_id_cols"][-1]

    def test_col_map_ts(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.col_map["ts"] == raw_data["channel_data_mapping"]["tstart"]

    def test_col_map_te(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.col_map["te"] == raw_data["channel_data_mapping"]["tend"]

    def test_col_map_val(self, cfg: SolverConfig, raw_data: dict):
        assert cfg.col_map["val"] == raw_data["channel_data_mapping"]["value"]

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
