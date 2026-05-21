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

from impulse_query_engine.analyze.query.solvers.solver_config import (
    ChannelMappingConfig,
    JoinKey,
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
    "conv": "conversion_factor",
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
        assert set(cfg.col_map.keys()) == {"cid", "ch", "ts", "te", "val", "conv"}

    def test_col_map_default_config(self):
        """Default SolverConfig col_map should match hardcoded defaults."""
        default = SolverConfig()
        assert default.col_map == {
            "cid": "container_id",
            "ch": "channel_id",
            "ts": "tstart",
            "te": "tend",
            "val": "value",
            "conv": "conversion_factor",
        }

    def test_col_map_consistent_with_properties(self, cfg: SolverConfig):
        """col_map values must match the individual property accessors."""
        assert cfg.col_map["cid"] == cfg.container_id_col
        assert cfg.col_map["ch"] == cfg.channel_id_col
        assert cfg.col_map["ts"] == cfg.tstart_col
        assert cfg.col_map["te"] == cfg.tend_col
        assert cfg.col_map["val"] == cfg.value_col
        assert cfg.col_map["conv"] == cfg.conversion_factor_col

    def test_col_map_values(self, cfg: SolverConfig):
        assert cfg.col_map == _EXPECTED_COL_MAP


# ---------------------------------------------------------------------------
# TestAliasInternalNameProperties – channel mapping / metrics internal names
# ---------------------------------------------------------------------------


class TestAliasInternalNameProperties:
    """Internal-name properties for the alias-resolution columns."""

    def test_source_channel_col(self):
        assert SolverConfig().source_channel_col == "source_channel"

    def test_data_key_col(self):
        assert SolverConfig().data_key_col == "data_key"

    def test_channel_alias_col(self):
        assert SolverConfig().channel_alias_col == "channel_alias"

    def test_channel_name_col(self):
        assert SolverConfig().channel_name_col == "channel_name"


# ---------------------------------------------------------------------------
# TestEffectiveAliasJoinKeys – default + override behavior
# ---------------------------------------------------------------------------


class TestEffectiveAliasJoinKeys:
    def test_default_when_join_keys_none(self):
        cfg = SolverConfig()
        assert cfg.channel_mapping.join_keys is None
        assert cfg.effective_alias_join_keys == [
            ("source_channel", "channel_name"),
            ("data_key", "data_key"),
        ]

    def test_single_column_override(self):
        cfg = SolverConfig(
            channel_mapping=ChannelMappingConfig(
                join_keys=[JoinKey(mapping_col="source_channel", metrics_col="channel_name")]
            )
        )
        assert cfg.effective_alias_join_keys == [("source_channel", "channel_name")]

    def test_different_names_per_side(self):
        cfg = SolverConfig(
            channel_mapping=ChannelMappingConfig(
                join_keys=[
                    JoinKey(mapping_col="source_channel", metrics_col="channel_name"),
                    JoinKey(mapping_col="map_dk", metrics_col="metrics_dk"),
                ]
            )
        )
        assert cfg.effective_alias_join_keys == [
            ("source_channel", "channel_name"),
            ("map_dk", "metrics_dk"),
        ]


# ---------------------------------------------------------------------------
# TestChannelMappingConfig – type acceptance + JSON round-trip
# ---------------------------------------------------------------------------


class TestChannelMappingConfig:
    def test_accepts_channel_mapping_config_instance(self):
        cm = ChannelMappingConfig(
            filters={"toolbox_id": "tb"},
            join_keys=[JoinKey(mapping_col="source_channel", metrics_col="channel_name")],
        )
        cfg = SolverConfig(channel_mapping=cm)
        assert cfg.channel_mapping is cm

    def test_json_round_trip_with_join_keys(self):
        raw = {
            "channel_mapping": {
                "column_name_mapping": {"alias": "channel_alias"},
                "filters": {"toolbox_id": "tb"},
                "join_keys": [{"mapping_col": "source_channel", "metrics_col": "channel_name"}],
            }
        }
        cfg = SolverConfig.from_dict(raw)
        assert isinstance(cfg.channel_mapping, ChannelMappingConfig)
        assert cfg.channel_mapping.column_name_mapping == {"alias": "channel_alias"}
        assert cfg.channel_mapping.filters == {"toolbox_id": "tb"}
        assert cfg.channel_mapping.join_keys == [
            JoinKey(mapping_col="source_channel", metrics_col="channel_name")
        ]
