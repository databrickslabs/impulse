# pylint: disable=missing-function-docstring
"""Unit tests for POI wiring on MeasurementDBConfig / MeasurementDB.

Covers the ``poi_channels_uri`` slot on the config factories and the
``has_poi_channels`` / ``poi_channels`` guard — the config-level half of POI
support that needs no SparkSession.
"""

from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig


def _db(cfg: MeasurementDBConfig) -> MeasurementDB:
    return MeasurementDB(cfg, ws=create_autospec(WorkspaceClient))


class TestForDebug:
    def test_poi_channels_wired_when_present(self):
        cfg = MeasurementDBConfig.for_debug({"channels": object(), "poi_channels": object()})
        assert cfg.poi_channels_uri == "poi_channels"
        assert _db(cfg).has_poi_channels()

    def test_poi_channels_none_when_absent(self):
        cfg = MeasurementDBConfig.for_debug({"channels": object()})
        assert cfg.poi_channels_uri is None
        assert not _db(cfg).has_poi_channels()


class TestForUnityCatalog:
    def test_poi_channels_uri_wired(self):
        cfg = MeasurementDBConfig.for_unity_catalog(
            "cat", poi_channels_uri="cat.core.poi_channels"
        )
        assert cfg.poi_channels_uri == "cat.core.poi_channels"
        assert _db(cfg).has_poi_channels()

    def test_poi_channels_uri_defaults_none(self):
        cfg = MeasurementDBConfig.for_unity_catalog("cat")
        assert cfg.poi_channels_uri is None
        assert not _db(cfg).has_poi_channels()


class TestPoiChannelsReaderGuard:
    def test_reader_raises_when_not_configured(self):
        db = _db(MeasurementDBConfig.for_debug({"channels": object()}))
        with pytest.raises(ValueError, match="poi_channels_uri is not configured"):
            db.poi_channels(spark=None)

    def test_reader_returns_debug_table(self):
        sentinel = object()
        db = _db(MeasurementDBConfig.for_debug({"poi_channels": sentinel}))
        # debug mode returns the in-memory table object as-is
        assert db.poi_channels(spark=None) is sentinel
