# pylint: disable=missing-function-docstring
"""Tests for selecting the read seam via ``query_engine.measurement_db``."""

from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

import impulse_query_engine.measurement_db_registry as registry
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from impulse_query_engine.measurement_db_registry import register_measurement_db
from impulse_reporting.config.config_parser import ImpulseConfig
from impulse_reporting.core.report import Report

_SOURCE = {
    "container_metrics_table": "cat.silver.container_metrics",
    "channel_metrics_table": "cat.silver.channel_metrics",
    "channels_uri": "cat.silver.channels",
}
_SINK = {"catalog": "cat", "schema": "gold", "table_prefix": "t"}


class AcmeConfig(MeasurementDBConfig):
    def __init__(self, *, session_table=None, **base_kwargs):
        super().__init__(**base_kwargs)
        self.session_table = session_table


class AcmeDB(MeasurementDB): ...


@pytest.fixture(autouse=True)
def acme_registered(monkeypatch):
    monkeypatch.setattr(registry, "_REGISTRY", dict(registry._REGISTRY))
    register_measurement_db("AcmeDB", AcmeConfig)(AcmeDB)


def _build(query_engine=None):
    config = ImpulseConfig.model_validate(
        {"source": _SOURCE, "unity_sink": _SINK, "query_engine": query_engine or {}}
    )
    return Report.create_measurement_db(config, create_autospec(WorkspaceClient))


def test_defaults_to_builtin():
    db = _build()
    assert type(db) is MeasurementDB
    assert type(db.config) is MeasurementDBConfig
    assert db.config.channels_uri == "cat.silver.channels"
    assert db.config.table_locations == "unity_catalog"


def test_selector_builds_registered_config_and_db():
    db = _build(
        {
            "measurement_db": "AcmeDB",
            "measurement_db_config": {"session_table": "cat.raw.sessions"},
        }
    )
    assert type(db) is AcmeDB
    assert type(db.config) is AcmeConfig
    assert db.config.session_table == "cat.raw.sessions"
    assert db.config.container_metrics_table == "cat.silver.container_metrics"
