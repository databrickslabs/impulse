# pylint: disable=missing-function-docstring
"""Tests for the ``AbstractMeasurementDB`` contract and the MeasurementDB registry wiring."""

from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from impulse_query_engine import __version__
from impulse_query_engine.measurement_db import (
    AbstractMeasurementDB,
    MeasurementDB,
    MeasurementDBConfig,
)
from impulse_query_engine.measurement_db_registry import (
    register_measurement_db,
    resolve_measurement_db,
)


def test_measurement_db_implements_the_contract():
    # Guard: if a new abstract method is added to AbstractMeasurementDB without an
    # implementation on the built-in, this fails here with a clear, local signal rather
    # than as a confusing instantiation error scattered across the rest of the suite.
    assert issubclass(MeasurementDB, AbstractMeasurementDB)
    assert not MeasurementDB.__abstractmethods__


def test_incomplete_implementation_cannot_be_instantiated():
    class Incomplete(AbstractMeasurementDB):
        pass

    with pytest.raises(TypeError, match="abstract"):
        Incomplete(MeasurementDBConfig(), ws=None)


def test_ws_is_the_verified_client():
    ws = create_autospec(WorkspaceClient)
    db = MeasurementDB(MeasurementDBConfig(), ws)

    assert db.ws is ws
    assert ws.config._product_info == ("databricks-impulse", __version__)  # tagged as Impulse
    ws.clusters.select_spark_version.assert_called_once()  # the connectivity check ran


def test_subclass_skipping_super_init_fails_loudly_on_ws():
    class SkipsSuperInit(MeasurementDB):
        def __init__(self, config, ws):
            self.config = config  # forgets super().__init__(config, ws)

    db = SkipsSuperInit(MeasurementDBConfig(), create_autospec(WorkspaceClient))

    expected = r"SkipsSuperInit\.__init__ must call super\(\)\.__init__\(config, ws\)"
    with pytest.raises(TypeError, match=expected):
        _ = db.ws
    with pytest.raises(TypeError, match=expected):  # standalone path: QueryBuilder reads db.ws
        _ = db.query


def test_subclass_cannot_assign_ws_itself():
    class AssignsWs(MeasurementDB):
        def __init__(self, config, ws):
            self.config = config
            self.ws = ws  # would bypass verify_workspace_client

    with pytest.raises(AttributeError, match="has no setter"):
        AssignsWs(MeasurementDBConfig(), create_autospec(WorkspaceClient))


def test_register_rejects_non_subclass():
    with pytest.raises(TypeError, match="must subclass AbstractMeasurementDB"):
        register_measurement_db("Bad", MeasurementDBConfig)(object)


def test_builtin_measurement_db_is_registered():
    registration = resolve_measurement_db("MeasurementDB")

    assert registration.impl_cls is MeasurementDB
    assert registration.config_cls is MeasurementDBConfig


def test_unknown_name_error_names_the_measurement_db_kind():
    # The registry mechanics are covered in registry_test.py; this pins the user-facing kind.
    with pytest.raises(KeyError, match=r"Unknown measurement DB 'Missing'"):
        resolve_measurement_db("Missing")
