# pylint: disable=missing-function-docstring
"""Tests for the ``AbstractMeasurementDB`` contract and the name registry."""

from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

import impulse_query_engine.measurement_db_registry as registry
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


@pytest.fixture(autouse=True)
def isolated_registry(monkeypatch):
    monkeypatch.setattr(registry, "_REGISTRY", dict(registry._REGISTRY))


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


def test_register_returns_class_and_last_registration_wins():
    class First(MeasurementDB): ...

    class Second(MeasurementDB): ...

    assert register_measurement_db("Custom", MeasurementDBConfig)(First) is First
    register_measurement_db("Custom", MeasurementDBConfig)(Second)
    assert resolve_measurement_db("Custom").db_cls is Second


def test_unknown_name_lists_registered_names():
    # The error names the unknown DB, lists the registered names, and tells the caller how to
    # fix it (import the registering package before parsing) -- all three are user-facing guidance.
    with pytest.raises(
        KeyError,
        match=r"'Missing'.*\['MeasurementDB'\].*Import the package that registers it",
    ):
        resolve_measurement_db("Missing")
