# pylint: disable=missing-function-docstring
"""Tests for the ``AbstractMeasurementDB`` contract and the name registry."""

import pytest

import impulse_query_engine.measurement_db_registry as registry
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
    assert issubclass(MeasurementDB, AbstractMeasurementDB)
    assert not MeasurementDB.__abstractmethods__


def test_incomplete_implementation_cannot_be_instantiated():
    class Incomplete(AbstractMeasurementDB):
        pass

    with pytest.raises(TypeError, match="abstract"):
        Incomplete(MeasurementDBConfig(), ws=None)


def test_builtin_is_registered():
    assert resolve_measurement_db("MeasurementDB") == (MeasurementDB, MeasurementDBConfig)


def test_unknown_name_lists_registered_names():
    with pytest.raises(KeyError, match=r"'Missing'.*\['MeasurementDB'\]"):
        resolve_measurement_db("Missing")


def test_register_returns_class_and_last_registration_wins():
    class First(MeasurementDB): ...

    class Second(MeasurementDB): ...

    assert register_measurement_db("Custom", MeasurementDBConfig)(First) is First
    register_measurement_db("Custom", MeasurementDBConfig)(Second)
    assert resolve_measurement_db("Custom").db_cls is Second


def test_register_rejects_non_subclass():
    with pytest.raises(TypeError, match="must subclass AbstractMeasurementDB"):
        register_measurement_db("Bad", MeasurementDBConfig)(object)
