from typing import TYPE_CHECKING

from .measurement_db_abstract import AbstractMeasurementDB
from .registry import Registration, Registry

if TYPE_CHECKING:
    from .measurement_db import MeasurementDBConfig

# No config_base: importing MeasurementDBConfig here would be circular (measurement_db.py
# imports this module to register the built-in DB).
_REGISTRY: Registry[AbstractMeasurementDB, "MeasurementDBConfig"] = Registry(
    "measurement DB", AbstractMeasurementDB
)


def register_measurement_db(name: str, config_cls: type["MeasurementDBConfig"]):
    """Register an ``AbstractMeasurementDB`` subclass under *name*, built with *config_cls*.

    Re-registering a name replaces the previous entry.
    """
    return _REGISTRY.register(name, config_cls)


def resolve_measurement_db(
    name: str,
) -> Registration[AbstractMeasurementDB, "MeasurementDBConfig"]:
    """Return the registration for *name*; raises ``KeyError`` listing the registered names."""
    return _REGISTRY.resolve(name)
