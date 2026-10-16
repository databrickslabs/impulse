from dataclasses import dataclass
from typing import TYPE_CHECKING

from .measurement_db_abstract import AbstractMeasurementDB

if TYPE_CHECKING:
    from .measurement_db import MeasurementDBConfig


@dataclass(frozen=True, slots=True)
class MeasurementDatabaseRegistration:
    db_cls: type[AbstractMeasurementDB]
    config_cls: type["MeasurementDBConfig"]


_REGISTRY: dict[str, MeasurementDatabaseRegistration] = {}


def register_measurement_db(name: str, config_cls: type["MeasurementDBConfig"]):
    """Register an ``AbstractMeasurementDB`` subclass under *name*, built with *config_cls*.

    Re-registering a name replaces the previous entry.
    """

    def decorator[T: type[AbstractMeasurementDB]](db_cls: T) -> T:
        if not issubclass(db_cls, AbstractMeasurementDB):
            raise TypeError(f"{db_cls!r} must subclass AbstractMeasurementDB")
        _REGISTRY[name] = MeasurementDatabaseRegistration(db_cls, config_cls)
        return db_cls

    return decorator


def resolve_measurement_db(name: str) -> MeasurementDatabaseRegistration:
    """Return the registration for *name*; raises ``KeyError`` listing the registered names."""
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(
            f"Unknown measurement DB {name!r}; registered: {sorted(_REGISTRY)}. "
            "Import the package that registers it before parsing the report config."
        ) from None
