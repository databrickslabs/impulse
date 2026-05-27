"""ObjectTrackAccessor: the ``ot`` proxy used to author perception predicates.

Usage:

    ot = db.query.object_track
    cyclist_fl_close = (
        ot.detection_class("cyclist")
        & ot.azimuth("front_left")
        & (ot.distance_m < 8.0)
        & (ot.confidence > 0.7)
    )

Each call (``ot.detection_class("cyclist")``) returns a ``PerceptionSelector``
predicate on the named ``object_tracks`` column.  Comparison operators on
numeric columns (``ot.distance_m < 8.0``) likewise return predicates.

The accessor knows the ``object_tracks`` schema in
``src/mda_query_engine/perception/schema/scenario.py`` and rejects unknown columns.
"""

from __future__ import annotations

from typing import Any

import pyspark.sql.types as T

from mda_query_engine.perception.schema.scenario import OBJECT_TRACKS

from .perception_selector import PerceptionSelector


def _is_numeric(dtype: T.DataType) -> bool:
    return isinstance(
        dtype, (T.DoubleType, T.FloatType, T.IntegerType, T.LongType, T.ShortType)
    )


def _is_string(dtype: T.DataType) -> bool:
    return isinstance(dtype, T.StringType)


_SCHEMA = {field.name: field.dataType for field in OBJECT_TRACKS.fields}
_NUMERIC_COLUMNS = {name for name, dtype in _SCHEMA.items() if _is_numeric(dtype)}
_STRING_COLUMNS = {name for name, dtype in _SCHEMA.items() if _is_string(dtype)}


class _NumericColumn:
    """Numeric column proxy — supports comparison operators that build a
    ``PerceptionSelector``.
    """

    def __init__(self, name: str, track_scope: bool = False):
        self._name = name
        self._track_scope = track_scope

    def __eq__(self, other: Any) -> PerceptionSelector:
        return PerceptionSelector(self._name, "eq", other, track_scope=self._track_scope)

    def __ne__(self, other: Any) -> PerceptionSelector:
        return PerceptionSelector(self._name, "ne", other, track_scope=self._track_scope)

    def __lt__(self, other: Any) -> PerceptionSelector:
        return PerceptionSelector(self._name, "lt", other, track_scope=self._track_scope)

    def __le__(self, other: Any) -> PerceptionSelector:
        return PerceptionSelector(self._name, "le", other, track_scope=self._track_scope)

    def __gt__(self, other: Any) -> PerceptionSelector:
        return PerceptionSelector(self._name, "gt", other, track_scope=self._track_scope)

    def __ge__(self, other: Any) -> PerceptionSelector:
        return PerceptionSelector(self._name, "ge", other, track_scope=self._track_scope)

    def __hash__(self) -> int:
        return hash(self._name)


class ObjectTrackAccessor:
    """Attribute proxy for authoring ``object_tracks`` predicates.

    String columns expose a callable form: ``ot.detection_class("cyclist")``
    produces ``PerceptionSelector("detection_class", "eq", "cyclist")``.
    Numeric columns expose a comparable proxy:
    ``ot.distance_m < 8.0`` produces
    ``PerceptionSelector("distance_m", "lt", 8.0)``.

    A ``source_contains(substring)`` helper is exposed because ``source``
    is a pipe-delimited multi-sensor provenance string and substring match
    is the natural query.

    ``ot(track_scope=True)`` returns a new accessor that taints every
    selector it produces with ``track_scope=True``. Selectors built from
    that accessor instance form per-object windows in the solver.
    """

    def __init__(self, track_scope: bool = False):
        self._track_scope = bool(track_scope)

    def __call__(self, *, track_scope: bool = False) -> "ObjectTrackAccessor":
        return ObjectTrackAccessor(track_scope=track_scope)

    def __getattr__(self, name: str) -> Any:
        # ``__getattr__`` is only consulted for missing attributes; the explicit
        # ``_track_scope`` set in ``__init__`` shields the flag from this branch.
        if name in _STRING_COLUMNS:
            return _StringPredicateBuilder(name, track_scope=self._track_scope)
        if name in _NUMERIC_COLUMNS:
            return _NumericColumn(name, track_scope=self._track_scope)
        if name == "source_contains":
            return lambda value: PerceptionSelector(
                "source", "contains", value, track_scope=self._track_scope
            )
        if name == "detection_class_contains":
            return lambda value: PerceptionSelector(
                "detection_class", "contains", value, track_scope=self._track_scope
            )
        raise AttributeError(
            f"ObjectTrackAccessor has no column or helper named {name!r}. "
            f"Known columns: {sorted(_SCHEMA.keys())}."
        )


class _StringPredicateBuilder:
    """Callable proxy for string columns — ``ot.detection_class("cyclist")``."""

    def __init__(self, name: str, track_scope: bool = False):
        self._name = name
        self._track_scope = track_scope

    def __call__(self, value: Any) -> PerceptionSelector:
        return PerceptionSelector(self._name, "eq", value, track_scope=self._track_scope)

    def __eq__(self, other: Any) -> PerceptionSelector:
        return PerceptionSelector(self._name, "eq", other, track_scope=self._track_scope)

    def __ne__(self, other: Any) -> PerceptionSelector:
        return PerceptionSelector(self._name, "ne", other, track_scope=self._track_scope)

    def __hash__(self) -> int:
        return hash(self._name)
