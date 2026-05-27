"""PerceptionSelector: a TimeSeriesSelector leaf whose ``build(cache)``
reads ``object_tracks`` rows from the per-container cache instead of
``channels``, applies a row-level predicate, and returns ``Intervals``
that compose with scalar predicates via ``& | ~``.

The cache passed in is ``PerceptionCache`` — a ``KVSTimeSeriesCache``
extension that also carries an ``object_tracks_pdf`` and the
``container_bounds`` needed to make ``~ot.X`` well-defined.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T

import mda_query_engine.util as U
from mda_query_engine.analyze.metadata.tag_expression import TagExpression
from mda_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesSelector,
)
from mda_query_engine.analyze.query.solvers.key_value_store_solver import (
    KVSTimeSeriesCache,
)
from mda_query_engine.analyze.query.solvers.series_cache import SeriesCache
from mda_query_engine.model.series.intervals import Intervals


def is_track_scoped(expression) -> bool:
    """Return True iff the expression tree contains a ``PerceptionSelector``
    with ``track_scope=True``.

    Used by the solver to decide whether to drive a per-object inner loop,
    and by ``PerceptionEvent`` to validate that a predicate's perception
    leaves agree on the flag.
    """
    if not isinstance(expression, TimeSeriesExpression):
        return False
    for sel in expression.get_selectors():
        if isinstance(sel, PerceptionSelector) and sel.track_scope:
            return True
    return False


def perception_selectors_track_scope_values(expression) -> set[bool]:
    """Return the set of ``track_scope`` values across the expression's
    ``PerceptionSelector`` leaves. Empty if the expression has no perception
    leaves. ``{True, False}`` signals an illegal mixed predicate.
    """
    values: set[bool] = set()
    if not isinstance(expression, TimeSeriesExpression):
        return values
    for sel in expression.get_selectors():
        if isinstance(sel, PerceptionSelector):
            values.add(bool(sel.track_scope))
    return values


# Row-level predicate operators on object_tracks columns.
# Encoded as strings so the AST round-trips through ``as_dict()`` / ``from_dict()``.
_NUMERIC_OPS = {
    "eq": lambda s, v: s == v,
    "ne": lambda s, v: s != v,
    "lt": lambda s, v: s < v,
    "le": lambda s, v: s <= v,
    "gt": lambda s, v: s > v,
    "ge": lambda s, v: s >= v,
}
_STRING_OPS = {
    "eq": lambda s, v: s == v,
    "ne": lambda s, v: s != v,
    "contains": lambda s, v: s.fillna("").str.contains(v, regex=False),
}


def _merge_overlapping_intervals(
    tstarts: np.ndarray, tends: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    """Merge a set of possibly overlapping/nested intervals into a sorted
    disjoint set.  ``Intervals.merge_overlaps`` only handles the disjoint
    sorted case correctly; perception predicates can produce intervals
    from different ``object_id`` groups that overlap or nest, so the merge
    happens here before the ``Intervals`` is constructed.
    """
    if len(tstarts) == 0:
        return np.array([], dtype=np.float64), np.array([], dtype=np.float64)
    order = np.argsort(tstarts, kind="stable")
    sorted_starts = tstarts[order]
    sorted_ends = tends[order]
    merged_starts: list[float] = [float(sorted_starts[0])]
    merged_ends: list[float] = [float(sorted_ends[0])]
    for i in range(1, len(sorted_starts)):
        s, e = float(sorted_starts[i]), float(sorted_ends[i])
        if s <= merged_ends[-1]:
            merged_ends[-1] = max(merged_ends[-1], e)
        else:
            merged_starts.append(s)
            merged_ends.append(e)
    return np.array(merged_starts, dtype=np.float64), np.array(merged_ends, dtype=np.float64)


class PerceptionCache(KVSTimeSeriesCache):
    """Per-container cache carrying both ``channels`` and ``object_tracks``.

    The channel side of the cache is exactly the ``KVSTimeSeriesCache`` shape
    used by ``KeyValueStoreSolver._solve_udf``. The perception side adds
    ``object_tracks_pdf`` (per-frame per-object rows for this container) and
    ``container_bounds`` (``(t_min, t_max)``) so that ``Intervals.__invert__``
    has a well-defined complement window.
    """

    def __init__(
        self,
        channels_pdf: pd.DataFrame,
        col_map: dict[str, str],
        object_tracks_pdf: pd.DataFrame,
        container_bounds: tuple[float, float],
    ):
        super().__init__(channels_pdf, col_map)
        self.object_tracks_pdf = object_tracks_pdf
        self.container_bounds = container_bounds


class PerceptionSelector(TimeSeriesSelector):
    """Leaf node: ``ot.<column>(<value>)`` or ``ot.<column> <op> <value>``.

    Stores the row-level predicate as ``(column, op, value)`` so it round-trips
    through ``as_dict()`` / ``from_dict()`` when the AST is broadcast to
    pandas-UDF workers. ``build(cache)`` reads ``cache.object_tracks_pdf``,
    applies the predicate, and returns one interval per matched frame, with
    consecutive matches within the same ``object_id`` merged.

    ``get_selector_expr()`` returns ``F.lit(False)`` so a ``PerceptionSelector``
    never matches a ``channel_metrics`` row in the existing 6-stage filter
    pipeline.

    ``track_scope`` controls per-object windowing. When False (default),
    matching frames across all objects collapse into a single per-container
    interval set — today's behaviour from the foundation perception ADR.
    When True, the solver partitions matched frames by ``object_id`` so each
    object yields its own ``[tstart, tend]`` intervals; downstream events
    write a LakeVision side-car row carrying that ``object_id``.
    """

    def __init__(self, column: str, op: str, value: Any, track_scope: bool = False):
        self._column = column
        self._op = op
        self._value = value
        self._track_scope = bool(track_scope)
        # Stub TagExpression — the channels-side pipeline filters this out
        # because ``get_selector_expr()`` returns ``F.lit(False)``.
        super().__init__(_PerceptionPredicateTagStub(column, op, value))

    @property
    def column(self) -> str:
        return self._column

    @property
    def op(self) -> str:
        return self._op

    @property
    def value(self) -> Any:
        return self._value

    @property
    def track_scope(self) -> bool:
        return self._track_scope

    def dtype(self):
        return T.ArrayType(T.ArrayType(T.DoubleType()))

    def build(self, cache: SeriesCache) -> Intervals:
        if not isinstance(cache, PerceptionCache):
            return Intervals.empty()
        otp = cache.object_tracks_pdf
        if otp is None or len(otp) == 0:
            return Intervals(np.array([]), np.array([]), bounds=cache.container_bounds)

        # Sort the whole track table first so the "next frame in this object_id"
        # is computed before predicate masking. A row whose predicate matches
        # carries a window from its own frame_ts to the next frame_ts of the
        # same object_id (regardless of whether that next frame matches the
        # predicate) — without this, an object whose predicate matches in a
        # non-contiguous run would incorrectly span the gap.
        otp = otp.sort_values(["object_id", "frame_ts"]).reset_index(drop=True)
        next_ts_full = otp.groupby("object_id")["frame_ts"].shift(-1)

        series = otp[self._column]
        if self._op in _STRING_OPS and (
            series.dtype == object or pd.api.types.is_string_dtype(series)
        ):
            mask = _STRING_OPS[self._op](series, self._value)
        elif self._op in _NUMERIC_OPS:
            mask = _NUMERIC_OPS[self._op](series, self._value)
        else:
            raise ValueError(
                f"Unsupported predicate op={self._op!r} on column={self._column!r}"
            )
        if hasattr(mask, "fillna"):
            mask = mask.fillna(False)

        if not mask.any():
            return Intervals(np.array([]), np.array([]), bounds=cache.container_bounds)

        tstarts = otp.loc[mask, "frame_ts"].to_numpy(dtype=np.float64)
        # For the last frame of an object_id with no successor, fall back to
        # frame_ts + 1 (a one-microsecond window) so the interval is non-empty.
        next_ts_arr = next_ts_full[mask].to_numpy(dtype=np.float64)
        tail = tstarts + 1.0
        tends = np.where(np.isnan(next_ts_arr), tail, next_ts_arr)

        merged_starts, merged_ends = _merge_overlapping_intervals(tstarts, tends)
        return Intervals(
            merged_starts,
            merged_ends,
            bounds=cache.container_bounds,
        )

    def get_selector_expr(self):
        return F.lit(False)

    def get_required_tag_exprs(self) -> set[TagExpression]:
        return set()

    def required_tags(self) -> set[str]:
        return set()

    def get_selectors(self) -> list[TimeSeriesSelector]:
        return [self]

    def __str__(self) -> str:
        scope = ", track_scope=True" if self._track_scope else ""
        return f"PerceptionSelector<{self._column} {self._op} {self._value!r}{scope}>"

    def as_dict(self) -> dict[str, Any]:
        obj = {
            "alias": self._alias,
            "type": U.name_of(PerceptionSelector),
            "column": self._column,
            "op": self._op,
            "value": self._value,
            "track_scope": self._track_scope,
        }
        return obj

    @staticmethod
    def from_dict(obj: dict) -> "PerceptionSelector":
        sel = PerceptionSelector(
            obj["column"],
            obj["op"],
            obj["value"],
            track_scope=bool(obj.get("track_scope", False)),
        )
        if obj.get("alias"):
            sel.alias(obj["alias"])
        return sel


class _PerceptionPredicateTagStub(TagExpression):
    """Placeholder TagExpression so ``TimeSeriesSelector`` can hold a
    ``PerceptionSelector`` predicate without a real channel tag.

    The channels-side filter pipeline never reads this: ``get_selector_expr``
    on a ``PerceptionSelector`` short-circuits to ``F.lit(False)``.
    """

    def __init__(self, column: str, op: str, value: Any):
        self._column = column
        self._op = op
        self._value = value

    def required_tags(self) -> set[str]:
        return set()

    def get_selector_expr(self):
        return F.lit(False)

    def build_pandas(self, pdf):
        return pd.Series([False] * len(pdf), index=pdf.index)

    def as_dict(self) -> dict:
        return {
            "type": U.name_of(_PerceptionPredicateTagStub),
            "column": self._column,
            "op": self._op,
            "value": self._value,
        }

    @staticmethod
    def from_dict(obj: dict) -> "_PerceptionPredicateTagStub":
        return _PerceptionPredicateTagStub(obj["column"], obj["op"], obj["value"])

    def __str__(self) -> str:
        return f"{self._column} {self._op} {self._value!r}"

    def __hash__(self) -> int:
        return hash((self._column, self._op, repr(self._value)))

    def __eq__(self, other) -> bool:
        if not isinstance(other, _PerceptionPredicateTagStub):
            return False
        return (
            self._column == other._column
            and self._op == other._op
            and self._value == other._value
        )
