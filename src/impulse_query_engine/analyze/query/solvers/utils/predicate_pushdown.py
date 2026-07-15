"""Value-predicate extraction from :class:`TimeSeriesExpression` ASTs.

Comparisons of a single physical channel against a numeric scalar (e.g.
``channel > 2000``) evaluate to :class:`Intervals` built **only** from the
samples where the comparison holds (see ``SampleSeries.__apply_op``).  Rows
failing the comparison therefore contribute nothing to the result and can be
filtered out of the channels table before the solve shuffle and the
grouped-map UDF.

This module computes, per ``selector_id``, a *needed-rows predicate*: rows
failing it provably cannot change any selection's result.  The analysis is a
whitelist -- anything unrecognized degrades to "all rows needed":

- ``selector <cmp> scalar`` (``>``, ``>=``, ``<``, ``<=``, ``==``) is *exact*:
  its Intervals are exactly the merged spans of predicate-true rows.
- ``&``/``|`` of two exact nodes on the **same** selector compose exactly to
  the conjunction/disjunction of their predicates.
- Interval-shaping operations (``merge_intervals``, ``debounce``, ...) keep
  the needed rows but destroy exactness: gap-filled intervals may overlap
  spans of predicate-false rows, so later same-selector combinations must OR
  the operands' needs (always sound) instead of AND-refining.
- Everything else -- raw series usage, aggregations, UDFs, ``where``,
  arithmetic, alias selectors, unit-converted (``uses_alias``) selectors --
  needs all rows of every selector it references.

Because all selections of a query share one scan and one per-container cache,
the per-selector predicates of all selections are OR-merged: a row may only
be dropped when it is dead for *every* usage in the query.

``!=`` is deliberately not extracted: for a NULL sample value Spark evaluates
``value != x`` to NULL and would drop a row that the pandas path (NULL ->
NaN, ``NaN != x`` -> True) still produces. NaN thresholds are rejected for
the same reason (Spark treats ``NaN = NaN`` as true, numpy as false).
"""

from __future__ import annotations

import abc
import math
import operator
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import Column

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
    TimeSeriesOp,
    TimeSeriesSelector,
)

#: Sentinel: every row of the selector's channel may be needed (no filter).
ALL_ROWS = None


class ValuePredicate(abc.ABC):
    """A Spark-independent predicate over the sample ``value`` column."""

    @abc.abstractmethod
    def to_spark_column(self, value_col: Column) -> Column:
        """
        Render the predicate as a Spark boolean column.

        Parameters
        ----------
        value_col : Column
            Column holding the sample values.

        Returns
        -------
        Column
            Boolean column that is True for rows satisfying the predicate.
        """


@dataclass(frozen=True)
class ValueComparison(ValuePredicate):
    """``value <op> threshold`` with op in {gt, ge, lt, le, eq}."""

    op: str
    threshold: float

    _OPS = {
        "gt": operator.gt,
        "ge": operator.ge,
        "lt": operator.lt,
        "le": operator.le,
        "eq": operator.eq,
    }

    def to_spark_column(self, value_col: Column) -> Column:
        # Cast mirrors the float64 coercion applied by SampleSeries.
        return self._OPS[self.op](value_col.cast("double"), F.lit(self.threshold))


@dataclass(frozen=True)
class ValueAnd(ValuePredicate):
    left: ValuePredicate
    right: ValuePredicate

    def to_spark_column(self, value_col: Column) -> Column:
        return self.left.to_spark_column(value_col) & self.right.to_spark_column(value_col)


@dataclass(frozen=True)
class ValueOr(ValuePredicate):
    left: ValuePredicate
    right: ValuePredicate

    def to_spark_column(self, value_col: Column) -> Column:
        return self.left.to_spark_column(value_col) | self.right.to_spark_column(value_col)


_COMPARISON_OPS = {
    operator.gt: "gt",
    operator.ge: "ge",
    operator.lt: "lt",
    operator.le: "le",
    operator.eq: "eq",
}
#: Comparison to use when the scalar is the left operand (``2000 < ch``).
_MIRRORED_OPS = {"gt": "lt", "ge": "le", "lt": "gt", "le": "ge", "eq": "eq"}
_BOOL_OPS = frozenset({operator.and_, operator.or_})
#: Pure functions of an interval set: they preserve which rows are needed but
#: break the rows <-> intervals correspondence (exactness).
_INTERVAL_SHAPING_OPS = frozenset(
    {
        "merge_overlaps",
        "merge_intervals",
        "debounce",
        "filter",
        "expand",
        "expand_left",
        "expand_right",
        "shrink",
        "shrink_left",
        "shrink_right",
        "starts",
        "ends",
        "start_points",
        "end_points",
    }
)


@dataclass(frozen=True)
class _NodeAnalysis:
    """Per-node result: needed rows per selector_id, plus exactness."""

    needed: dict[int, ValuePredicate | None]
    #: ``(selector_id, predicate)`` when the node's Intervals are exactly the
    #: merged spans of predicate-true rows of that single selector.
    exact: tuple[int, ValuePredicate] | None


def analyze_selections(selections: Iterable[Any]) -> dict[int, ValuePredicate | None] | None:
    """
    Compute per-selector needed-rows predicates for a whole query.

    Parameters
    ----------
    selections : Iterable
        The query's selection expressions.

    Returns
    -------
    dict or None
        Mapping ``selector_id -> ValuePredicate | ALL_ROWS``, OR-merged over
        all selections.  ``ALL_ROWS`` means the selector needs an unfiltered
        channel.  Returns ``None`` when any selection is not a
        ``TimeSeriesExpression`` -- such selections may share channels in
        ways this analysis cannot enumerate, so pushdown must be disabled
        for the query.
    """
    merged: dict[int, ValuePredicate | None] = {}
    for selection in selections:
        if not isinstance(selection, TimeSeriesExpression):
            return None
        merged = _merge_or(merged, _analyze_node(selection).needed)
    return merged


def _analyze_node(node: TimeSeriesExpression) -> _NodeAnalysis:
    # Exact type checks: subclasses (e.g. TimeSeriesUDF) have different
    # build() semantics and must fall through to the opaque default.
    if type(node) is TimeSeriesOp:
        if node.optype == "builtin" and len(node.args) == 2 and not node.kwargs:
            cmp_op = _COMPARISON_OPS.get(node.operation) if callable(node.operation) else None
            if cmp_op is not None:
                return _analyze_comparison(node, cmp_op)
            if node.operation in _BOOL_OPS:
                return _analyze_bool_op(node)
        if node.optype == "cls" and node.operation in _INTERVAL_SHAPING_OPS and node.args:
            operand, extra = node.args[0], node.args[1:]
            if isinstance(operand, TimeSeriesExpression) and not any(
                isinstance(a, TimeSeriesExpression) for a in (*extra, *node.kwargs.values())
            ):
                return _NodeAnalysis(needed=_analyze_node(operand).needed, exact=None)
    return _opaque(node)


def _analyze_comparison(node: TimeSeriesOp, cmp_op: str) -> _NodeAnalysis:
    left, right = node.args
    if _is_bare_selector(left) and _is_supported_scalar(right):
        selector, threshold = left, right
    elif _is_supported_scalar(left) and _is_bare_selector(right):
        selector, threshold = right, left
        cmp_op = _MIRRORED_OPS[cmp_op]
    else:
        return _opaque(node)
    pred = ValueComparison(cmp_op, float(threshold))
    return _NodeAnalysis(needed={selector.selector_id: pred}, exact=(selector.selector_id, pred))


def _analyze_bool_op(node: TimeSeriesOp) -> _NodeAnalysis:
    left, right = node.args
    if not (isinstance(left, TimeSeriesExpression) and isinstance(right, TimeSeriesExpression)):
        return _opaque(node)
    left_a, right_a = _analyze_node(left), _analyze_node(right)
    if (
        left_a.exact is not None
        and right_a.exact is not None
        and left_a.exact[0] == right_a.exact[0]
    ):
        # Same-selector combination of exact nodes stays exact.
        combine = ValueAnd if node.operation is operator.and_ else ValueOr
        selector_id = left_a.exact[0]
        pred = combine(left_a.exact[1], right_a.exact[1])
        return _NodeAnalysis(needed={selector_id: pred}, exact=(selector_id, pred))
    return _NodeAnalysis(needed=_merge_or(left_a.needed, right_a.needed), exact=None)


def _opaque(node: TimeSeriesExpression) -> _NodeAnalysis:
    """Fallback: every referenced selector needs all of its rows."""
    return _NodeAnalysis(
        needed={s.selector_id: ALL_ROWS for s in node.get_selectors()}, exact=None
    )


def _merge_or(
    a: dict[int, ValuePredicate | None], b: dict[int, ValuePredicate | None]
) -> dict[int, ValuePredicate | None]:
    """OR two needed-rows maps: a row is needed when either side needs it."""
    merged = dict(a)
    for selector_id, pred in b.items():
        if selector_id not in merged:
            merged[selector_id] = pred
        elif merged[selector_id] is ALL_ROWS or pred is ALL_ROWS:
            merged[selector_id] = ALL_ROWS
        else:
            merged[selector_id] = ValueOr(merged[selector_id], pred)
    return merged


def _is_bare_selector(arg: Any) -> bool:
    # uses_alias selectors receive unit-converted values in the cache; the
    # raw table values differ, so no predicate may be derived from them.
    return type(arg) is TimeSeriesSelector and not arg.uses_alias


def _is_supported_scalar(arg: Any) -> bool:
    if isinstance(arg, (bool, np.bool_, int, np.integer)):
        return True
    if isinstance(arg, (float, np.floating)):
        return not math.isnan(float(arg))
    return False
