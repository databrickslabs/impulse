"""A dedicated predicate DSL for POI (point-of-interest) row filtering.

This replaces the earlier reuse of :class:`TagExpression` for POI. A POI table is a
**wide, natively-typed** table (``duration double``, ``occurrences int``, ``poi_type
varchar`` …), so — unlike the EAV tag tables whose ``value`` column is always a string —
POI predicates need **no caller-supplied cast**: the column type comes from the table.

The model mirrors :class:`MetricExpression` (the accessor for wide ``container_metrics``
columns), which is the right analogue for a wide typed table, rather than
:class:`TagExpression` (EAV). ``QueryBuilder.poi_metric("duration") > 5`` is to the POI
table what ``QueryBuilder.metric("duration_ms") > 5`` is to ``container_metrics``.

Two classes:

- :class:`PoiMetricSelector` — a reference to a POI column, produced by ``q.poi_metric``.
  Comparison operators on it build a :class:`PoiPredicate`.
- :class:`PoiPredicate` — the small predicate AST. It satisfies the two contracts the
  solver needs: :meth:`get_selector_expr` (compiles to a Spark ``Column`` so
  ``DefaultSolver.filter_poi`` can push it down) and a stable :meth:`__str__` (so a
  ``PoiSelector`` carrying it hashes to a stable ``selector_id`` / definition hash).
"""

from __future__ import annotations

import operator
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql.column import Column

# Operator → SQL-ish symbol, for a stable, readable ``__str__`` that feeds the definition
# hash. Kept explicit (not ``operator.__name__``) so the string form never drifts.
_OP_SYMBOL = {
    operator.eq: "==",
    operator.ne: "!=",
    operator.gt: ">",
    operator.ge: ">=",
    operator.lt: "<",
    operator.le: "<=",
    operator.and_: "&",
    operator.or_: "|",
}


class PoiPredicate:
    """A comparison/boolean predicate over POI columns.

    Built by comparing a :class:`PoiMetricSelector` (``q.poi_metric("duration") > 5``) and
    combined with ``&`` / ``|``. Consumed by ``DefaultSolver.filter_poi`` via
    :meth:`get_selector_expr`.
    """

    def __init__(self, op, left, right):
        self.op = op
        self.left = left
        self.right = right

    def get_selector_expr(self) -> Column:
        """Compile this predicate to a Spark ``Column`` for pushdown in ``filter_poi``.

        A :class:`PoiMetricSelector` operand becomes a (optionally cast) ``F.col``; a
        :class:`PoiPredicate` operand recurses; anything else is a literal.
        """
        return self.op(self._compile(self.left), self._compile(self.right))

    @staticmethod
    def _compile(operand):
        if isinstance(operand, PoiMetricSelector):
            return operand.column_expr()
        if isinstance(operand, PoiPredicate):
            return operand.get_selector_expr()
        return operand

    def required_columns(self) -> set[str]:
        """Return the set of POI column names this predicate references."""
        cols: set[str] = set()
        for operand in (self.left, self.right):
            if isinstance(operand, PoiMetricSelector):
                cols.add(operand.name)
            elif isinstance(operand, PoiPredicate):
                cols |= operand.required_columns()
        return cols

    def __and__(self, other: "PoiPredicate") -> "PoiPredicate":
        return PoiPredicate(operator.and_, self, other)

    def __or__(self, other: "PoiPredicate") -> "PoiPredicate":
        return PoiPredicate(operator.or_, self, other)

    def __str__(self) -> str:
        return f"({self._str(self.left)} {_OP_SYMBOL.get(self.op, '?')} {self._str(self.right)})"

    @staticmethod
    def _str(operand) -> str:
        return str(operand) if isinstance(operand, (PoiMetricSelector, PoiPredicate)) else repr(operand)

    def as_dict(self) -> dict[str, Any]:
        return {
            "type": "PoiPredicate",
            "op": _OP_SYMBOL.get(self.op, "?"),
            "left": self.left.as_dict() if hasattr(self.left, "as_dict") else self.left,
            "right": self.right.as_dict() if hasattr(self.right, "as_dict") else self.right,
        }


class PoiMetricSelector:
    """A reference to a single POI column, produced by ``QueryBuilder.poi_metric(name)``.

    Comparison operators build a :class:`PoiPredicate`. An optional ``cast_type`` is
    accepted for parity with ``q.metric``/``q.tag`` and for the rare case of a POI column
    whose physical type is not what the comparison needs, but it is **not required**: POI
    columns are natively typed, so ``q.poi_metric("duration") > 5`` compares numerically
    with no cast (unlike EAV tags, where the string ``value`` column forced a cast).

    Parameters
    ----------
    name : str
        The POI column name (after ``PoiConfig.column_name_mapping``).
    cast_type : str or None, optional
        Optional Spark type to cast the column to before comparison. Defaults to ``None``
        (use the column's native type).
    """

    def __init__(self, name: str, cast_type: str | None = None):
        self.name = name
        self.cast_type = cast_type

    def column_expr(self) -> Column:
        col = F.col(self.name)
        if self.cast_type is not None:
            col = col.cast(self.cast_type)
        return col

    def __eq__(self, other) -> PoiPredicate:
        return PoiPredicate(operator.eq, self, other)

    def __ne__(self, other) -> PoiPredicate:
        return PoiPredicate(operator.ne, self, other)

    def __gt__(self, other) -> PoiPredicate:
        return PoiPredicate(operator.gt, self, other)

    def __ge__(self, other) -> PoiPredicate:
        return PoiPredicate(operator.ge, self, other)

    def __lt__(self, other) -> PoiPredicate:
        return PoiPredicate(operator.lt, self, other)

    def __le__(self, other) -> PoiPredicate:
        return PoiPredicate(operator.le, self, other)

    __hash__ = None  # predicates are not hashable; PoiMetricSelector isn't either

    def __str__(self) -> str:
        cast = f", cast={self.cast_type}" if self.cast_type else ""
        return f"PoiMetricSelector<{self.name}{cast}>"

    def as_dict(self) -> dict[str, Any]:
        return {"type": "PoiMetricSelector", "name": self.name, "cast_type": self.cast_type}


def poi_kind_predicate(**kind_filters) -> PoiPredicate:
    """Build the ANDed equality predicate behind ``QueryBuilder.poi(**kind_filters)``.

    ``poi(poi_type="aeb", event_type="computed")`` becomes
    ``(poi_type == "aeb") & (event_type == "computed")`` as :class:`PoiPredicate` nodes.

    Parameters
    ----------
    **kind_filters : dict
        Column-name → value equality pairs identifying the POI rows.

    Returns
    -------
    PoiPredicate
        The combined predicate.

    Raises
    ------
    ValueError
        If no kind filter is given (a bare ``poi()`` would match the whole table).
    """
    pred: PoiPredicate | None = None
    for key, value in kind_filters.items():
        eq = PoiMetricSelector(key) == str(value)
        pred = eq if pred is None else (pred & eq)
    if pred is None:
        raise ValueError("poi(...) needs at least one kind filter, e.g. poi(poi_type='aeb')")
    return pred
