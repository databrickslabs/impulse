"""Bottom-up classification of TSAL expression trees into pushdowns."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence
from typing import Any

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesAliasSelector,
    TimeSeriesExpression,
    TimeSeriesOp,
    TimeSeriesSelector,
)

from .pushdown import Pushdown, PushdownRule

#: Plan-time check whether a classified node's pushdown is planned.
Accept = Callable[[TimeSeriesExpression, Pushdown], bool]


def child_expressions(node: TimeSeriesExpression) -> Iterator[TimeSeriesExpression]:
    """Yield the direct sub-expressions of *node*.

    Operations yield their operands; other node types (aggregations, events,
    calculated channels, …) yield every expression held in their attributes,
    including inside lists, tuples and dicts.  The walk is best-effort: the
    optimizer checks the leaves it finds against ``get_selectors()``.
    """
    if isinstance(node, TimeSeriesSelector):
        return
    if isinstance(node, TimeSeriesOp):
        # Covers TimeSeriesUDF, whose ``optype`` attribute holds its first operand.
        values: Iterable = (*node.args, *node.kwargs.values())
    elif isinstance(node, TimeSeriesAliasSelector):
        values = node._aliases
    else:
        values = (value for key, value in vars(node).items() if key != "_alias")
    yield from _expressions_in(values)


def _expressions_in(values: Iterable) -> Iterator[TimeSeriesExpression]:
    for value in values:
        if isinstance(value, TimeSeriesExpression):
            yield value
        elif isinstance(value, list | tuple):
            yield from _expressions_in(value)
        elif isinstance(value, dict):
            yield from _expressions_in(value.values())


class Classifier:
    """Classifies expression nodes bottom-up into the pushdowns computing them.

    A node is classified after its operands: the first rule whose
    :meth:`~.pushdown.PushdownRule.match` returns a pushdown for the node, given
    its operands' classification, computes it.  Only plain :class:`TimeSeriesOp`
    nodes can be pushed down: their ``build`` serves precomputed results from the
    cache, other node types (UDFs, aggregations, events, …) would evaluate on
    missing raw rows instead.

    Classification is syntactic, so the driver and the UDF classify alike.
    Results are memoized per node object: use one classifier per optimization or
    per container, and never store it on a plan or a node.

    Parameters
    ----------
    rules : Sequence[PushdownRule]
        Rules in priority order.
    """

    def __init__(self, rules: Sequence[PushdownRule]):
        self.rules = tuple(rules)
        # Nodes are not hashable; holding each node keeps its id from being reused.
        self._memo: dict[int, tuple[TimeSeriesOp, Pushdown | None]] = {}

    def __call__(self, node: TimeSeriesExpression) -> Pushdown | None:
        """The pushdown computing *node*, or ``None``."""
        if type(node) is not TimeSeriesOp:
            return None
        stack: list[tuple[TimeSeriesOp, bool]] = [(node, False)]
        while stack:
            current, operands_classified = stack.pop()
            if id(current) in self._memo:
                continue
            if operands_classified:
                self._memo[id(current)] = (current, self._match(current))
            else:
                stack.append((current, True))
                stack.extend(
                    (child, False)
                    for child in child_expressions(current)
                    if type(child) is TimeSeriesOp
                )
        return self._memo[id(node)][1]

    def operand(self, value: Any) -> TimeSeriesSelector | Pushdown | None:
        """Classification of an operand: its selector for a channel, else its pushdown."""
        if isinstance(value, TimeSeriesSelector):
            return value
        if isinstance(value, TimeSeriesExpression):
            return self(value)
        return None

    def _match(self, node: TimeSeriesOp) -> Pushdown | None:
        for rule in self.rules:
            pushdown = rule.match(node, self.operand)
            if pushdown is not None:
                return pushdown
        return None


def discover(
    root: TimeSeriesExpression, classify: Classifier, accept: Accept | None = None
) -> tuple[list[tuple[TimeSeriesExpression, Pushdown]], list[TimeSeriesSelector]]:
    """Find the topmost subtrees of *root* that are pushed down.

    A classified node is a candidate when *accept* (by default every pushdown)
    accepts its pushdown; otherwise the walk continues into its operands, whose
    pushdowns can still be candidates.  The walk does not descend into a
    candidate and keeps no visited set, so a node referenced twice is counted
    twice, like ``get_selectors()`` does.

    Returns
    -------
    tuple
        ``(candidates, leaves)``: the ``(node, pushdown)`` candidates and the
        selectors reached outside of them.
    """
    candidates: list[tuple[TimeSeriesExpression, Pushdown]] = []
    leaves: list[TimeSeriesSelector] = []
    stack: list[TimeSeriesExpression] = [root]
    while stack:
        node = stack.pop()
        pushdown = classify(node)
        if pushdown is not None and (accept is None or accept(node, pushdown)):
            candidates.append((node, pushdown))
        elif isinstance(node, TimeSeriesSelector):
            leaves.append(node)
        else:
            stack.extend(child_expressions(node))
    return candidates, leaves
