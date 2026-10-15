"""Generic walk over TSAL expression trees."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesAliasSelector,
    TimeSeriesExpression,
    TimeSeriesOp,
    TimeSeriesSelector,
)

from .pushdown import Pushdown, PushdownRule


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


def match_node(node: TimeSeriesExpression, rules: Sequence[PushdownRule]) -> Pushdown | None:
    """Return the first rule's pushdown for *node*, or ``None``.

    Only plain :class:`TimeSeriesOp` nodes can be pushed down: their ``build``
    serves precomputed results from the cache, other node types (UDFs,
    aggregations, events, …) would evaluate on missing raw rows instead.
    """
    if type(node) is not TimeSeriesOp:
        return None
    for rule in rules:
        pushdown = rule.match(node)
        if pushdown is not None:
            return pushdown
    return None


def discover(
    root: TimeSeriesExpression, rules: Sequence[PushdownRule]
) -> tuple[list[tuple[TimeSeriesExpression, Pushdown]], list[TimeSeriesSelector]]:
    """Find the maximal subtrees of *root* that a rule can push down.

    The walk does not descend into a matched subtree and keeps no visited set, so
    a node referenced twice is counted twice, like ``get_selectors()`` does.

    Returns
    -------
    tuple
        ``(candidates, leaves)``: the matched ``(node, pushdown)`` pairs and the
        selectors reached outside of them.
    """
    candidates: list[tuple[TimeSeriesExpression, Pushdown]] = []
    leaves: list[TimeSeriesSelector] = []
    stack: list[TimeSeriesExpression] = [root]
    while stack:
        node = stack.pop()
        pushdown = match_node(node, rules)
        if pushdown is not None:
            candidates.append((node, pushdown))
        elif isinstance(node, TimeSeriesSelector):
            leaves.append(node)
        else:
            stack.extend(child_expressions(node))
    return candidates, leaves
