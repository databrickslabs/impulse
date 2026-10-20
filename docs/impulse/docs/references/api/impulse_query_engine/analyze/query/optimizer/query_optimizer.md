---
sidebar_label: query_optimizer
title: impulse_query_engine.analyze.query.optimizer.query_optimizer
---

Rule-based optimizer choosing the subtrees Spark computes ahead of the pandas UDF.


#### default\_rules

```python
def default_rules() -> tuple[PushdownRule, ...]
```

All built-in pushdown rules.


## QueryOptimizer

```python
class QueryOptimizer()
```

Plans which subtrees of a solve's selections are computed natively in Spark.

Nodes are classified bottom-up (:class:`~.traversal.Classifier`), and the
topmost classified nodes whose pushdown the channel data supports are the
candidates; a node that is not supported leaves its operands as candidates.

All selections of a solve share the raw data of their physical channels, so
a subtree is pushed down only when **every** occurrence of its input selector,
across all selections, sits inside a pushed-down subtree.  Then the raw rows
of that selector are no longer needed and are replaced by the (smaller)
derived rows.  Selectors used anywhere else keep their raw data, and their
subtrees are evaluated in pandas as before.

**Arguments**:

- `rules` (`Sequence[PushdownRule]`): Rules to apply, in priority order.  Defaults to :func:`default_rules`.

#### optimize

```python
def optimize(selections: Iterable[Any],
             context: PushdownContext) -> PushdownPlan
```

Build the pushdown plan for the *selections* of one solve call.

**Arguments**:

- `selections` (`Iterable`): The query's selections.
- `context` (`PushdownContext`): Plan-time facts about the channel data.

**Returns**:

`PushdownPlan`: The plan; empty when nothing can be pushed down.

