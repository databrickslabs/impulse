---
sidebar_label: plan
title: impulse_query_engine.analyze.query.optimizer.plan
---

The outcome of query optimization: which subtrees Spark computes ahead of the UDF.


## PushedNode

```python
class PushedNode()
```

A pushed-down node as the UDF receives it.

Pickled with the selections, so :attr:`node` and :attr:`selectors` (the selectors
the rule computes from) are the objects inside them; the UDF finds the node by
identity.  :attr:`rule` names a built-in rule (:func:`~.query_optimizer.builtin_rules`).


## PushdownPlan

```python
class PushdownPlan()
```

Pushdowns selected for one solve call; the UDF receives only :attr:`pushed_nodes`.

Variant ``0`` is the raw channel data; variants ``1..n`` are derived series,
one per distinct :attr:`Pushdown.variant_key`.

**Arguments**:

- `rules` (`tuple of PushdownRule`): The optimizer's rules.
- `variant_ids` (`Mapping[tuple, int]`): Variant key to variant id.
- `pushdowns_by_variant` (`Mapping[int, Pushdown]`): Variant id to a representative pushdown (rule and parameters).
- `variants_by_selector` (`Mapping[int, tuple of int]`): Selector id to the variant ids derived from it.
- `raw_selector_ids` (`frozenset of int`): Selectors whose raw channel data is still required.
- `pushed_nodes` (`tuple of PushedNode`): Each pushed-down node of the selections once; what the UDF receives.

#### build

```python
def build(rules: Iterable[PushdownRule],
          nodes: Iterable[tuple[TimeSeriesExpression, Pushdown]],
          selector_ids: Iterable[int]) -> PushdownPlan
```

Assign variant ids to the pushdowns of *nodes*, in discovery order.

**Arguments**:

- `rules` (`Iterable[PushdownRule]`): The optimizer's rules.
- `nodes` (`Iterable of (TimeSeriesExpression, Pushdown)`): Accepted ``(node, pushdown)`` pairs; each pushdown's selector is pushed
down entirely.  A repeated node is recorded once.
- `selector_ids` (`Iterable[int]`): Ids of all selectors in the solve.

**Raises**:

- `ValueError`: If a pushdown computes on another pushdown's result (``source``),
which the pipeline cannot compute.

#### is\_empty

```python
def is_empty() -> bool
```

``True`` when nothing is pushed down.


#### pushed\_selector\_ids

```python
def pushed_selector_ids() -> frozenset[int]
```

Selectors whose raw channel data is replaced by derived variants.


#### variants\_by\_rule

```python
def variants_by_rule() -> list[tuple[PushdownRule, dict[int, Pushdown]]]
```

Group the variants by the rule that computes them.

**Raises**:

- `ValueError`: If two different rules share a name: their pipeline columns would
collide.

#### variant\_of

```python
def variant_of(node: TimeSeriesExpression) -> tuple[Pushdown, int] | None
```

``(pushdown, variant)`` if the node object *node* is pushed down, else ``None``.

Matched by identity, not structure.


#### native\_targets

```python
def native_targets(
    selections: Iterable[Any]
) -> dict[int, dict[int, list[tuple[int, bool]]]] | None
```

Sources of calculated channels that are pushed down as a whole.

**Arguments**:

- `selections` (`Iterable`): The selections of a ``solve_calculated_channels`` call.

**Returns**:

`dict or None`: ``{variant: {selector_id: [(channel_id, uses_alias), ...]}}``: the
calculated channels (by output ``channel_id``) whose rows are the
given variant's rows of the channel resolved for ``selector_id``.
``None`` unless every selection is a ``CalculatedChannel`` whose
whole expression is a pushdown emitting its result rows.

#### channel\_variants

```python
def channel_variants(selector_ids: Column) -> Column
```

Variant ids a matched channel ships, given its ``selector_ids`` array.

A channel ships only its raw data (``[0]``) unless every one of its
selectors is pushed down; then it ships the derived variants instead.


#### channel\_selector\_ids

```python
def channel_selector_ids(selector_ids: Column | str) -> Column
```

A matched channel's distinct, non-null ``selector_ids``, as ``long``.

Variant tagging (:meth:`channel_variants`) and the native calculated-channel
rows both read a channel's selectors through this, so they agree on which
selectors it carries.


#### describe

```python
def describe() -> str
```

One-line summary of the plan for logging.


