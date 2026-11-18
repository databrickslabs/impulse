---
sidebar_label: plan
title: impulse_query_engine.analyze.query.optimizer.plan
---

The outcome of query optimization: which subtrees Spark computes ahead of the UDF.


## PushedNode

```python
class PushedNode()
```

A pushed-down node as the UDF sees it: the node, its variant, its rule's name and

the selectors the rule computes from.

The UDF receives these instead of the plan.  They are pickled together with the
selections, so :attr:`node` and :attr:`selectors` are the very objects inside them,
and the UDF finds the node by identity.  The rule is named, not shipped, and
resolved among the built-in rules (:func:`~.query_optimizer.builtin_rules`).


## PushdownPlan

```python
class PushdownPlan()
```

Pushdowns selected for one solve call; the UDF receives only :attr:`pushed_nodes`.

Variant ``0`` is the raw channel data; variants ``1..n`` are derived series,
one per distinct :attr:`Pushdown.variant_key`.

**Arguments**:

- `rules` (`tuple of PushdownRule`): Rules used to recognize pushed-down nodes during evaluation.
- `variant_ids` (`Mapping[tuple, int]`): Variant key to variant id.
- `pushdowns_by_variant` (`Mapping[int, Pushdown]`): Variant id to a representative pushdown (rule and parameters).
- `variants_by_selector` (`Mapping[int, tuple of int]`): Selector id to the variant ids derived from it.
- `raw_selector_ids` (`frozenset of int`): Selectors whose raw channel data is still required.
- `pushed_nodes` (`tuple of PushedNode`): The pushed-down nodes of the solve's selections, each once: what the UDF
receives.

#### build

```python
def build(
    rules: Iterable[PushdownRule],
    pushdowns: Iterable[Pushdown],
    selector_ids: Iterable[int],
    nodes: Iterable[tuple[TimeSeriesExpression, Pushdown]] = ()
) -> PushdownPlan
```

Assign variant ids to *pushdowns*, in discovery order.

**Arguments**:

- `rules` (`Iterable[PushdownRule]`): The optimizer's rules.
- `pushdowns` (`Iterable[Pushdown]`): Accepted pushdowns; each one's selector is pushed down entirely.
- `selector_ids` (`Iterable[int]`): Ids of all selectors in the solve.
- `nodes` (`Iterable of (TimeSeriesExpression, Pushdown)`): The accepted nodes of the selections and their pushdowns, among
*pushdowns*; a node occurring more than once is recorded once.

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

``(pushdown, variant)`` when the node object *node* is pushed down, else ``None``.

By identity: a structurally equal but separate node is not pushed down.


#### lookup

```python
def lookup(node: TimeSeriesExpression,
           classify: Classifier | None = None) -> tuple[Pushdown, int] | None
```

Return ``(pushdown, variant)`` when *node* is a pushed-down subtree.

Pass a *classify* shared by the lookups of one evaluation so that every
node is classified once.


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


