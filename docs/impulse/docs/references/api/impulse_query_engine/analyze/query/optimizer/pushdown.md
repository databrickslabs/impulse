---
sidebar_label: pushdown
title: impulse_query_engine.analyze.query.optimizer.pushdown
---

Building blocks of Spark pushdown rules.


#### VARIANT\_COL

Column tagging each channel-data row with the series variant it belongs to.


#### RAW\_VARIANT

Variant id of the unmodified (raw) channel data.


## PushdownContext

```python
class PushdownContext()
```

Plan-time facts about the channel data a pushdown would run on.

**Arguments**:

- `tstart_type, tend_type` (`pyspark.sql.types.DataType`): Spark types of the channel-data ``tstart`` / ``tend`` columns.

#### from\_schema

```python
def from_schema(schema: T.StructType, config: SolverConfig) -> PushdownContext
```

Build the context from the column-mapped channel-data schema.


## Pushdown

```python
class Pushdown()
```

A subtree that a :class:`PushdownRule` computes natively in Spark.

Expression nodes overload ``==``, so pushdowns compare by identity; use
:attr:`variant_key` to identify equivalent pushdowns.

**Arguments**:

- `rule` (`PushdownRule`): The rule that matched the subtree.
- `selector` (`TimeSeriesSelector`): The physical channel selector the subtree reads.
- `params` (`tuple of (str, Any)`): Canonical, hashable operation parameters (e.g. ``(("sample_rate", 1e6),)``).
- `consumed` (`tuple of TimeSeriesSelector`): Every selector occurrence inside the subtree (its ``get_selectors()``).

#### variant\_key

```python
def variant_key() -> tuple
```

Identity of the derived series, independent of the input channel.


#### describe

```python
def describe() -> str
```

Human-readable ``rule(param=value, ...)`` label.


## PushdownRule

```python
class PushdownRule(abc.ABC)
```

Recognizes a subtree and computes it in Spark ahead of the pandas UDF.

A rule's derived rows use the channel-data shape
``(container_id, channel_id, variant, tstart, tend, value)``; :meth:`decode`
turns one container/channel/variant slice of them back into the object the
original subtree evaluates to.  The result must equal evaluating the subtree
in pandas on the same channel data.


#### name

Unique rule name, part of every :attr:`Pushdown.variant_key`.


#### match

```python
def match(node: TimeSeriesExpression) -> Pushdown | None
```

Return the pushdown for *node*, or ``None`` if the rule does not apply.

Purely syntactic: it runs on the driver while planning and again inside
the UDF to recognize the node, so it must not depend on data or schemas.


#### supports

```python
def supports(pushdown: Pushdown, context: PushdownContext) -> bool
```

Whether *pushdown* can run on channel data described by *context*.


#### result\_type

```python
def result_type(pushdown: Pushdown) -> type
```

Core-model class :meth:`decode` returns for *pushdown*.


#### to\_spark

```python
def to_spark(rows: DataFrame, variants: Mapping[int, Pushdown],
             config: SolverConfig) -> DataFrame
```

Compute all *variants* of this rule in one pass.

**Arguments**:

- `rows` (`pyspark.sql.DataFrame`): Channel-data rows tagged with :data:`VARIANT_COL`, one copy per
variant a channel needs; only ids in *variants* occur.
- `variants` (`Mapping[int, Pushdown]`): Variant id to a representative pushdown carrying its parameters.
- `config` (`SolverConfig`): Provides the internal column names.

**Returns**:

`pyspark.sql.DataFrame`: Derived rows with the container id, channel id, :data:`VARIANT_COL`,
``tstart``, ``tend`` and ``value`` columns of *rows*, same types.

#### decode

```python
def decode(rows: pd.DataFrame, pushdown: Pushdown,
           cache: TimeSeriesCache) -> Any
```

Build the subtree's result from one container/channel/variant slice.


