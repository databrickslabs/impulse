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

Planning and decoding run in Python: :meth:`match` recognizes the subtree,


#### name

Unique rule name, part of every :attr:`Pushdown.variant_key`.


#### ordered

Whether the rule needs its series in order (neighbours, runs, series

facts).  Per-sample rules set ``False`` and skip the series scan.


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


#### runs

```python
def runs() -> Mapping[str, RunBreak]
```

Run definitions beyond the built-in continuous blocks, by name.

Each maps ``(prev, cur)`` samples to whether ``cur`` starts a new run;


#### row\_columns

```python
def row_columns(ctx: SeriesContext) -> Mapping[str, Column]
```

Columns computed for every sample, in order; refer to them via ``ctx.col``.


#### keep

```python
def keep(ctx: SeriesContext) -> Column
```

Whether a sample can emit rows; the others are dropped early.


#### kept\_columns

```python
def kept_columns(ctx: SeriesContext) -> Mapping[str, Column]
```

Columns computed for the kept samples, in order (``ctx.block_end`` available).


#### emit

```python
def emit(ctx: SeriesContext) -> Column
```

The derived rows of a kept sample.

**Returns**:

`Column`: An array of :data:`~.context.ELEMENT_TYPE` structs built with
:meth:`SeriesContext.element`; empty or null emits nothing.  Times
left out keep the sample's own ``tstart`` / ``tend``.

#### decode

```python
def decode(rows: pd.DataFrame, pushdown: Pushdown,
           cache: TimeSeriesCache) -> Any
```

Build the subtree's result from one container/channel/variant slice.


#### emits\_result\_rows

```python
def emits_result_rows(pushdown: Pushdown) -> bool
```

Whether *pushdown*'s derived rows are its result's samples as they are.

Holds when the result is a :class:`SampleSeries` built by the default


