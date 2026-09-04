---
sidebar_label: time_series_expression
title: impulse_query_engine.analyze.metadata.time_series_expression
---

## SeriesType

```python
class SeriesType(StrEnum)
```

Determines how a channel's values are interpreted:

``CONTINUOUS`` — the default; the channel is a continuous signal captured by sampling,
considered *valid* within each ``[tstart, tend)`` interval. Value v_i was measured at
tstart_i and no other value was measured until tend_i; the value between samples can
be reconstructed by interpolation.

``POINTS_IN_TIME`` — a time series of discrete events, valid *only at* their timestamps,
with no interpolation or validity in between.


## TimeSeriesSelector

```python
class TimeSeriesSelector(TimeSeriesExpression, RequiresDeserialization)
```

#### \_\_init\_\_

```python
def __init__(expr,
             uses_alias: bool = False,
             series_type: SeriesType = SeriesType.CONTINUOUS,
             value_type: SeriesValueType = SeriesValueType.DOUBLE)
```

Initialize a TimeSeriesSelector.

**Arguments**:

- `expr` (`TagExpression`): Tag expression to select.
- `uses_alias` (`bool`): Whether the channel resolves via the channel-alias table.
- `series_type` (`SeriesType`): Which object this selector builds. The default (``CONTINUOUS``)
builds a :class:`SampleSeries`; ``POINTS_IN_TIME`` builds a
:class:`PointsInTimeSeries`. Matching is identical; only the built
object and its result dtype differ. This is the plan-time source of
truth for the series type.
- `value_type` (`SeriesValueType`): For ``POINTS_IN_TIME``, the declared value data type
(``DOUBLE`` / ``STRING``); ignored otherwise. Drives plan-time typing
and string-op gating; validated against the silver
``poi_channels.dtype`` at solve time.

#### dtype

```python
def dtype() -> T.DataType
```

Returns the Spark data type.

**Returns**:

`pyspark.sql.types.DataType`: ``BinaryType`` when this selector builds a :class:`SampleSeries`
(a serialized blob). When it builds a :class:`PointsInTimeSeries`,
the value-type-aware ``PointsInTimeSeries.dtype()``
(``array<array<double>>`` for numeric,
``array<struct<tstart,value>>`` for string).

#### deserialize

```python
def deserialize(d)
```

Deserialize a :class:`SampleSeries` result after collection/toPandas.

A :class:`PointsInTimeSeries` result is serialized by ``get_data()``
(a plain ``[[t, v], ...]`` list) and needs no deserialization, so it is
returned as-is; only a :class:`SampleSeries` (binary) blob is decoded.

**Arguments**:

- `d` (`Any`): Data to deserialize.

**Returns**:

`SampleSeries or Any`: The decoded :class:`SampleSeries`, else *d* unchanged.

#### build

```python
def build(cache: SeriesCache) -> SampleSeries | PointsInTimeSeries
```

Instantiate the selected series from cache data.

Resolution is identical regardless of series type — resolve the matching
candidates, take the first ``(container_id, channel_id)``, and let the
cache build the right object.  The **data** is authoritative for the built
type: :meth:`TimeSeriesCache.load_blob` returns a


#### get\_required\_tag\_exprs

```python
def get_required_tag_exprs() -> set[TagExpression]
```

Get required tag expressions.

**Returns**:

`set of TagExpression`: Required tag expressions.

#### required\_tags

```python
def required_tags() -> set[str]
```

Get required tag keys.

**Returns**:

`set of str`: Required tag keys.

#### get\_selector\_expr

```python
def get_selector_expr()
```

Get selector expression.

**Returns**:

`Any`: Selector expression.

#### with\_alias

```python
def with_alias(*args)
```

Create an alias selector.

**Arguments**:

- `*args`: Aliases to use.

**Returns**:

`TimeSeriesAliasSelector`: Alias selector.

#### \_\_str\_\_

```python
def __str__()
```

String representation.

**Returns**:

`str`: String representation.

#### as\_dict

```python
def as_dict() -> dict[str, Any]
```

Dictionary representation.

**Returns**:

`dict`: Dictionary representation.

#### from\_dict

```python
def from_dict(obj: dict)
```

Construct from dictionary.

**Arguments**:

- `obj` (`dict`): Dictionary containing selector data.

**Returns**:

`TimeSeriesSelector`: Selector instance.

## TimeSeriesAliasSelector

```python
class TimeSeriesAliasSelector(TimeSeriesExpression)
```

#### \_\_init\_\_

```python
def __init__(*aliases)
```

Initialize a TimeSeriesAliasSelector.

**Arguments**:

- `*aliases` (`TimeSeriesSelector`): Aliases to select.

#### dtype

```python
def dtype() -> T.DataType
```

Returns the Spark data type.

**Returns**:

`pyspark.sql.types.DataType`: Data type (BinaryType).

#### build

```python
def build(cache: SeriesCache) -> SampleSeries | PointsInTimeSeries
```

Build the time series from cache.

**Arguments**:

- `cache` (`SeriesCache`): Cache containing time series data.

**Returns**:

`SampleSeries or PointsInTimeSeries`: Built series (aliased selectors build a :class:`SampleSeries` today).

#### get\_required\_tag\_exprs

```python
def get_required_tag_exprs() -> set[TagExpression]
```

Get required tag expressions.

**Returns**:

`set of TagExpression`: Required tag expressions.

#### required\_tags

```python
def required_tags() -> set[str]
```

Get required tag keys.

**Returns**:

`set of str`: Required tag keys.

#### get\_selector\_expr

```python
def get_selector_expr()
```

Get selector expression.

**Returns**:

`Any`: Selector expression.

#### \_\_str\_\_

```python
def __str__()
```

String representation.

**Returns**:

`str`: String representation.

## TimeSeriesOp

```python
class TimeSeriesOp(TimeSeriesExpression)
```

#### \_\_init\_\_

```python
def __init__(operation, optype, *args, **kwargs)
```

Initialize a TimeSeriesOp.

**Arguments**:

- `operation` (`callable`): The operation to apply.
- `optype` (`str`): Type of operation.
- `*args`: Arguments (like (TimeSeriesSelector<TagOp<eq(TagSelector<channel_name>,Vehicle Speed Sensor)>>, 1))
for the operation.
- `**kwargs`: Keyword arguments for the operation.

#### get\_required\_tag\_exprs

```python
def get_required_tag_exprs() -> set[TagExpression]
```

Get required tag expressions.

**Returns**:

`set of TagExpression`: Required tag expressions.

#### required\_tags

```python
def required_tags() -> set[str]
```

Get required tag keys.

**Returns**:

`set of str`: Required tag keys.

#### get\_selector\_expr

```python
def get_selector_expr()
```

Get selector expression.

**Returns**:

`Any`: Selector expression.

#### build

```python
def build(cache: SeriesCache)
```

Build the time series from cache.

**Arguments**:

- `cache` (`SeriesCache`): Cache containing time series data.

**Returns**:

`Any`: Built time series object.

#### \_\_str\_\_

```python
def __str__()
```

String representation.

**Returns**:

`str`: String representation.

#### as\_dict

```python
def as_dict() -> dict[str, Any]
```

Dictionary representation.

**Returns**:

`dict`: Dictionary representation.

#### from\_dict

```python
def from_dict(obj)
```

Construct from dictionary.

**Arguments**:

- `obj` (`dict`): Dictionary containing operation data.

**Returns**:

`TimeSeriesOp`: Operation instance.

## TimeSeriesUDF

```python
class TimeSeriesUDF(TimeSeriesOp)
```

#### \_\_init\_\_

```python
def __init__(func,
             *args,
             container_tags=None,
             container_metrics=None,
             **kwargs)
```

Initialize a TimeSeriesUDF.

**Arguments**:

- `func` (`callable`): The user-defined function to apply.
- `*args`: Arguments for the UDF.
- `container_tags` (`list of str`): Container-tag keys to inject into *func* as a ``container_tags``
keyword argument at build time (keyword-only; not treated as an
operand).
- `container_metrics` (`list of str`): Container-metric columns to inject into *func* as a
``container_metrics`` keyword argument at build time
(keyword-only; not treated as an operand).
- `**kwargs`: Keyword arguments for the UDF.

#### build

```python
def build(cache: SeriesCache)
```

Build the time series from cache using the UDF.

When the UDF declared ``container_tags`` / ``container_metrics``, the
requested values are resolved from *cache* and passed to *func* as
``container_tags`` / ``container_metrics`` keyword arguments (dicts
keyed by the declared names; missing values are ``None``).

**Arguments**:

- `cache` (`SeriesCache`): Cache containing time series data.

**Returns**:

`Any`: Result of applying the UDF to the built arguments.

#### \_\_str\_\_

```python
def __str__()
```

Return the string representation of the TimeSeriesUDF.

**Returns**:

`str`: String representation.

## CallableTimeSeriesExpression

```python
class CallableTimeSeriesExpression()
```

#### \_\_init\_\_

```python
def __init__(func, container_tags=None, container_metrics=None)
```

Initialize a CallableTimeSeriesExpression.

**Arguments**:

- `func` (`callable`): Function to wrap.
- `container_tags` (`list of str`): Container-tag keys forwarded to each :class:`TimeSeriesUDF` this
wrapper builds.
- `container_metrics` (`list of str`): Container-metric columns forwarded to each :class:`TimeSeriesUDF`
this wrapper builds.

#### \_\_call\_\_

```python
def __call__(*args, **kwargs)
```

Create a TimeSeriesUDF with the wrapped function.

**Arguments**:

- `*args`: Arguments for the function.
- `**kwargs`: Keyword arguments for the function.

**Returns**:

`TimeSeriesUDF`: UDF-wrapped expression.

