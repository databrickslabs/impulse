---
sidebar_label: time_series_expression
title: impulse_query_engine.analyze.metadata.time_series_expression
---

## SeriesType

```python
class SeriesType(StrEnum)
```

How a channel's samples are interpreted (mirrors :class:`RawEncoder`).

``SAMPLE`` — the default; ``[tstart, tend)`` intervals over which the value is
*valid* (reconstructed by an interpolation method, zero-order hold today),
backed by :class:`SampleSeries`.

``POINTS_IN_TIME`` — ``(tᵢ, vᵢ)`` points valid *only at* their timestamps, no
between-point validity, backed by :class:`PointsInTimeSeries`.


## PoiValueType

```python
class PoiValueType(StrEnum)
```

The value data type of a POI channel — selects its ``poi_channels`` value

column and which in-memory :class:`PointsInTimeSeries` variant is built.

``DOUBLE`` — numeric points (``poi_channels.value_double``); the full
arithmetic / ordering / reduction operator set applies.

``STRING`` — string points (``poi_channels.value_string``, e.g. DTC codes);
only sampling and equality apply (see :class:`PointsInTimeSeries`).


## TimeSeriesSelector

```python
class TimeSeriesSelector(TimeSeriesExpression, RequiresDeserialization)
```

#### \_\_init\_\_

```python
def __init__(expr,
             uses_alias: bool = False,
             series_type: SeriesType = SeriesType.SAMPLE,
             value_type: PoiValueType = PoiValueType.DOUBLE)
```

Initialize a TimeSeriesSelector.

**Arguments**:

- `expr` (`TagExpression`): Tag expression to select.
- `uses_alias` (`bool`): Whether the channel resolves via the channel-alias table.
- `series_type` (`SeriesType`): How the selected channel's samples are interpreted.  ``SAMPLE``
(default) builds a :class:`SampleSeries` — today's behavior,
unchanged.  ``POINTS_IN_TIME`` builds a :class:`PointsInTimeSeries`
(values valid only at their timestamps); identification / matching is
identical, only the built object and its result dtype differ.  This is
the plan-time source of truth for the series type (so ``dtype()`` is
correct for a bare POI selection with no per-channel metadata lookup).
- `value_type` (`PoiValueType`): For a ``POINTS_IN_TIME`` selection, the declared value data type
(``DOUBLE`` / ``STRING``).  Ignored for ``SAMPLE``.  Drives plan-time
typing and string-op gating; validated against the silver
``poi_channels.dtype`` at solve time (assertion contract).

#### dtype

```python
def dtype()
```

Returns the Spark data type.

**Returns**:

`pyspark.sql.types.DataType`: ``BinaryType`` for a SAMPLE selection (serialized ``SampleSeries``),
or the value-type-aware ``PointsInTimeSeries.dtype()`` for a
POINTS_IN_TIME selection (``array<array<double>>`` for numeric,
``array<struct<tstart,value>>`` for string).

#### deserialize

```python
def deserialize(d)
```

Deserialize a SAMPLE result after collection/toPandas.

POINTS_IN_TIME results are serialized by ``get_data()`` (a plain
``[[t, v], ...]`` list) and need no deserialization, so they are returned
as-is; only a SAMPLE (binary) blob is decoded to a :class:`SampleSeries`.

**Arguments**:

- `d` (`Any`): Data to deserialize.

**Returns**:

`SampleSeries or Any`: Deserialized sample series (SAMPLE), else *d* unchanged.

#### build

```python
def build(cache: SeriesCache)
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
def dtype()
```

Returns the Spark data type.

**Returns**:

`pyspark.sql.types.DataType`: Data type (BinaryType).

#### build

```python
def build(cache: SeriesCache) -> SampleSeries
```

Build the time series from cache.

**Arguments**:

- `cache` (`SeriesCache`): Cache containing time series data.

**Returns**:

`SampleSeries`: Built sample series.

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
def __init__(func, *args, **kwargs)
```

Initialize a TimeSeriesUDF.

**Arguments**:

- `func` (`callable`): The user-defined function to apply.
- `*args`: Arguments for the UDF.
- `**kwargs`: Keyword arguments for the UDF.

#### build

```python
def build(cache: SeriesCache)
```

Build the time series from cache using the UDF.

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
def __init__(func)
```

Initialize a CallableTimeSeriesExpression.

**Arguments**:

- `func` (`callable`): Function to wrap.

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

