---
sidebar_label: time_series_expression
title: impulse_query_engine.analyze.metadata.time_series_expression
---

## TimeSeriesSelector

```python
class TimeSeriesSelector(TimeSeriesExpression, RequiresDeserialization)
```

#### \_\_init\_\_

```python
def __init__(expr, uses_alias: bool = False)
```

Initialize a TimeSeriesSelector.

**Arguments**:

- `expr` (`TagExpression`): Tag expression to select.

#### dtype

```python
def dtype()
```

Returns the Spark data type.

**Returns**:

`pyspark.sql.types.DataType`: Data type (BinaryType).

#### deserialize

```python
def deserialize(d)
```

Deserialize sample series after collection/toPandas.

**Arguments**:

- `d` (`Any`): Data to deserialize.

**Returns**:

`SampleSeries`: Deserialized sample series.

#### build

```python
def build(cache: SeriesCache) -> SampleSeries
```

Instantiate a SampleSeries from given cache data.

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

