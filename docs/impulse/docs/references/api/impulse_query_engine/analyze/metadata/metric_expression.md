---
sidebar_label: metric_expression
title: impulse_query_engine.analyze.metadata.metric_expression
---

## MetricSelector

```python
class MetricSelector(MetricExpression)
```

#### \_\_init\_\_

```python
def __init__(key: str)
```

Initialize a MetricSelector.

**Arguments**:

- `key` (`str`): The name of the metric to select.

#### get\_selector\_expr

```python
def get_selector_expr() -> Column
```

Return a Spark SQL column expression for the selected metric.

**Returns**:

`pyspark.sql.Column`: Spark SQL column corresponding to the metric key.

#### build\_pandas

```python
def build_pandas(df) -> pd.Series
```

Return a pandas Series for the selected metric from the DataFrame.

**Arguments**:

- `df` (`pandas.DataFrame`): DataFrame containing metric data.

**Returns**:

`pandas.Series`: Series corresponding to the metric key.

#### \_\_repr\_\_

```python
def __repr__()
```

Return the string representation of the MetricSelector.

**Returns**:

`str`: String representation.

#### \_\_str\_\_

```python
def __str__()
```

Return the string representation of the MetricSelector.

**Returns**:

`str`: String representation.

#### required\_metrics

```python
def required_metrics() -> set[str]
```

Return a set containing the metric key.

**Returns**:

`set of str`: Set containing the metric key.

## MetricOp

```python
class MetricOp(MetricExpression)
```

#### \_\_init\_\_

```python
def __init__(operation, *args, **kwargs)
```

Initialize a MetricOp.

**Arguments**:

- `operation` (`callable`): The operation to apply.
- `*args`: Arguments like MetricExpressions for the operation.
- `**kwargs`: Keyword arguments like MetricExpressions for the operation.

#### get\_selector\_expr

```python
def get_selector_expr() -> Column
```

Build a Spark SQL expression for the metric selection.

**Returns**:

`pyspark.sql.Column`: Spark SQL column representing the metric operation.

#### build\_pandas

```python
def build_pandas(df) -> pd.Series
```

Build a pandas Series for the metric operation from the given DataFrame.

**Arguments**:

- `df` (`pandas.DataFrame`): DataFrame containing metric data.

**Returns**:

`pandas.Series`: Series representing the metric operation.

#### \_\_repr\_\_

```python
def __repr__()
```

Return the string representation of the MetricOp.

**Returns**:

`str`: String representation.

#### \_\_str\_\_

```python
def __str__()
```

Return the string representation of the MetricOp.

**Returns**:

`str`: String representation.

#### required\_metrics

```python
def required_metrics() -> set[str]
```

Return a set of required metric keys for the operation.

**Returns**:

`set of str`: Set of required metric keys.

