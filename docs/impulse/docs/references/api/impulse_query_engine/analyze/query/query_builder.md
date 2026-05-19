---
sidebar_label: query_builder
title: impulse_query_engine.analyze.query.query_builder
---

## QueryBuilder

```python
class QueryBuilder()
```

#### \_\_init\_\_

```python
def __init__(db: "impulse_query_engine.analyze.MeasurementDB")
```

Initialize the QueryBuilder.

**Arguments**:

- `db` (`impulse_query_engine.analyze.MeasurementDB`): Measurement database object.

#### where

```python
def where(*args)
```

Add filter expressions to the query.

**Arguments**:

- `*args` (`list`): Filter expressions to be added.

**Returns**:

`QueryBuilder`: The updated QueryBuilder instance.

#### filter

```python
def filter(*args)
```

Alias for where().

**Arguments**:

- `*args` (`list`): Filter expressions to be added.

**Returns**:

`QueryBuilder`: The updated QueryBuilder instance.

#### havingTag

```python
def havingTag(**kwargs)
```

Add tag-based filters to the query.

**Arguments**:

- `**kwargs` (`dict`): Tag-value pairs to filter by.

**Returns**:

`QueryBuilder`: The updated QueryBuilder instance.

#### tag

```python
def tag(key: str, cast_type: str | None = None) -> TagSelector
```

Create a tag selector for the given key.

**Arguments**:

- `key` (`str`): Name of the tag (element_id in the EAV table).
- `cast_type` (`str or None`): Spark type to cast the tag value to before comparison
(e.g. ``"int"``, ``"double"``, ``"string"``).

**Returns**:

`TagSelector`: Tag selector object.

#### metric

```python
def metric(name) -> MetricSelector
```

Create a metric selector for the given name.

**Arguments**:

- `name` (`str`): Name of the metric.

**Returns**:

`MetricSelector`: Metric selector object.

#### channel

```python
def channel(**kwargs) -> TimeSeriesSelector
```

Create a time series selector for the given channel tags.

**Arguments**:

- `**kwargs` (`dict`): Channel tag-value pairs.

**Returns**:

`TimeSeriesSelector`: Time series selector object.

#### select

```python
def select(*args) -> Self
```

Set the selection expressions for the query.

**Arguments**:

- `*args` (`list`): Selection expressions.

**Returns**:

`QueryBuilder`: The updated QueryBuilder instance.

#### solve

```python
def solve(spark,
          solver: QuerySolver = BlobSolver(),
          pre_filtered_containers_df: DataFrame = None) -> DataFrame
```

Execute the query using the specified solver and return a Spark DataFrame.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `solver` (`QuerySolver`): Query solver to use (default is BlobSolver).
- `pre_filtered_containers_df` (`DataFrame`): Pre-filtered container metrics DataFrame for incremental processing.
When provided, only these containers will be processed.
When None, all containers matching query filters are processed (full mode).

**Returns**:

`pyspark.sql.DataFrame`: DataFrame containing query results.

#### toPandas

```python
def toPandas(spark, solver: QuerySolver = BlobSolver()) -> pd.DataFrame
```

Execute the query and collect results into a Pandas DataFrame.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `solver` (`QuerySolver`): Query solver to use (default is BlobSolver).

**Returns**:

`pd.DataFrame`: Pandas DataFrame containing query results.

