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

#### poi\_channel

```python
def poi_channel(dtype: SeriesValueType = SeriesValueType.DOUBLE,
                **kwargs) -> TimeSeriesSelector
```

Create a Points-in-Time (POI) channel selector.

Parallel to :meth:`channel` — it builds the **same** ``TimeSeriesSelector``
from a tag/column match on ``**kwargs`` (e.g.
``poi_channel(channel_name="DTC")``), differing only in that it is stamped
``series_type=POINTS_IN_TIME`` (so it solves to a
:class:`~impulse_query_engine.model.series.points_in_time_series.PointsInTimeSeries`
— a value valid only *at* each timestamp — rather than a ``SampleSeries``)
and carries the declared value ``dtype``.

Channel *identification* (tag/column match, ``get_selector_expr``,
``required_tags``, ``selector_id``) is identical to :meth:`channel`; only
the built object and its result dtype differ.

**Arguments**:

- `dtype` (`SeriesValueType or str`): The POI channel's value data type: ``DOUBLE`` (default, numeric) or
``STRING`` (e.g. DTC codes — only sampling and equality apply). Accepts
either the enum or its string value (``"double"`` / ``"string"``). This
declared type drives plan-time result typing and string-op gating; it
is validated against the silver ``poi_channels.dtype`` at solve time
(an actual/declared mismatch raises).
- `**kwargs` (`dict`): Channel tag-value pairs, matched exactly like :meth:`channel`'s.

**Returns**:

`TimeSeriesSelector`: A selector stamped ``series_type=POINTS_IN_TIME`` with the given value type.

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

#### solve\_calculated\_channels

```python
def solve_calculated_channels(
        spark,
        solver: QuerySolver = BlobSolver(),
        pre_filtered_containers_df: DataFrame = None) -> DataFrame
```

Compute calculated channels and return a narrow silver-shaped DataFrame.

Every selection must be a :class:`CalculatedChannel`.  This runs the same
metadata filter pipeline as :meth:`solve` (resolving the input channels
each calculated channel depends on), then evaluates each calculated
channel per container and emits rows in the silver ``channel_data`` shape
— ``container_id, channel_id, tstart, tend, value`` — plus a single
``identity`` ``MapType(string, string)`` column holding each channel's
identity dict.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `solver` (`QuerySolver`): Query solver to use.  Must implement ``solve_calculated_channels``
(``DefaultSolver`` does); the default ``BlobSolver`` does not.
- `pre_filtered_containers_df` (`DataFrame`): Pre-filtered container metrics for incremental processing.  When
provided, only these containers are processed; when None, all
containers matching the query filters are processed.

**Raises**:

- `ValueError`: If any selection is not a ``CalculatedChannel``, or if a wrapped
expression does not evaluate to a ``SampleSeries``.

**Returns**:

`pyspark.sql.DataFrame`: Narrow DataFrame ``[container_id, channel_id, tstart, tend, value,
identity]``.

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

