---
sidebar_label: delta_solver
title: impulse_query_engine.analyze.query.solvers.delta_solver
---

## DeltaSolver

```python
class DeltaSolver(QuerySolver)
```

#### \_\_init\_\_

```python
def __init__(spark,
             config: SolverConfig = None,
             is_raw_data: bool = True,
             drop_implausible_data: bool = False)
```

Initialize the DeltaSolver.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `config` (`SolverConfig`): Solver configuration.  When *None* a default :class:`SolverConfig`
is used (backward-compatible column names).
- `is_raw_data` (`bool`): Indicates whether the input data is raw point data (with a timestamp column) or already in RLE format
(with tstart and tend columns).
- `drop_implausible_data` (`bool`): Specifies whether we should drop implausible data points before RLE encoding.
IMPORTANT: The silver layer needs the is_plausible column for this to work.
If this is set to True, all data points which are marked as implausible will be dropped before RLE encoding.

#### filter\_container\_tags

```python
def filter_container_tags(spark, query) -> DataFrame
```

Stage 1: Generate DataFrame filtered by container tags.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `query` (`QueryBuilder`): Query object containing filters and db info.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame filtered by container tags.

#### filter\_container\_metrics

```python
def filter_container_metrics(spark,
                             query,
                             container_df,
                             pre_filtered_containers_df=None) -> DataFrame
```

Stage 2: Filter containers by metrics.

Returns full container metrics (not just container_id) so that
ContainerDimension and ContainerEvent can access start_ts/stop_ts.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `query` (`QueryBuilder`): Query object containing filters and db info.
- `container_df` (`pyspark.sql.DataFrame`): DataFrame from filter_container_tags stage.
- `pre_filtered_containers_df` (`pyspark.sql.DataFrame`): Pre-filtered containers for incremental processing.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame containing filtered container metrics.

#### filter\_channel\_tags

```python
def filter_channel_tags(spark, db, container_df, selectors) -> DataFrame
```

Stage 3: Filter channels by tags and compute ``selector_id``.

Extracts leaf selectors, pivots the channel-tags table, filters
matching channels, and assigns each row its ``selector_id`` so
that Stage 4 can be a simple passthrough.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `db` (`MeasurementDB`): Measurement database for table access.
- `container_df` (`pyspark.sql.DataFrame`): DataFrame containing container information.
- `selectors` (`list[TimeSeriesSelector]`): Non-aliased (direct) selectors.

**Returns**:

`pyspark.sql.DataFrame`: ``(container_id, channel_id, selector_id)``

#### filter\_channel\_metrics

```python
def filter_channel_metrics(spark, db, channel_df, selectors) -> DataFrame
```

Stage 4: Join with ``channel_metrics`` to restrict to channels that

have metric entries.

The input *channel_df* already carries a ``selector_id`` column
from Stage 3.  This stage inner-joins it with the channel-metrics
table so that channels without any recorded samples are excluded,
then wraps ``selector_id`` into an array ``selector_ids``.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `db` (`MeasurementDB`): Measurement database for table access.
- `channel_df` (`pyspark.sql.DataFrame`): DataFrame from :meth:`filter_channel_tags` with columns
``(container_id, channel_id, selector_id)``.
- `selectors` (`list[TimeSeriesSelector]`): Non-aliased selectors (unused — selector_id comes from Stage 3).

**Returns**:

`pyspark.sql.DataFrame`: DataFrame with ``(container_id, channel_id, selector_ids)``
where ``selector_ids`` is an array column.

#### solve

```python
def solve(query, channels_df, selections, dtypes)
```

Solve the query by grouping channels and applying selections.

**Arguments**:

- `query` (`QueryBuilder`): Query object containing database and filter information.
- `channels_df` (`pyspark.sql.DataFrame`): DataFrame containing channel information.
- `selections` (`list`): List of selection expressions to apply.
- `dtypes` (`list`): List of data types for each selection.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame containing results for each container.

