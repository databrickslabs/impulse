---
sidebar_label: basic_narrow_solver
title: mda_query_engine.analyze.query.solvers.basic_narrow_solver
---

## BasicNarrowSolver

```python
class BasicNarrowSolver(QuerySolver)
```

#### \_\_init\_\_

```python
def __init__(spark,
             config: SolverConfig = None,
             is_raw_data: bool = False,
             drop_implausible_data: bool = False)
```

Initialize the BasicNarrowSolver.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `config` (`SolverConfig`): Solver configuration.  When *None* a default :class:`SolverConfig`
is used (backward-compatible column names).
- `is_raw_data` (`bool`): Whether the input data is raw point data (timestamp column)
rather than RLE format (tstart/tend columns).
- `drop_implausible_data` (`bool`): Whether to drop data points marked as implausible before
processing.  Requires an ``is_plausible`` column in the
silver layer.

#### filter\_container\_tags

```python
def filter_container_tags(spark, query) -> DataFrame
```

Generate DataFrame filtered by container tags.

The BasicNarrowSolver does not filter by container tags,
so it returns an empty DataFrame.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `query` (`QueryBuilder`): Query object containing filters and db info.

**Returns**:

`pyspark.sql.DataFrame`: Empty DataFrame.

#### filter\_container\_metrics

```python
def filter_container_metrics(spark,
                             query,
                             container_df,
                             pre_filtered_containers_df=None) -> DataFrame
```

Filter containers by metrics.

Returns full container metrics (not just container_id) so that
ContainerDimension and ContainerEvent can access start_ts/stop_ts.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `query` (`QueryBuilder`): Query object containing filters and db info.
- `container_df` (`pyspark.sql.DataFrame`): DataFrame from filter_container_tags stage (unused by this solver).
- `pre_filtered_containers_df` (`pyspark.sql.DataFrame`): Pre-filtered containers for incremental processing.
When provided, restricts processing to only these containers.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame containing filtered container metrics.

#### filter\_channel\_tags

```python
def filter_channel_tags(spark, db, container_df, selectors) -> DataFrame
```

Pass through container DataFrame.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `db` (`MeasurementDB`): Measurement database for table access.
- `container_df` (`pyspark.sql.DataFrame`): DataFrame containing container information.
- `selectors` (`list[TimeSeriesSelector]`): Non-aliased selectors (unused by this solver).

**Returns**:

`pyspark.sql.DataFrame`: The input container DataFrame.

#### filter\_channel\_metrics

```python
def filter_channel_metrics(spark, db, container_df, selectors) -> DataFrame
```

Filter channels by metrics and required tags.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `db` (`MeasurementDB`): Measurement database for table access.
- `container_df` (`pyspark.sql.DataFrame`): DataFrame containing container information.
- `selectors` (`list[TimeSeriesSelector]`): Non-aliased (direct) selectors.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame with ``(container_id, channel_id, selector_ids)``.

#### solve

```python
def solve(query, channels_df, selections, dtypes) -> DataFrame
```

Solve the query by grouping channels and applying selections.

**Arguments**:

- `query` (`QueryBuilder`): Query object containing database and filter information.
- `channels_df` (`pyspark.sql.DataFrame`): DataFrame containing channel information.
- `selections` (`list`): List of selection expressions to apply.
- `dtypes` (`list`): List of data types for each selection.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame containing results for each container.

