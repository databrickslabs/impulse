---
sidebar_label: query_solver
title: impulse_query_engine.analyze.query.solvers.query_solver
---

## QuerySolver

```python
class QuerySolver(ABC)
```

Abstract base class for query solvers.

Defines a 6-stage filter pipeline that all solvers must implement:
filter_container_tags -> filter_container_metrics -> filter_channel_tags ->
filter_channel_metrics -> filter_candidates -> solve.

``filter_container_metrics`` must return a DataFrame that includes **all
columns** needed for container dimensions and event bounds (e.g.
``container_id``, ``start_ts``/``stop_ts`` or ``start_dt``/``stop_dt``),
not only ``container_id``.


#### filter\_container\_tags

```python
def filter_container_tags(spark, query) -> DataFrame
```

Abstract method to filter containers by tags.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `query` (`QueryBuilder`): Query object containing filters and db info.

**Raises**:

- `NotImplementedError`: If not implemented by subclass.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame filtered by container tags.

#### filter\_container\_metrics

```python
def filter_container_metrics(spark,
                             query,
                             container_df,
                             pre_filtered_containers_df=None) -> DataFrame
```

Abstract method to filter containers by metrics.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `query` (`QueryBuilder`): Query object containing filters and db info.
- `container_df` (`pyspark.sql.DataFrame`): DataFrame from filter_container_tags stage.
- `pre_filtered_containers_df` (`pyspark.sql.DataFrame`): Pre-filtered containers for incremental processing.
When provided, restricts processing to only these containers.

**Raises**:

- `NotImplementedError`: If not implemented by subclass.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame containing filtered container metrics.

#### filter\_channel\_tags

```python
def filter_channel_tags(spark, db: MeasurementDB, container_df,
                        selectors) -> DataFrame
```

Stage 3: Filter channels by measurements and tags.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `db` (`MeasurementDB`): Measurement database for table access.
- `container_df` (`pyspark.sql.DataFrame`): DataFrame containing container information.
- `selectors` (`list[TimeSeriesSelector]`): Non-aliased (direct) selectors extracted from the query.

**Raises**:

- `NotImplementedError`: If not implemented by subclass.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame containing filtered channel tags.

#### filter\_channel\_metrics

```python
def filter_channel_metrics(spark, db: MeasurementDB, channel_df,
                           selectors) -> DataFrame
```

Stage 4: Filter channels by metrics.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `db` (`MeasurementDB`): Measurement database for table access.
- `channel_df` (`pyspark.sql.DataFrame`): DataFrame containing channel information.
- `selectors` (`list[TimeSeriesSelector]`): Non-aliased (direct) selectors extracted from the query.

**Raises**:

- `NotImplementedError`: If not implemented by subclass.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame with ``(container_id, channel_id, selector_ids)``
where ``selector_ids`` is an array column.

#### filter\_aliased\_channel\_metrics

```python
def filter_aliased_channel_metrics(spark, db: MeasurementDB, container_df,
                                   selectors) -> DataFrame
```

Resolve aliased channel selections via the channel_mapping table.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `db` (`MeasurementDB`): Measurement database for table access.
- `container_df` (`pyspark.sql.DataFrame`): DataFrame containing filtered container IDs.
- `selectors` (`list[TimeSeriesSelector]`): Aliased selectors extracted from the query.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame with ``(container_id, channel_id, selector_ids)``
where ``selector_ids`` is an array column.

#### resolve\_channel\_selections

```python
def resolve_channel_selections(spark, channel_metrics_df,
                               aliased_channel_metrics_df) -> DataFrame
```

Union direct and aliased channel metrics, combining selector_ids.

Only called when aliased selectors are present.  The default
implementation raises ``NotImplementedError``; solvers that support
aliasing (e.g. ``KeyValueStoreSolver``) must override this.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `channel_metrics_df` (`pyspark.sql.DataFrame`): Direct channel metrics with ``selector_ids`` array column.
- `aliased_channel_metrics_df` (`pyspark.sql.DataFrame`): Aliased channel metrics with ``selector_ids`` array column.

**Returns**:

`pyspark.sql.DataFrame`: Merged DataFrame with ``(container_id, channel_id, selector_ids)``.

#### filter\_candidates

```python
def filter_candidates(query, channel_df) -> DataFrame
```

Stage 5: Select best channel candidate.

**Arguments**:

- `query` (`QueryBuilder`): Query object containing filters and db info.
- `channel_df` (`pyspark.sql.DataFrame`): DataFrame containing channel information.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame containing selected channel candidates.

#### solve

```python
def solve(query, channels_df, selections, dtypes)
```

Stage 6: Solve query.

**Arguments**:

- `query` (`QueryBuilder`): Query object containing database and filter information.
- `channels_df` (`pyspark.sql.DataFrame`): DataFrame containing channel information.
- `selections` (`list`): List of selection expressions to apply.
- `dtypes` (`list`): List of data types for each selection.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame containing results for each container.

