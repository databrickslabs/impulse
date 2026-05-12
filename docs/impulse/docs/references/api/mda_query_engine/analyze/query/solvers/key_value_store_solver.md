---
sidebar_label: key_value_store_solver
title: mda_query_engine.analyze.query.solvers.key_value_store_solver
---

## KeyValueStoreSolver

```python
class KeyValueStoreSolver(QuerySolver)
```

Solver for querying container metadata from a narrow/EAV key-value-store table.

This solver reads container tags from a narrow-format table where each
attribute is stored as a separate row (entity_id, element_id, value) and
pivots it to wide format for filtering. It then filters the container_metrics
table and resolves channel aliases via the channel_mapping table.

Physical column names that differ from the framework-internal names are
translated via per-table ``column_name_mapping`` entries at the point
where each table is read.  All subsequent processing uses the internal
column names exposed by :class:`SolverConfig`.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `config` (`SolverConfig or None`): Optional configuration.  When *None* (default) no filtering by
project or toolbox is applied.
- `is_raw_data` (`bool`): Whether the input data is raw point data (timestamp column)
rather than RLE format (tstart/tend columns).
- `drop_implausible_data` (`bool`): Whether to drop data points marked as implausible before
processing.  Requires an ``is_plausible`` column in the
silver layer.

#### filter\_container\_tags

```python
def filter_container_tags(spark, query) -> DataFrame
```

Filter container tags from the key-value-store table (narrow/EAV format).

If no ``container_tags_table`` is configured on the database, this
stage is a no-op and an empty DataFrame is returned: the solver is
operating on a wide-only data model (no narrow container_tags table).

Otherwise, reads the narrow-format key-value-store table, applies the
per-table ``column_name_mapping`` to rename physical columns to
internal names, then applies the top-level ``project_id`` filter
and any per-table ``container_tags.filters``.  Pivots to wide format
if tag filters are present.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `query` (`QueryBuilder`): The query object containing filters and db info.

**Returns**:

`DataFrame`: A DataFrame containing the filtered container_ids.
If no ``container_tags_table`` is configured, an empty DataFrame.
If no tag filters are present, returns distinct container_ids.
Otherwise, returns pivoted data with filter expressions applied.

#### filter\_container\_metrics

```python
def filter_container_metrics(spark,
                             query,
                             container_df,
                             pre_filtered_containers_df=None) -> DataFrame
```

Filter container_metrics and join with tag-filtered container IDs.

Reads the ``container_metrics`` table, applies the per-table
``column_name_mapping`` to rename physical columns to internal names,
applies the top-level ``project_id`` filter, any per-table
``container_metrics.filters``, and any ``MetricExpression`` filters
extracted from the query.  Finally, inner-joins the result with the
tag-filtered container DataFrame.

If no ``container_tags_table`` is configured on the database, the
join with ``container_df`` is skipped: stage 1 produced no
container IDs because no narrow tag table exists.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `query` (`QueryBuilder`): Query object containing filters and db info.
- `container_df` (`pyspark.sql.DataFrame`): DataFrame containing tag-filtered container IDs (output of
:meth:`filter_container_tags`).
- `pre_filtered_containers_df` (`pyspark.sql.DataFrame`): Pre-filtered container_metrics DataFrame.  When provided, it
replaces the read from ``query.db.container_metrics``.

**Returns**:

`pyspark.sql.DataFrame`: Filtered container metrics with all original columns preserved.
Deduplicated by ``container_id``.

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

#### filter\_aliased\_channel\_metrics

```python
def filter_aliased_channel_metrics(spark, db: MeasurementDB, container_df,
                                   selectors) -> DataFrame
```

Resolve aliased channel selections via the channel_mapping table.

Applies the per-table ``column_name_mapping`` to rename physical
columns, then applies the top-level ``project_id`` filter and any
per-table ``channel_mapping.filters``, and finally joins with
channel_metrics to resolve aliases.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `db` (`MeasurementDB`): Measurement database for table access.
- `container_df` (`pyspark.sql.DataFrame`): DataFrame containing tag-filtered container IDs.
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

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `channel_metrics_df` (`pyspark.sql.DataFrame`): Direct channel metrics with ``selector_ids`` array column.
- `aliased_channel_metrics_df` (`pyspark.sql.DataFrame`): Aliased channel metrics with ``selector_ids`` array column.

**Returns**:

`pyspark.sql.DataFrame`: Merged DataFrame with ``(container_id, channel_id, selector_ids)``.

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

