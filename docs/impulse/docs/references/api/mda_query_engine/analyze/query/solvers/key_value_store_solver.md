---
sidebar_label: key_value_store_solver
title: mda_query_engine.analyze.query.solvers.key_value_store_solver
---

## KeyValueStoreSolver

```python
class KeyValueStoreSolver(BasicNarrowSolver)
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

#### filter\_container\_tags

```python
def filter_container_tags(spark, query) -> DataFrame
```

Filter container tags from the key-value-store table (narrow/EAV format).

Reads the narrow-format key-value-store table, applies the per-table
``column_name_mapping`` to rename physical columns to internal names,
then applies the top-level ``project_id`` filter and any per-table
``container_tags.filters``.  Pivots to wide format if tag filters
are present.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `query` (`QueryBuilder`): The query object containing filters and db info.

**Returns**:

`DataFrame`: A DataFrame containing the filtered container_ids.
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

