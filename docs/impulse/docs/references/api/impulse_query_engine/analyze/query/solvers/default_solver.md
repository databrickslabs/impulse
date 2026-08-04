---
sidebar_label: default_solver
title: impulse_query_engine.analyze.query.solvers.default_solver
---

## DefaultSolver

```python
class DefaultSolver(QuerySolver)
```

The default query-engine solver.  Adapts to the shape of the silver layer.

Channel selection works in two modes, chosen per query by whether a
``channel_tags_table`` is configured on the database:

- **EAV channel_tags** (``channel_tags_table`` set): channel attributes
  such as ``channel_name`` live as narrow key/value rows in a
  ``channel_tags`` table, which is pivoted to wide format on the fly and
  matched against the channel selectors.
- **Wide channel_metrics** (no ``channel_tags_table``): the selection
  attributes are columns on ``channel_metrics`` and are matched directly,
  with no pivot.

Container tags are likewise optional: when a ``container_tags_table`` is
configured the narrow/EAV table is pivoted for tag-based container
filtering; otherwise container attributes are expected as columns on
``container_metrics`` (wide-only model).  The solver additionally supports
channel-alias resolution via the ``channel_mapping`` table and per-alias
unit conversion via the ``unit_conversion`` table.

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
- `raw_encoder` (`RawEncoder`): Which encoder converts RAW point data into intervals for solving.
``RawEncoder.RLE`` (default) run-length encodes equal-valued runs;
``RawEncoder.INTERVAL`` only derives ``tend`` and drops exact
duplicates.  Only consulted when ``is_raw_data`` is ``True``.

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

Stage 3: resolve channel selectors against the EAV ``channel_tags``

table, or pass through when no such table is configured.

When a ``channel_tags_table`` is configured on the database, the narrow
key/value rows are pivoted to wide format (one column per required tag
key), matched against the selectors, and each surviving
``(container_id, channel_id)`` row is assigned its ``selector_id`` so
that stage 4 only has to drop channels lacking metric entries.

When no ``channel_tags_table`` is configured, the channel-selection
attributes (e.g. ``channel_name``) live as columns on
``channel_metrics``; this stage is then a pass-through and the matching
happens in :meth:`filter_channel_metrics`.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `db` (`MeasurementDB`): Measurement database for table access.
- `container_df` (`pyspark.sql.DataFrame`): DataFrame containing the tag-filtered container information.
- `selectors` (`list[TimeSeriesSelector]`): Non-aliased (direct) selectors.

**Returns**:

`pyspark.sql.DataFrame`: ``(container_id, channel_id, selector_id)`` in EAV mode, or the
input container DataFrame unchanged in wide mode.

#### filter\_channel\_metrics

```python
def filter_channel_metrics(spark, db, channel_df, selectors) -> DataFrame
```

Stage 4: produce ``(container_id, channel_id, selector_ids)``.

In EAV mode (``channel_tags_table`` configured) *channel_df* already
carries a ``selector_id`` from :meth:`filter_channel_tags`; this stage
inner-joins ``channel_metrics`` on ``(container_id, channel_id)`` to
drop channels without metric entries, then wraps ``selector_id`` into
the ``selector_ids`` array.

In wide mode (no ``channel_tags_table``) *channel_df* is the container
frame; the selectors are applied directly to ``channel_metrics``
columns, the result is restricted to the candidate containers, and
``selector_ids`` is computed from the matching selectors.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `db` (`MeasurementDB`): Measurement database for table access.
- `channel_df` (`pyspark.sql.DataFrame`): In EAV mode, the ``(container_id, channel_id, selector_id)`` frame
from :meth:`filter_channel_tags`; in wide mode, the container frame.
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

When the database is configured with a ``unit_conversion_table`` and
the ``channel_mapping`` table carries ``source_unit`` / ``target_unit``
columns, this method also propagates the effective unit pair on each
resolved row.  The effective ``source_unit`` is computed as
``COALESCE(channel_metrics.unit, channel_mapping.source_unit)`` so
that the authoritative per-channel physical unit on
``channel_metrics`` takes precedence over the mapping-level default
when present.  ``target_unit`` is always taken from the mapping —
there is no analogous column on ``channel_metrics``.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `db` (`MeasurementDB`): Measurement database for table access.
- `container_df` (`pyspark.sql.DataFrame`): DataFrame containing tag-filtered container IDs.
- `selectors` (`list[TimeSeriesSelector]`): Aliased selectors extracted from the query.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame with
``(container_id, channel_id, <metrics-side join keys>,
channel_alias, alias_priority, selector_ids)`` where
``selector_ids`` is an array column.  The metrics-side join key
columns come from ``effective_alias_join_keys`` (default:
``channel_name``, ``data_key``) and are deduplicated in case the
same physical column appears on both sides of a join-key tuple.
When unit conversion is active (see above), also carries
``source_unit`` and ``target_unit`` columns.

#### resolve\_channel\_selections

```python
def resolve_channel_selections(spark, channel_metrics_df,
                               aliased_channel_metrics_df) -> DataFrame
```

Union direct and aliased channel metrics, combining selector_ids.

When the aliased side carries ``source_unit`` / ``target_unit``
columns (added by :meth:`filter_aliased_channel_metrics` when a
unit conversion table is configured), those columns are preserved
through the union and aggregation.  Direct selectors produce null
unit columns, which causes the downstream conversion-factor join
in :meth:`solve` to leave their values unchanged.

Validates that each ``(container_id, channel_id)`` carries at most
one distinct ``source_unit`` and one distinct ``target_unit``.  Per
physical channel the unit-conversion model can attach only one
factor; conflicting aliases would otherwise pick an arbitrary
target and silently mis-convert one of them.

**Arguments**:

- `spark` (`SparkSession`): Spark session used for query execution.
- `channel_metrics_df` (`pyspark.sql.DataFrame`): Direct channel metrics with ``selector_ids`` array column.
- `aliased_channel_metrics_df` (`pyspark.sql.DataFrame`): Aliased channel metrics with ``selector_ids`` array column.

**Raises**:

- `ValueError`: If two or more aliased selectors resolve to the same physical
channel with conflicting ``source_unit`` or ``target_unit``
values.  Up to three offending channels are listed in the
message.

**Returns**:

`pyspark.sql.DataFrame`: Merged DataFrame with ``(container_id, channel_id, selector_ids)``
(plus ``source_unit`` / ``target_unit`` when present on the
aliased side).

#### solve

```python
def solve(query, channels_df, selections, dtypes) -> DataFrame
```

Solve the query by grouping channels and applying selections.

When a ``unit_conversion_table`` is configured on the database and
*channels_df* carries ``source_unit`` / ``target_unit`` columns
(added upstream by :meth:`filter_aliased_channel_metrics`),
per-channel conversion factors are computed and propagated into
the grouped-map UDF so that time-series values are converted from
the source to the target unit on the fly.

**Arguments**:

- `query` (`QueryBuilder`): Query object containing database and filter information.
- `channels_df` (`pyspark.sql.DataFrame`): DataFrame containing channel information.
- `selections` (`list`): List of selection expressions to apply.
- `dtypes` (`list`): List of data types for each selection.

**Returns**:

`pyspark.sql.DataFrame`: DataFrame containing results for each container.

#### solve\_calculated\_channels

```python
def solve_calculated_channels(query, channels_df, selections) -> DataFrame
```

Solve calculated channels by grouping channels and exploding each result.

Structurally parallels :meth:`solve` — sharing the
:meth:`_prepare_channels_join` prelude and :meth:`_apply_grouped_map`
tail — but the grouped-map UDF emits narrow silver-shaped rows (many per
container) instead of one wide row.  Output columns are
``[container_id, channel_id, tstart, tend, value, identity]`` where
``identity`` is a ``MapType(string, string)`` holding the channel's
identity dict.

**Arguments**:

- `query` (`QueryBuilder`): Query object containing database and filter information.
- `channels_df` (`pyspark.sql.DataFrame`): Channel-match DataFrame from the filter pipeline.
- `selections` (`list`): List of ``CalculatedChannel`` selections to evaluate.

**Returns**:

`pyspark.sql.DataFrame`: Narrow DataFrame of calculated-channel samples.

