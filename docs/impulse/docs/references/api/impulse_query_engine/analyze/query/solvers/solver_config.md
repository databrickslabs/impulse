---
sidebar_label: solver_config
title: impulse_query_engine.analyze.query.solvers.solver_config
---

Configuration for solver column mappings.

Provides Pydantic models that map silver-layer column names to the internal
column names used by the solver classes, making the solvers independent
of a specific data-layer naming convention.

Each input table has its own :class:`TableConfig` section with an optional
``column_name_mapping`` (physical column → internal name) and ``filters``
(internal column → equality value).

Solvers apply the ``column_name_mapping`` when reading a table to rename
physical columns to internal names.  All subsequent processing — including
filter application — uses the framework-internal column names exposed as
properties on :class:`SolverConfig`.


## RawEncoder

```python
class RawEncoder(StrEnum)
```

Encoder used to convert RAW point data into intervals for solving.

Only relevant when the query engine operates on RAW (point) data; RLE input
is passed through unchanged regardless of this setting.

Values
------
RLE
    Run-length encode: collapse consecutive samples that share the same
    ``value`` within a container/channel into a single interval (see
    :class:`~impulse_query_engine.analyze.query.solvers.utils.rle_encoder.RleEncoder`).
INTERVAL
    Derive ``tend`` from the following sample's timestamp and drop exact
    duplicate points, *without* merging equal-valued runs (see
    :class:`~impulse_query_engine.analyze.query.solvers.utils.interval_encoder.IntervalEncoder`).


## TableConfig

```python
class TableConfig(BaseModel)
```

Per-table configuration for column renaming and equality filters.

**Arguments**:

- `column_name_mapping` (`dict[str, str]`): Mapping from physical column names on the table to internal
names used by the solver.  An empty dict means no renaming
(physical names already match internal names).
- `filters` (`dict[str, str]`): Equality filters applied to the table **after** column renaming.
Keys are internal column names; values are the literal values
to match.

## JoinKey

```python
class JoinKey(BaseModel)
```

A single column pair in the ``channel_mapping`` → ``channel_metrics`` join.

Used by :class:`ChannelMappingConfig.join_keys` to override the default
alias-resolution composite key.

Both fields reference column names **after** ``column_name_mapping`` has
been applied on the respective table; the two sides are independent, so
a column may appear under different names on the two tables.

**Arguments**:

- `mapping_col` (`str`): Column name on ``channel_mapping`` after its ``column_name_mapping``
has been applied.
- `metrics_col` (`str`): Column name on ``channel_metrics`` after its ``column_name_mapping``
has been applied.

## ChannelMappingConfig

```python
class ChannelMappingConfig(TableConfig)
```

``TableConfig`` plus an optional alias-resolution join-key spec.

**Arguments**:

- `join_keys` (`list[JoinKey] or None`): Custom composite key for the ``channel_mapping`` → ``channel_metrics``
join performed by ``DefaultSolver.filter_aliased_channel_metrics``.
When ``None`` (the default), the solver uses the backward-compatible
pair ``[(source_channel, channel_name), (data_key, data_key)]``
sourced from :class:`SolverConfig` internal-name properties.
Provide a custom list to change the join arity or column choice
(e.g. a single-column join when ``data_key`` is not part of the
channel identity in your silver layout).

## SolverConfig

```python
class SolverConfig(BaseModel)
```

Per-table configuration for solver column name mappings and filters.

The framework uses a fixed set of internal column names (e.g.
``container_id``, ``channel_id``, ``tstart``, ``tend``, ``value``).
When a silver-layer table uses different physical column names, the
per-table ``column_name_mapping`` renames them to the internal names
so that solver code can always reference the same constants.

**Arguments**:

- `project_id` (`str or None`): Optional project identifier applied as a filter on relevant tables
(container_tags, channel_mapping) by solvers that support it.
- `container_tags` (`TableConfig`): Column mappings and filters for the container tags (narrow/EAV) table.
- `container_metrics` (`TableConfig`): Column mappings and filters for the container metrics table.
- `channel_tags` (`TableConfig`): Column mappings and filters for the channel tags table.
- `channel_metrics` (`TableConfig`): Column mappings and filters for the channel metrics table.
- `channel_mapping` (`ChannelMappingConfig`): Column mappings, filters, and the alias-resolution ``join_keys``
override for the channel mapping (alias) table.
- `channels` (`TableConfig`): Column mappings and filters for the channel data table.
- `unit_conversion` (`TableConfig`): Column mappings and filters for the unit conversion table.
- `epoch_unit` (`{"s", "ms", "us", "ns"} or None`): Epoch unit of the channel sample timestamps (``tstart`` / ``tend``, or
``timestamp`` for RAW data).  When set, ``TIMESTAMP``-typed container
``start_ts`` / ``stop_ts`` are converted to epoch numbers in this unit for
container-boundary events (``ContainerEvent``, ``TimeWindowEvent``) and for
expressions that request them in the solve.  Channel timestamps are never
converted; they must already be epoch numbers.  Only required for a
``TimeWindowEvent`` over ``TIMESTAMP`` boundaries; when unset, nothing is
converted.

#### from\_json

```python
def from_json(cls, json_path: str) -> "SolverConfig"
```

Load a SolverConfig from a JSON file.

**Arguments**:

- `json_path` (`str`): Path to the JSON configuration file.

**Returns**:

`SolverConfig`: A new SolverConfig instance populated from the file.

#### from\_dict

```python
def from_dict(cls, data: dict) -> "SolverConfig"
```

Create a SolverConfig from a dictionary.

This is a convenience alias for ``model_validate(data)``.

**Arguments**:

- `data` (`dict`): Dictionary with configuration keys.

**Returns**:

`SolverConfig`: A new SolverConfig instance populated from *data*.

#### container\_id\_col

```python
def container_id_col() -> str
```

Internal column name for the container identifier.


#### channel\_id\_col

```python
def channel_id_col() -> str
```

Internal column name for the channel identifier.


#### channel\_id\_cols

```python
def channel_id_cols() -> list[str]
```

Composite key ``[container_id, channel_id]``.


#### tstart\_col

```python
def tstart_col() -> str
```

Internal column name for the start timestamp.


#### tend\_col

```python
def tend_col() -> str
```

Internal column name for the end timestamp.


#### start\_ts\_col

```python
def start_ts_col() -> str
```

Internal column name for the measurement-start epoch timestamp on container_metrics.


#### stop\_ts\_col

```python
def stop_ts_col() -> str
```

Internal column name for the measurement-stop epoch timestamp on container_metrics.


#### value\_col

```python
def value_col() -> str
```

Internal column name for the signal value on the channels table.


#### poi\_timestamp\_col

```python
def poi_timestamp_col() -> str
```

Internal column name for the point timestamp on the poi_channels table.


#### poi\_value\_double\_col

```python
def poi_value_double_col() -> str
```

Internal column name for the numeric value on the poi_channels table.


#### poi\_value\_string\_col

```python
def poi_value_string_col() -> str
```

Internal column name for the string value on the poi_channels table.


#### tag\_key\_col

```python
def tag_key_col() -> str
```

Internal column name for the attribute key on the container_tags (EAV) table.


#### tag\_value\_col

```python
def tag_value_col() -> str
```

Internal column name for the attribute value on the container_tags (EAV) table.


#### alias\_priority\_col

```python
def alias_priority_col() -> str
```

Internal column name for the alias priority on the channel_mapping table.


#### source\_channel\_col

```python
def source_channel_col() -> str
```

Internal column name for the source-channel identifier on the channel_mapping table.


#### data\_key\_col

```python
def data_key_col() -> str
```

Internal column name for the data-key identifier.

Default present on both ``channel_mapping`` and ``channel_metrics``;
used by the default :meth:`effective_alias_join_keys` for both sides.
Layouts where the two tables carry the data-key column under different
physical names can either rename both to ``"data_key"`` via per-table
``column_name_mapping`` or override


#### channel\_alias\_col

```python
def channel_alias_col() -> str
```

Internal column name for the alias identifier on the channel_mapping table.

Referenced by the dedup window in


#### channel\_name\_col

```python
def channel_name_col() -> str
```

Internal column name for the channel-name identifier on the channel_metrics table.


#### project\_id\_col

```python
def project_id_col() -> str
```

Internal column name for the project identifier.


#### parent\_id\_col

```python
def parent_id_col() -> str
```

Internal column name for the parent/scope identifier.


#### conversion\_factor\_col

```python
def conversion_factor_col() -> str
```

Internal column name for the conversion factor on the unit_conversion table.

Also used as the column that carries the per-channel combined factor
downstream from :meth:`DefaultSolver._compute_conversion_factors`
into the grouped-map UDF.


#### source\_unit\_col

```python
def source_unit_col() -> str
```

Internal column name for the source unit on the channel_mapping table.


#### target\_unit\_col

```python
def target_unit_col() -> str
```

Internal column name for the target unit on the channel_mapping table.


#### unit\_col

```python
def unit_col() -> str
```

Internal column name for the unit identifier.

Used in two places that happen to share the same default name:

- On the ``unit_conversion`` table, as the key joined against
  ``channel_mapping.source_unit`` / ``target_unit`` to look up a
  conversion factor.
- On the ``channel_metrics`` table (optional), as the authoritative
  physical unit of a channel.  When present, takes precedence over
  ``channel_mapping.source_unit`` for aliased reads via the
  :meth:`DefaultSolver.filter_aliased_channel_metrics`
  coalesce.

Users with different internal names per table can rename physical
columns to ``unit`` on each table independently via the per-table
``column_name_mapping``.


#### group\_id\_col

```python
def group_id_col() -> str
```

Internal column name for the unit group id on the unit_conversion table.


#### timestamp\_col

```python
def timestamp_col() -> str
```

Internal column name for the timestamp on the channels table for raw encoded channel data.


#### is\_plausible\_col

```python
def is_plausible_col() -> str
```

Internal column name for the plausibility flag on the channels table for raw encoded channel data.


#### effective\_alias\_join\_keys

```python
def effective_alias_join_keys() -> list[tuple[str, str]]
```

Return the resolved alias-resolution join keys as ``(mapping_col, metrics_col)`` tuples.

Falls back to the default composite key
``[(source_channel_col, channel_name_col), (data_key_col, data_key_col)]``
when :attr:`ChannelMappingConfig.join_keys` is ``None``.  Otherwise
returns the configured list.

Both members of each tuple are column names **after**
``column_name_mapping`` has been applied on the respective table.


#### col\_map

```python
def col_map() -> dict[str, str]
```

Short-key → internal-column-name mapping for UDFs and caches.


#### reject\_implausible\_channels\_filter\_in\_raw

```python
def reject_implausible_channels_filter_in_raw(is_raw: bool) -> None
```

Raise if an is_plausible channels filter is set in RAW mode.

Such a filter runs before raw encoding and bridges intervals across dropped
samples instead of splitting them; use drop_implausible_data instead. No-op
when not raw.


#### normalize\_container\_boundaries

```python
def normalize_container_boundaries(df: DataFrame) -> DataFrame
```

Convert ``TIMESTAMP`` container start/stop columns to epoch numbers.

Opt-in via :attr:`epoch_unit`: when it is unset, *df* is returned unchanged.
Otherwise each ``TIMESTAMP`` ``start_ts`` / ``stop_ts`` column becomes an epoch
number in that unit, computed from ``unix_micros`` (exact and independent of the
session time zone).  ``"s"`` / ``"ms"`` give doubles (``"s"`` equals Spark's
``cast(timestamp as double)``), ``"us"`` / ``"ns"`` give longs.  Numeric columns
are left as they are.  Applying the same transform before both the event fact
and the solve keeps their window boundaries identical.

**Arguments**:

- `df` (`pyspark.sql.DataFrame`): Column-mapped ``container_metrics`` frame (or a projection of it).

**Raises**:

- `ValueError`: If :attr:`epoch_unit` is set and a boundary column is ``TIMESTAMP_NTZ`` or
``DATE`` (their epoch depends on a time zone and is not supported).

**Returns**:

`pyspark.sql.DataFrame`: *df* with converted boundary columns.

#### require\_epoch\_boundaries

```python
def require_epoch_boundaries(df: DataFrame, owner: str) -> None
```

Raise unless the container start/stop columns on *df* are epoch numbers.

Call after :meth:`normalize_container_boundaries`.  Checks the schema only, so it
fails fast on the driver before any Spark job runs.

**Arguments**:

- `df` (`pyspark.sql.DataFrame`): Normalized container_metrics frame.
- `owner` (`str`): Name of the feature that needs epoch boundaries, used in the error message.

**Raises**:

- `ValueError`: If ``start_ts`` / ``stop_ts`` is still a date/time type.

