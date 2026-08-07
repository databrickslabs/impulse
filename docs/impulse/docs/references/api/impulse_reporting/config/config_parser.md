---
sidebar_label: config_parser
title: impulse_reporting.config.config_parser
---

#### is\_valid\_table\_name

```python
def is_valid_table_name(table_name: str) -> str
```

Validate if a string is a valid Unity Catalog table name.

**Arguments**:

- `table_name` (`str`): The table name to validate. Should be in format 'catalog.schema.table'.

**Raises**:

- `ValueError`: If the table name does not match the required format or contains invalid characters.

**Returns**:

`str`: The validated table name if valid.

#### is\_valid\_unity\_entity\_name

```python
def is_valid_unity_entity_name(entity_name: str) -> str
```

Validate if a string is a valid Unity Catalog entity name.

**Arguments**:

- `entity_name` (`str`): The entity name to validate (catalog, schema, or table prefix).

**Raises**:

- `ValueError`: If the entity name contains invalid characters.

**Returns**:

`str`: The validated entity name if valid.

## Solvers

```python
class Solvers(Enum)
```

Enumeration of available solver types for the query engine.

``DEFAULT_SOLVER`` is the single, unified solver. ``DELTA_SOLVER`` and
``KEY_VALUE_STORE_SOLVER`` are **deprecated aliases** kept so that existing
report configs continue to deserialize; both now resolve to the same
``DefaultSolver``. They will be removed in a future release.

**Arguments**:

- `DEFAULT_SOLVER` (`str`): None
- `DELTA_SOLVER` (`str`): Deprecated alias for ``DEFAULT_SOLVER``.
- `KEY_VALUE_STORE_SOLVER` (`str`): Deprecated alias for ``DEFAULT_SOLVER``.

## Source

```python
class Source(BaseModel)
```

Configuration for data source tables in Unity Catalog.

**Arguments**:

- `container_tags_table` (`str`): Full Unity Catalog path to the container tags table (narrow/EAV format).
Required when filtering by container tags. Omit for wide-only data
models that carry container attributes as columns on
``container_metrics``. ``project_id`` scoping is independent of this
field — it works in both narrow EAV and wide-only data models because
it is applied to ``container_metrics`` (and ``channel_mapping`` if
configured) regardless of whether ``container_tags_table`` is set.
- `container_metrics_table` (`str`): Full Unity Catalog path to the container metrics table.
- `channel_metrics_table` (`str`): Full Unity Catalog path to the channel metrics table.
- `channels_uri` (`str`): Full Unity Catalog path to the channels data table.
- `channel_mapping_table` (`str`): Full Unity Catalog path to the channel mapping table. Required when using
``channel_with_alias()`` for logical alias resolution.
- `unit_conversion_table` (`str`): Full Unity Catalog path to the unit conversion table. When set together
with a ``channel_mapping_table`` whose rows carry ``source_unit`` and
``target_unit`` columns, the query engine converts time-series values
from the source to the target unit during ``solve()``.

## UnitySink

```python
class UnitySink(BaseModel)
```

Configuration for data sink location in Unity Catalog.

**Arguments**:

- `catalog` (`str`): Target catalog name for output tables.
- `schema` (`str`): Target schema name for output tables.
- `table_prefix` (`str`): Prefix to use for generated output table names.

## Comparator

```python
class Comparator(str, Enum)
```

Supported comparison operators for container filters.


## CastType

```python
class CastType(str, Enum)
```

Supported Spark cast types for tag value columns.


## TagFilter

```python
class TagFilter(BaseModel)
```

A single tag-based filter applied on the container_tags_table (EAV).

**Arguments**:

- `tag_name` (`str`): The tag key / element_id to filter on.
- `comparator` (`Comparator`): The comparison operator.
- `value` (`str | int | float | datetime`): The expected value. Must match the cast_type: str for STRING,
int for INT, int|float for DOUBLE, ISO-format string for TIMESTAMP
(automatically parsed to datetime).
- `cast_type` (`CastType`): Spark type to cast the tag value to before comparison.

## MetricFilter

```python
class MetricFilter(BaseModel)
```

A single metric-based filter applied on the container_metrics_table.

**Arguments**:

- `column_name` (`str`): The metric column to filter on.
- `comparator` (`Comparator`): The comparison operator.
- `value` (`str | int | float | datetime`): The expected value. When value_type is provided, must match accordingly.
- `value_type` (`CastType`): When provided, validates and/or converts the value to the expected type.

## ContainerFilters

```python
class ContainerFilters(BaseModel)
```

Container-level filters in disjunctive normal form (OR of ANDs).

Each outer list element is a group of filters that are AND-combined.
The resulting group expressions are then OR-combined.

**Arguments**:

- `tag_filters` (`list[list[TagFilter]]`): Tag-based filter groups (applied on container_tags_table).
- `metric_filters` (`list[list[MetricFilter]]`): Metric-based filter groups (applied on container_metrics_table).

## QueryEngine

```python
class QueryEngine(BaseModel)
```

Configuration for the query engine solver.

**Arguments**:

- `solver` (`Solvers, default=Solvers.DEFAULT_SOLVER`): The solver type to use for query execution.
- `raw_encoder` (`RawEncoder, optional, default=None`): Encoder used to convert RAW point data into intervals.  ``RLE``
collapses consecutive equal-valued samples into runs; ``INTERVAL``
only derives ``tend`` and drops exact duplicates.  Only takes effect
when ``data_type=RAW``; ignored for RLE input.  When omitted and
``data_type=RAW``, it is resolved to ``RLE`` at validation time;
for RLE input the field stays ``None`` and is never consulted.
- `solver_config` (`SolverConfig`): Per-table column name mappings and filter configuration for
the solver.  Use this when your silver-layer tables use
non-default column names or when you need project/toolbox
scoping.  Key sub-fields:

- ``project_id`` (str): Top-level project filter value applied
  to container_tags, container_metrics, and channel_mapping
  tables when the corresponding columns exist after column
  renaming.
- Per-table sections (``container_tags``, ``container_metrics``,
  ``channel_mapping``, ``channels``, etc.) each with
  ``column_name_mapping`` and ``filters`` dicts.

When omitted, all default column names are used and no
project/toolbox filtering is applied.

#### validate\_drop\_implausible\_data\_requires\_raw

```python
def validate_drop_implausible_data_requires_raw()
```

`drop_implausible_data=True` currently only takes effect with RAW data.

The filter is applied inside the RAW -> interval conversion path by the
selected ``raw_encoder`` (``RleEncoder`` / ``IntervalEncoder``).


#### default\_raw\_encoder\_for\_raw\_data

```python
def default_raw_encoder_for_raw_data()
```

When ``data_type=RAW`` and ``raw_encoder`` is unset, default to RLE.


## IncrementalConfig

```python
class IncrementalConfig(BaseModel)
```

Configuration for incremental processing behavior.

**Arguments**:

- `enabled` (`bool, default=False`): Whether incremental processing is enabled.
- `silver_last_modified_column` (`str, default="timestamp"`): Column name in the silver layer used for freshness comparison.
- `gold_last_modified_column` (`str, default="last_modified"`): Column name in the gold layer used for freshness comparison.

## CalculatedChannels

```python
class CalculatedChannels(BaseModel)
```

Configuration for calculated-channel outputs.

**Arguments**:

- `emit_channel_metrics` (`bool, default=False`): When True, also emit a ``calculated_channel_metrics`` gold table (silver
``channel_metrics`` shape) alongside the calculated-channel fact table, so
the fact + metrics pair can serve as an Impulse silver source.
- `attribute_columns` (`list of str, default=[]`): Calculated-channel attribute keys to surface as columns on the metrics
table (e.g. ``["unit"]``). Empty (the default) → no attribute columns.
Identity keys are always surfaced dynamically and win over an
attribute key of the same name.
- `kpis` (`list of str, default=["duration", "min", "max", "mean"]`): KPIs computed on the metrics table, one column per name. Each must be a
registered KPI (see ``calculated_channel_kpis.KPI_BUILDERS``); an unknown
name is rejected at validation. Duplicates are removed (order preserved).

## ImpulseConfig

```python
class ImpulseConfig(BaseModel)
```

Main configuration model.

Attributes
 ----------
 source : Source
     Configuration for input data sources.
 unity_sink : UnitySink
     Configuration for output data location.
 container_filters : ContainerFilters, optional
     Optional container-level filters (tag-based and/or metric-based).
 query_engine : QueryEngine, optional
     Optional query engine configuration. Defaults to Solvers.DEFAULT_SOLVER.
 incremental : IncrementalConfig, optional
     Optional incremental processing configuration. Defaults to IncrementalConfig().
 calculated_channels : CalculatedChannels, optional
     Optional calculated-channel output configuration (e.g. opting in to the
     ``calculated_channel_metrics`` table). Defaults to CalculatedChannels().
 measurement_dimensions : list of str, optional
     Column names to surface from ``container_metrics`` into the
     gold-layer ``measurement_dimension`` table. Names are matched
     **after** ``query_engine.solver_config.container_metrics.column_name_mapping``
     has been applied — i.e. these are the internal (post-mapping)
     column names, not the physical silver column names. If a silver
     table uses a physical name like ``my_measurement_id`` mapped to
     ``container_id``, list ``"container_id"`` here. Each listed name
     lands in the gold table verbatim, so the configured name is also
     the gold column name. Defaults to
     ``["container_id", "start_ts", "stop_ts"]``. The framework does not
     inject any column the user omits — keeping ``container_id`` in the
     list is recommended because it is the upsert key for incremental
     processing and the join key to event-fact tables, but the choice
     is the user's.
 Examples
 --------
>>> config_data = {
 ...     "source": {
 ...         "container_metrics_table": "impulse_demo.silver.container_metric",
 ...         "channel_metrics_table": "impulse_demo.silver.channel_metric",
 ...         "channels_uri": "impulse_demo.silver.channel_data",
 ...         "channel_mapping_table": "impulse_demo.data_model.channel_mapping"
 ...     },
 ...     "unity_sink": {
 ...         "catalog": "impulse_demo",
 ...         "schema": "silver_refactored",
 ...         "table_prefix": "evaluation"
 ...     },
 ...     "container_filters": {
 ...         "tag_filters": [
 ...             [
 ...                 {"tag_name": "uut_id", "comparator": "==", "value": "AA080518", "cast_type": "string"}
 ...             ]
 ...         ],
 ...         "metric_filters": [
 ...             [
 ...                 {"column_name": "uut_id", "comparator": "==", "value": "AA080518"},
 ...                 {"column_name": "start_ts", "comparator": ">=", "value": "2025-04-27T05:20:54.000Z"}
 ...             ]
 ...         ]
 ...     },
 ...     "query_engine": {
 ...         "solver": "DefaultSolver",
 ...         "solver_config": {
 ...             "project_id": "my_project",
 ...             "container_tags": {
 ...                 "column_name_mapping": {"entity_id": "container_id"},
 ...                 "filters": {"parent_id": "my_parent_id"}
 ...             },
 ...             "container_metrics": {
 ...                 "column_name_mapping": {}
 ...             },
 ...             "channel_metrics": {
 ...                 "column_name_mapping": {}
 ...             },
 ...             "channel_mapping": {
 ...                 "column_name_mapping": {},
 ...                 "filters": {"toolbox_id": "my_toolbox"}
 ...             },
 ...             "channels": {
 ...                 "column_name_mapping": {}
 ...             }
 ...         }
 ...     }
 ... }
 >>> config = ImpulseConfig.model_validate(config_data)


