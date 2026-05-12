---
sidebar_label: config_parser
title: mda_reporting.config.config_parser
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

## MeasurementDimensions

```python
class MeasurementDimensions(Enum)
```

Enumeration for available measurement dimensions information.

**Arguments**:

- `CONTAINER_ID` (`str`): Identifier for the container.
- `UUT_ID` (`str`): Identifier for the unit under test (UUT).
- `PROJECT_ID` (`str`): Identifier for the project.
- `UUT_NAME` (`str`): Name of the unit under test (UUT). Currently not present in implementation.
- `FILE_NAME` (`str`): Name of the file associated with the measurement.
- `SOURCE_FILE_PATH` (`str`): Path to the source file containing the measurement data.
- `START_TS` (`str`): Timestamp of the first data point in the measurement.
- `STOP_TS` (`str`): Timestamp of the last data point in the measurement.
- `ODO_START` (`str`): Starting odometer reading for the measurement. Currently not present in implementation.
- `ODO_STOP` (`str`): Stopping odometer reading for the measurement. Currently not present in implementation.
- `ENVIRONMENT` (`str`): Environment in which the measurement was taken either puma or datalogger.

#### PROJECT\_ID

todo not present currently


#### UUT\_NAME

todo not present currently


#### ODO\_START

todo not present currently


#### ODO\_STOP

todo not present currently


#### get\_column

```python
def get_column() -> Column
```

Returns the corresponding Spark SQL column for the measurement dimension.

The column names are mapped to their respective values based on the ER gold naming conventions.

**Returns**:

`pyspark.sql.Column`: The Spark SQL column corresponding to the measurement dimension.

#### map\_gold\_name\_to\_silver

```python
def map_gold_name_to_silver() -> str
```

Maps the silver layer column name to the ER gold layer column name.

**Returns**:

`str`: The gold layer column name.

## Solvers

```python
class Solvers(Enum)
```

Enumeration of available solver types for the query engine.

**Arguments**:

- `BASIC_NARROW_SOLVER` (`str`): None
- `DELTA_SOLVER` (`str`): None
- `KEY_VALUE_STORE_SOLVER` (`str`): None

## Source

```python
class Source(BaseModel)
```

Configuration for data source tables in Unity Catalog.

**Arguments**:

- `container_tags_table` (`str`): Full Unity Catalog path to the container tags table (narrow/EAV format).
Required when using KeyValueStoreSolver.
- `container_metrics_table` (`str`): Full Unity Catalog path to the container metrics table.
- `channel_metrics_table` (`str`): Full Unity Catalog path to the channel metrics table.
- `channels_uri` (`str`): Full Unity Catalog path to the channels data table.
- `channel_mapping_table` (`str`): Full Unity Catalog path to the channel mapping table. Required when using
``channel_with_alias()`` for logical alias resolution.

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

- `solver` (`Solvers, default=Solvers.BASIC_NARROW_SOLVER`): The solver type to use for query execution.
- `solver_config` (`SolverConfig`): Per-table column name mappings and filter configuration for
the solver.  Use this when your silver-layer tables use
non-default column names or when you need project/toolbox
scoping.  Key sub-fields:

- ``project_id`` (str): Top-level project filter value applied
  to container_tags and channel_mapping tables.
- Per-table sections (``container_tags``, ``channel_mapping``,
  ``channels``, etc.) each with ``column_name_mapping`` and
  ``filters`` dicts.

When omitted, all default column names are used and no
project/toolbox filtering is applied.

#### validate\_project\_id\_for\_key\_value\_store\_solver

```python
def validate_project_id_for_key_value_store_solver()
```

Validate that project_id is provided when using KeyValueStoreSolver.


#### validate\_drop\_implausible\_data\_requires\_raw

```python
def validate_drop_implausible_data_requires_raw()
```

`drop_implausible_data=True` currently only takes effect with RAW data.

The filter is applied inside the RAW -> RLE conversion path in
``IntervalEncoder.prepare_channels_df``. RLE input short-circuits that
path and the flag is silently ignored, so we reject the combination at
config validation time.


## IncrementalConfig

```python
class IncrementalConfig(BaseModel)
```

Configuration for incremental processing behavior.

**Arguments**:

- `enabled` (`bool, default=False`): Whether incremental processing is enabled.
- `silver_last_modified_column` (`str, default="timestamp"`): Column name in the silver layer used for freshness comparison.
- `gold_last_modified_column` (`str, default="last_modified"`): Column name in the gold layer used for freshness comparison.

## MdaConfig

```python
class MdaConfig(BaseModel)
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
     Optional query engine configuration. Defaults to Solvers.BASIC_NARROW_SOLVER.
 incremental : IncrementalConfig, optional
     Optional incremental processing configuration. Defaults to IncrementalConfig().
 measurement_dimensions : list of MeasurementDimensions, optional
     List of measurement dimensions to include in the configuration.
 Examples
 --------
>>> config_data = {
 ...     "source": {
 ...         "container_metrics_table": "mda_demo.silver.container_metric",
 ...         "channel_metrics_table": "mda_demo.silver.channel_metric",
 ...         "channels_uri": "mda_demo.silver.channel_data",
 ...         "channel_mapping_table": "mda_demo.data_model.channel_mapping"
 ...     },
 ...     "unity_sink": {
 ...         "catalog": "mda_demo",
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
 ...         "solver": "KeyValueStoreSolver",
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
 >>> config = MdaConfig.model_validate(config_data)


#### validate\_container\_tags\_for\_key\_value\_store\_solver

```python
def validate_container_tags_for_key_value_store_solver()
```

Validate that container_tags_table is provided when using KeyValueStoreSolver.


