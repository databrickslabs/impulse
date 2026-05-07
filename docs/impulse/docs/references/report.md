---
sidebar_position: 1
title: Report
---

# Report

The `Report` class is the central entry point for defining and executing an analysis. It loads configuration, sets up
the data source and sink, and orchestrates computation and persistence.

### Creating a Report

```python
from databricks.sdk import WorkspaceClient
from mda_reporting.core.report import Report

ws = WorkspaceClient()

# From a JSON config file
my_report = Report(name="my_report", spark=spark, workspace_client=ws, config_path="./config/config.json")

# From a dictionary
my_report = Report(name="my_report", spark=spark, workspace_client=ws, config=config_dict)
```

| Parameter          | Type              | Description                                                |
|--------------------|-------------------|------------------------------------------------------------|
| `name`             | `str`             | Name of the report. Used to generate a unique `report_id`. |
| `spark`            | `SparkSession`    | Active Spark session for data processing.                  |
| `workspace_client` | `WorkspaceClient` | Authenticated Databricks workspace client.                 |
| `config`           | `dict`, optional  | Configuration as a Python dictionary.                      |
| `config_path`      | `str`, optional   | Path to a JSON configuration file.                         |

Either `config` or `config_path` must be provided.

### Report methods

| Method                              | Description                                                                                                                    | Arguments                                                                                       |
|-------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| `get_db()`                          | Returns the `MeasurementDB` instance for signal definition.                                                                    | --                                                                                              |
| `get_solver()`                      | Returns the active `QuerySolver`.                                                                                              | --                                                                                              |
| `get_sink_config()`                 | Returns the active `SinkConfig` (e.g. `UnitySinkConfig` with `catalog_name`, `schema_name`, `table_prefix`).                   | --                                                                                              |
| `add_page(page)`                    | Adds a `Page` to the report.                                                                                                   | `page`: `Page` instance.                                                                        |
| `add_event(event)`                  | Registers an `Event` with the report. All events used by aggregations must be registered.                                      | `event`: `Event` instance.                                                                      |
| `determine_report(is_incremental)`  | Computes all events, aggregations, and container dimensions. Results are stored on the report object.                          | `is_incremental`: `bool` or `None`. Mode hint; overridden by `config.incremental` when present. See [Incremental](#incremental-optional). |
| `persist_results()`                 | Writes all computed results (fact and dimension tables) to the configured Gold layer sink.                                     | --                                                                                              |

### Execution workflow

```python
# 1. Define signals, events, aggregations (see sections below)
# 2. Add events and pages
my_report.add_event(my_event)
my_report.add_page(page)

# 3. Compute
my_report.determine_report()

# 4. Persist
my_report.persist_results()
```

`determine_report()` validates that every event referenced by an aggregation has been registered with `add_event()`
before computation begins.

### Sinkless mode

If no `unity_sink` is configured, the report runs without persistence. `determine_report()` still computes events,
aggregations, and dimensions and exposes them on the report object, but `persist_results()` becomes a no-op. This is
useful for ad-hoc analysis, notebooks, and tests where writing to Unity Catalog is not desired.

---

## Configuration

Configuration is defined as JSON (or an equivalent Python dictionary) and validated using Pydantic models.

### Configuration schema

```json
{
  "source": {
    "container_metrics_table": "catalog.schema.container_metrics",
    "channel_metrics_table": "catalog.schema.channel_metrics",
    "channels_uri": "catalog.schema.channels",
    "container_tags_table": "catalog.schema.container_tags",
    "channel_tags_table": "catalog.schema.channel_tags"
  },
  "unity_sink": {
    "catalog": "my_catalog",
    "schema": "gold",
    "table_prefix": "my_report"
  },
  "query_engine": {
    "solver": "DeltaSolver"
  },
  "container_filters": {
    "tag_filters": [
      [
        { "tag_name": "uut_id", "comparator": "==", "value": "ABC123", "cast_type": "string" }
      ]
    ],
    "metric_filters": [
      [
        { "column_name": "start_dt", "comparator": ">=", "value": "2025-04-27T05:20:54.000Z", "value_type": "timestamp" },
        { "column_name": "stop_dt",  "comparator": "<=", "value": "2025-04-27T06:00:00.000Z", "value_type": "timestamp" }
      ]
    ]
  },
  "measurement_dimensions": ["container_id", "vehicle_key", "start_ts", "stop_ts"]
}
```

### Source

Defines the Silver layer input tables. All table names must follow Unity Catalog naming: `catalog.schema.table`.

| Field                     | Type  | Required | Description                                              |
|---------------------------|-------|----------|----------------------------------------------------------|
| `container_metrics_table` | `str` | Yes      | Container metadata (start/stop times, duration).         |
| `channel_metrics_table`   | `str` | Yes      | Channel-level statistics (min, max, mean, sample count). |
| `channels_uri`            | `str` | Yes      | Raw time-series data (timestamps + values).              |
| `container_tags_table`    | `str` | No       | Key-value tags for containers.                           |
| `channel_tags_table`      | `str` | No       | Key-value tags for channels.                             |

### Unity Sink

Defines where Gold layer output tables are written.

| Field          | Type  | Required | Description                           |
|----------------|-------|----------|---------------------------------------|
| `catalog`      | `str` | Yes      | Target Unity Catalog name.            |
| `schema`       | `str` | Yes      | Target schema name.                   |
| `table_prefix` | `str` | Yes      | Prefix for all generated table names. |

Output tables are named `{table_prefix}_{entity}`, for example `my_report_histogram_fact`.

### Query Engine

| Field                   | Type           | Default               | Description                                                                                                                 |
|-------------------------|----------------|-----------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `solver`                | `str`          | `"BasicNarrowSolver"` | `"BasicNarrowSolver"`, `"DeltaSolver"`, or `"KeyValueStoreSolver"`.                                                         |
| `data_type`             | `str`          | `"RLE"`               | `"RLE"` (intervals `[tstart, tend)`) or `"RAW"` (raw timestamps; converted to RLE before aggregation).                      |
| `drop_implausible_data` | `bool`         | `false`               | When `true`, drops rows where `is_plausible = false`. Requires `data_type = "RAW"`; combining with `"RLE"` raises a validation error. |
| `project_id`            | `str`          | `null`                | Required when `solver = "KeyValueStoreSolver"`.                                                                             |
| `parent_id`             | `str`          | `null`                | Optional parent-entity filter on the concept-entities table (KVS solver only), e.g. `"uut_concept"`.                        |
| `entity_maps_to`        | `str`          | `"uut_id"`            | How `entity_id` in concept-entities maps to `container_metrics`: `"uut_id"` (1-to-many) or `"container_id"` (1-to-1).       |
| `solver_config`         | `SolverConfig` | `null`                | Column-name overrides for custom silver schemas. See [SolverConfig](#solverconfig) below.                                   |

#### SolverConfig
Maps the solver's internal column names to the actual silver-layer column names. Only needed when the silver schema diverges from the defaults.

| Field                         | Type             | Default                                                    | Description                                                                                                                             |
|-------------------------------|------------------|------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `container_id_col`            | `str`            | `"container_id"`                                           | Column identifying a container.                                                                                                         |
| `channel_id_cols`             | `list[str]`      | `["container_id", "channel_id"]`                           | Columns that together uniquely identify a channel.                                                                                      |
| `channel_data_mapping`        | `dict[str, str]` | `{"tstart": "tstart", "tend": "tend", "value": "value"}`   | Maps internal keys (`tstart`, `tend`, `value`) to silver column names on `channels_uri`.                                                |
| `container_meta_data_mapping` | `dict[str, str]` | `{"project_id": "project_id"}`                             | Maps internal keys (`project_id`) to silver column names on `container_metrics_table`.                                                  |
| `entity_id_col`               | `str`            | `"entity_id"`                                              | Entity id column in concept-entities / KVS tables.                                                                                      |
| `parent_id_col`               | `str`            | `"parent_id"`                                              | Parent-entity id column in concept-entities / KVS tables.                                                                               |

### Incremental (optional)

Incremental processing lets `determine_report()` skip containers and definitions it already handled on a prior run. Turn it on via `incremental.enabled` in config, or pass `is_incremental=True` at call time.

#### On each run

1. Compare every event and aggregation against its stored `definition_hash` in the gold dimension table. Classify each as **changed** (hash differs, or it's brand new) or **unchanged** (hash matches).
2. For unchanged definitions, process only the containers that are new or have newer silver data than gold. Skip the rest.
3. For changed definitions, reprocess all containers that match the report's filters.
4. Persist via Delta `MERGE` on natural keys for unchanged definitions; replace atomically via `replaceWhere` on `visual_id` or `event_id` for changed ones.

#### Mode resolution

`Report._resolve_is_incremental` picks the mode in this order:

1. No gold layer yet? Run full. Nothing to compare against.
2. `config.incremental` set? `config.incremental.enabled` wins.
3. Otherwise the `is_incremental` argument to `determine_report()` wins.
4. Neither set? Run full.

The first run of a new report is always full. Subsequent runs pick up where the last one left off.

#### What counts as a definition change

Only the hashed attributes matter. Anything else is cosmetic and won't trigger reprocessing.

| Type              | Hashed                                                      |
|-------------------|-------------------------------------------------------------|
| `BasicEvent`      | `expr` string                                               |
| `ContainerEvent`  | `name`                                                      |
| `Histogram`       | `base_expr`, `bins`, `event`                                |
| `Histogram2D`     | `x_expr`, `y_expr`, `x_bins`, `y_bins`, `event`             |
| `StatsAggregator` | `input_expressions`, `statistics`, `event`                  |

Renaming an aggregation, tweaking the description, or swapping `channel_name` or units keeps the hash stable. No reprocessing.

#### Container-update detection

`ContainerUpsertDetector.detect_upserted_containers` finds two things and unions them:

- **New containers**: silver rows that don't exist in gold (left anti-join on `container_id`).
- **Updated containers**: silver rows where `silver_last_modified_column` is newer than the matching gold `gold_last_modified_column`. If either column is missing from its side, update detection is silently skipped and only new containers get picked up.

#### Config fields

| Field                         | Type   | Default         | Description                                                                           |
|-------------------------------|--------|-----------------|---------------------------------------------------------------------------------------|
| `enabled`                     | `bool` | `false`         | Turns incremental processing on.                                                      |
| `silver_last_modified_column` | `str`  | `"timestamp"`   | Silver-side column used to detect container updates.                                  |
| `gold_last_modified_column`   | `str`  | `"_created_at"` | Gold-side column used to detect prior-run freshness.                                  |

#### Operational notes

- A single run can be partly incremental: one event is changed (full reprocess), another is unchanged (upserted containers only), a newly added aggregation is brand new (also full reprocess). Each entity walks its own path.
- `replaceWhere` is atomic per fact table. When a definition changes, all rows for that `visual_id` or `event_id` get deleted and rewritten in one transaction. No intermediate inconsistent state, but there is a brief rewrite window.
- `MERGE` keeps existing rows that don't conflict, so unchanged definitions accumulate rows for new containers without rewriting the old ones.

### Container Filters (optional)

Restricts the set of containers that are processed. Filters are expressed in **disjunctive normal form** — each inner list is AND-combined, and the outer list is OR-combined. Two independent filter families:

- `tag_filters` — applied on `container_tags_table` (EAV key/value model).
- `metric_filters` — applied on `container_metrics_table` (columnar model).

```json
{
  "container_filters": {
    "tag_filters": [
      [
        { "tag_name": "uut_id", "comparator": "==", "value": "ABC123", "cast_type": "string" }
      ]
    ],
    "metric_filters": [
      [
        { "column_name": "start_dt", "comparator": ">=", "value": "2025-04-27T05:20:54.000Z", "value_type": "timestamp" },
        { "column_name": "stop_dt",  "comparator": "<=", "value": "2025-04-27T06:00:00.000Z", "value_type": "timestamp" }
      ]
    ]
  }
}
```

**TagFilter:**

| Field        | Type  | Required | Description                                                                         |
|--------------|-------|----------|-------------------------------------------------------------------------------------|
| `tag_name`   | `str` | Yes      | Tag key to filter on.                                                               |
| `comparator` | `str` | Yes      | One of `==`, `!=`, `>`, `>=`, `<`, `<=`.                                            |
| `value`      | any   | Yes      | Expected value; must match `cast_type`.                                             |
| `cast_type`  | `str` | No       | `string` (default), `int`, `double`, or `timestamp` (ISO-format string).            |

**MetricFilter:**

| Field         | Type  | Required | Description                                                                             |
|---------------|-------|----------|-----------------------------------------------------------------------------------------|
| `column_name` | `str` | Yes      | Column on `container_metrics_table` to filter on (e.g. `start_dt`, `stop_dt`, `uut_id`).|
| `comparator`  | `str` | Yes      | One of `==`, `!=`, `>`, `>=`, `<`, `<=`.                                                |
| `value`       | any   | Yes      | Expected value.                                                                         |
| `value_type`  | `str` | No       | When provided, validates/converts the value (`string`, `int`, `double`, `timestamp`).   |

### Measurement Dimensions

A list of dimension columns to include in the `measurement_dimension` table. Available values:

| Dimension          | Description                                    |
|--------------------|------------------------------------------------|
| `container_id`     | Container identifier.                          |
| `uut_id`           | Unit under test identifier.                    |
| `vehicle_key`      | Vehicle identifier.                            |
| `file_name`        | Source measurement file name.                  |
| `source_file_path` | Full path to the source file.                  |
| `start_ts`         | Measurement start timestamp.                   |
| `stop_ts`          | Measurement stop timestamp.                    |
| `environment`      | Recording environment (e.g. PUMA, datalogger). |
| `project_id`       | Project identifier.                            |

Default: `["container_id", "vehicle_key", "start_ts", "stop_ts"]`

