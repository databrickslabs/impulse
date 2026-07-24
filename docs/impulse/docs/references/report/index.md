---
title: Report
---

# Report

The `Report` class is the central entry point for defining and executing an analysis. It loads configuration, sets up
the data source and sink, and orchestrates computation and persistence.

### Creating a Report

```python
from databricks.sdk import WorkspaceClient
from impulse_reporting.core.report import Report

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
| `add_calculated_channel(channel)`   | Registers a [`CalculatedChannel`](./channel.md) with the report.                                                              | `channel`: `CalculatedChannel` instance.                                                        |
| `get_calculated_channels()`         | Returns the list of registered calculated channels.                                                                           | --                                                                                              |
| `determine_report(is_incremental)`  | Computes all events, aggregations, calculated channels, and container dimensions. Results are stored on the report object.     | `is_incremental`: `bool` or `None`. Mode hint; overridden by `config.incremental` when present. See [Incremental processing](#incremental-processing). |
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

Configuration is defined as JSON (or an equivalent Python dictionary) and
validated using Pydantic models. See
[Configuration](../../config/configuration.md) for the full schema.

## Incremental processing

Incremental processing lets `determine_report()` skip containers and
definitions it already handled on a prior run. Turn it on via
`incremental.enabled` in config (see
[Configuration › incremental](../../config/configuration.md#incremental-optional)),
or pass `is_incremental=True` at call time.

#### On each run

1. Compare every event and aggregation against its stored `definition_hash` in the gold dimension table. Classify each as **changed** (hash differs, or it's brand new) or **unchanged** (hash matches).
2. For unchanged definitions, process only the containers that are new or have newer silver data than gold. Skip the rest.
3. For changed definitions, reprocess all containers that match the report's filters.
4. Persist each fact table with a single Delta `MERGE` on its natural keys. The changed rows (all containers) and unchanged rows (reprocessed containers) are written together, and stale rows within the reprocessed scope are deleted in the same transaction (see [Operational notes](#operational-notes)).

#### Mode resolution

`Report._resolve_is_incremental` picks the mode in this order:

1. No gold layer yet? Run full. Nothing to compare against.
2. `config.incremental` set? `config.incremental.enabled` wins.
3. Otherwise the `is_incremental` argument to `determine_report()` wins.
4. Neither set? Run full.

The first run of a new report is always full. Subsequent runs pick up where the last one left off.

#### What counts as a definition change

Only the hashed attributes matter. Anything else is cosmetic and won't trigger reprocessing.

| Type                | Hashed                                                      |
|---------------------|-------------------------------------------------------------|
| `BasicEvent`        | `expr` string                                               |
| `ContainerEvent`    | `name`                                                      |
| `Histogram`         | `base_expr`, `bins`, `event`                                |
| `Histogram2D`       | `x_expr`, `y_expr`, `x_bins`, `y_bins`, `event`             |
| `StatsAggregator`   | `input_expressions`, `statistics`, `event`                  |
| `CalculatedChannel` | `expr` string + `identity`                                  |

Renaming an aggregation, tweaking the description, or swapping `channel_name` or units keeps the hash stable. No reprocessing.

#### Container-update detection

`ContainerUpsertDetector.detect_upserted_containers` finds two things and unions them:

- **New containers**: silver rows that don't exist in gold (left anti-join on `container_id`).
- **Updated containers**: silver rows where `silver_last_modified_column` is newer than the matching gold `gold_last_modified_column`. If either column is missing from its side, update detection is silently skipped and only new containers get picked up.

#### Operational notes

- A single run can be partly incremental: one event is changed (full reprocess), another is unchanged (upserted containers only), a newly added aggregation is brand new (also full reprocess). Each entity walks its own path, but all of an entity's rows land in **one** `MERGE` per fact table (entity types that share a fact table are combined), so there is no intermediate inconsistent state.
- The `MERGE` updates matched rows and inserts new ones, and also **deletes stale rows** (`whenNotMatchedBySourceDelete`) — but only within a bounded scope: the updated containers and the changed-definition entity ids. A modified container that now produces *fewer* rows (e.g. an event that fires fewer times, or an event-scoped statistic with fewer instances) has its surplus rows removed rather than left behind as orphans.
- Rows outside that scope are never touched: containers that weren't reprocessed, and entities whose definition didn't change, keep their existing gold rows untouched. New containers carry no delete scope (they have no prior gold rows) — their rows are simply inserted.
