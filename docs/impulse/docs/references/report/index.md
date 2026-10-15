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
| `add_event(event)`                  | Registers an `Event` with the report. All events used by aggregations must be registered. Event names must be unique (ignoring case). | `event`: `Event` instance.                                                                      |
| `add_calculated_channel(channel)`   | Registers a [`CalculatedChannel`](./channel.md) with the report.                                                              | `channel`: `CalculatedChannel` instance.                                                        |
| `get_calculated_channels()`         | Returns the list of registered calculated channels.                                                                           | --                                                                                              |
| `run(is_incremental, persist_results, cleanup_temp_tables)` | Computes all events, aggregations, calculated channels, and container dimensions and writes them (fact and dimension tables) to the configured Gold layer sink. With `persist_results=False`, results are kept on the report object instead. On a capped incremental run it loops until all containers are processed, stopping early with a warning if a batch makes no forward progress. | `is_incremental`: `bool` or `None`. Mode hint; overridden by `config.incremental` when present. See [Incremental processing](#incremental-processing). `persist_results`: `bool`, default `True`. `False` computes without writing. `cleanup_temp_tables`: `bool` or `None`. When truthy, drops the batch-solving `__impulse_temp_*` tables after a successful write; `None` (default) falls back to `config.unity_sink.cleanup_temp_tables`. |

### Execution workflow

```python
# 1. Define signals, events, aggregations (see sections below)
# 2. Add events and pages
my_report.add_event(my_event)
my_report.add_page(page)

# 3. Compute and persist
my_report.run()
```

`run()` validates that every event referenced by an aggregation has been registered with `add_event()`
before computation begins. The aggregation must reference the registered event object itself; a re-created event with
the same name is rejected.

:::note Deprecated: `determine_report()` / `persist_results()`
The two-step `determine_report()` + `persist_results()` still works but emits a `FutureWarning` and will be
removed in a future release. Replace the pair with `run()`, and a standalone `determine_report()` with
`run(persist_results=False)`. Unlike `run()`, a single `determine_report()` + `persist_results()` pass does not
drain a capped incremental run. The `persist_results` argument of `run()` is a flag and is unrelated to the
deprecated method of the same name.
:::

### Sinkless mode

If no `unity_sink` is configured, the report runs without persistence. `run()` still computes events,
aggregations, and dimensions and exposes them on the report object, but writes nothing. To compute without writing
while a sink is configured, call `run(persist_results=False)`. This is useful for ad-hoc analysis, notebooks, and
tests where writing to Unity Catalog is not desired.

---

## Configuration

Configuration is defined as JSON (or an equivalent Python dictionary) and
validated using Pydantic models. See
[Configuration](../../config/configuration.md) for the full schema.

## Incremental processing

Incremental processing lets `run()` skip containers and
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
3. Otherwise the `is_incremental` argument to `run()` wins.
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
| `StatsAggregator`   | `input_expressions`, `statistics`, `event`, `channel_names`, custom statistics (labels, functions, inputs, params, cross-channel `channel_name`) |
| `CalculatedChannel` | `expr` string + `identity`                                  |

Renaming an aggregation, tweaking the description, or changing units keeps the hash stable. No reprocessing. `channel_names` (and a cross-channel statistic's `channel_name`) **do** affect the hash for `StatsAggregator` / `PointValueAggregator`, because they are the fact table's `channel_name` merge key — renaming forces a recompute so old-name rows are pruned rather than left stale.

#### Forcing a full recalculation

To recompute specific entities over all containers even when their `definition_hash` is unchanged, list their names under [`full_recalculation`](../../config/configuration.md#full_recalculation-optional) in config. The listed aggregations, events, and calculated channels are treated as **changed** for that run — they recompute over all matching containers and their gold rows are fully replaced — while every other entity stays incremental. This is meant for backfills after a persistence fix or corrected upstream data. In full mode it is a no-op, and a name that matches no registered entity fails the run fast.

#### Container-update detection

`ContainerUpsertDetector.detect_upserted_containers` finds two things and unions them:

- **New containers**: silver rows that don't exist in gold (left anti-join on `container_id`).
- **Updated containers**: silver rows where `silver_last_modified_column` is newer than the matching gold `gold_last_modified_column`. If either column is missing from its side, update detection is silently skipped and only new containers get picked up.

Detection reads `container_metrics` with `column_name_mapping` applied, so `container_id` and `silver_last_modified_column` are matched against the post-mapping frame (the physical names unless you remap those columns). A remapped container-id column therefore works in incremental mode.

#### Operational notes

- A single run can be partly incremental: one event is changed (full reprocess), another is unchanged (upserted containers only), a newly added aggregation is brand new (also full reprocess). Each entity walks its own path, but all of an entity's rows land in **one** `MERGE` per fact table (entity types that share a fact table are combined), so there is no intermediate inconsistent state.
- The `MERGE` updates matched rows and inserts new ones, and also **deletes stale rows** (`whenNotMatchedBySourceDelete`) — but only within a bounded scope: the updated containers and the changed-definition entity ids. A modified container that now produces *fewer* rows (e.g. an event that fires fewer times, or an event-scoped statistic with fewer instances) has its surplus rows removed rather than left behind as orphans.
- Rows outside that scope are never touched: containers that weren't reprocessed, and entities whose definition didn't change, keep their existing gold rows untouched. New containers carry no delete scope (they have no prior gold rows) — their rows are simply inserted.
- The `MERGE` runs with schema evolution, so a fact-table schema change across releases doesn't break an incremental run: a newly added column is added to the gold table (existing rows get NULL). A column dropped from the schema is *retained* in gold (new rows get NULL for it) — Delta doesn't drop columns via MERGE, so a genuine column removal or an incompatible type change still needs a full (non-incremental) rebuild.
