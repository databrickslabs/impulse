---
name: impulse-reporting
description: >
  Build and run an Impulse reporting pipeline that persists events and aggregations to the gold-layer
  star schema. Use when the user wants to "build an Impulse report", compute histograms/statistics
  across all recordings and write them to Delta tables for a dashboard or scheduled job, or run
  incrementally so only new/changed data is reprocessed. Covers the Report/Page lifecycle
  (`add_event`, `add_page`, `determine_report`, `persist_results`), incremental processing, and
  sinkless mode.
---

# Impulse — reporting pipeline

The reporting mode computes events and aggregations across all matching recordings and persists them
to the gold-layer star schema (see `impulse-data-model`), ready for AI/BI dashboards or Lakehouse apps.
`Report` (from `impulse_reporting.core.report`) is the orchestrator.

This skill covers the lifecycle. The pieces plug in from sibling skills: **`impulse-config`** (the
config dict), **`impulse-tsal`** (channel selection and signals), **`impulse-events`** (event windows),
and **`impulse-aggregations`** (histograms and statistics).

## Creating a Report

```python
from databricks.sdk import WorkspaceClient
from impulse_reporting.core.report import Report

ws = WorkspaceClient()

# From a dict
report = Report(name="my_report", spark=spark, workspace_client=ws, config=config_dict)

# From a JSON file
report = Report(name="my_report", spark=spark, workspace_client=ws, config_path="./config/config.json")
```

| Parameter          | Type              | Description                                            |
|--------------------|-------------------|--------------------------------------------------------|
| `name`             | `str`             | Report name; used to derive a unique `report_id`.      |
| `spark`            | `SparkSession`    | Active Spark session (present in Databricks notebooks). |
| `workspace_client` | `WorkspaceClient` | Authenticated Databricks SDK client.                   |
| `config`           | `dict`            | Config dict (or use `config_path`).                    |
| `config_path`      | `str`             | Path to a JSON config file (or use `config`).          |

Exactly one of `config` / `config_path` must be provided.

Useful methods: `get_db()` → the `MeasurementDB` for signal selection; `get_solver()` → the configured
solver; `get_sink_config()` → the resolved sink; `add_event(event)`; `add_page(page)`;
`determine_report(is_incremental=None)`; `persist_results()`.

## The lifecycle

```python
from impulse_reporting.core.page import Page
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.aggregations.histogram import HistogramDuration

# 1. Select channels and derive signals (impulse-tsal)
db = report.get_db()
eng_rpm = db.query.channel(channel_name="Engine RPM", brand="Seat", model="Leon")
veh_spd = db.query.channel(channel_name="Vehicle Speed Sensor")

# 2. Define events and REGISTER them (impulse-events)
rpm_band = BasicEvent(name="rpm_band", expr=(eng_rpm > 2000) & (eng_rpm < 5000))
report.add_event(rpm_band)

# 3. Add a page and its aggregations (impulse-aggregations)
page = Page(page_number=1)
report.add_page(page)
page.add_aggregation(HistogramDuration(
    name="rpm_hist", base_expr=eng_rpm,
    bins=[0, 1000, 2000, 3000, 4000, 5000], event=rpm_band,
    channel_name="Engine RPM", bins_unit="rpm", values_unit="s",
))

# 4. Compute everything (in parallel across containers)
report.determine_report()

# 5. Write the gold star schema
report.persist_results()
```

`determine_report()` validates that **every event referenced by an aggregation has been registered
with `add_event()`** before computing — otherwise it raises. Register events even when an aggregation
is the only thing that uses them.

After `determine_report()`, computed results are also available on the report object without a write —
`report.aggregation_dfs[...]` (used by `impulse-ml`) — which is what makes sinkless mode useful.

## Sinkless mode (compute without writing)

Omit `unity_sink` from the config. `determine_report()` still computes events, aggregations, and
dimensions and exposes them on the report; `persist_results()` becomes a no-op. Good for notebooks,
tests, and feeding ML (see `impulse-ml`). Reading a DataFrame straight out of the query engine without
any report at all is `impulse-analyze`.

## Incremental processing

Reuses prior results for unchanged definitions and reprocesses only containers that are new or updated
in silver. Turn it on via `incremental.enabled` in config (see `impulse-config`) or pass
`is_incremental=True` to `determine_report()`.

**Mode resolution** (in order):

1. No gold layer yet → full run (nothing to compare against). The first run of a report is always full.
2. `config.incremental` set → `config.incremental.enabled` wins.
3. Otherwise the `is_incremental` argument wins.
4. Neither set → full.

**On each run:** each event/aggregation is compared against its stored `definition_hash`. Unchanged
definitions reprocess only new/updated containers (persisted via Delta `MERGE` on natural keys);
changed or brand-new definitions reprocess all matching containers (replaced atomically via
`replaceWhere` on `visual_id`/`event_id`). A single run can mix both per entity.

**What counts as a definition change** — only the hashed attributes; renames, descriptions, and units
are cosmetic and do not trigger reprocessing:

| Type              | Hashed                                             |
|-------------------|----------------------------------------------------|
| `BasicEvent`      | `expr` string                                      |
| `ContainerEvent`  | `name`                                             |
| `Histogram`       | `base_expr`, `bins`, `event`                       |
| `Histogram2D`     | `x_expr`, `y_expr`, `x_bins`, `y_bins`, `event`    |
| `StatsAggregator` | `input_expressions`, `statistics`, `event`         |

**Container-update detection** unions two sets: new containers (silver rows absent from gold) and
updated containers (silver `silver_last_modified_column` newer than the gold `gold_last_modified_column`).
If either column is missing on its side, update detection is skipped and only new containers are picked
up.

## Scheduling

A report pipeline is plain Python — put the lifecycle in a notebook or `.py` task and schedule it as a
Databricks Workflow to keep the gold layer fresh. Incremental mode makes repeated runs cheap.
