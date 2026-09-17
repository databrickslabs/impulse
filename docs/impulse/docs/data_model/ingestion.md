---
sidebar_position: 2
title: Ingestion
---

# Ingestion

Impulse's [`DefaultSolver`](../references/query_engine/query_solvers.md) reads from a
silver layer of **three required tables**: `container_metrics`,
`channel_metrics`, and `channels`. Two further tables, `container_tags`
and `channel_tags`, are **fully optional** — add them only if you want
tag-based container filtering or EAV channel selection
(`query.channel(channel_name="Engine_RPM")`); without `channel_tags`,
channels are selected directly from columns on `channel_metrics`. The full
schema is on the [Silver Layer ER Diagram](silver_layer_schema.md). This page
is for engineers who already have measurement data (CSV, MDF4, a
vendor-specific binary, or Delta with a different shape) and need a starting
point for landing it in that layout.

Impulse does not ship an ingestion component. The library reads from the
silver layer; producing it is your responsibility. **Landing your data in
the shape below during ingest is the simplest path.** If reshaping is
impractical for your situation, see
[Adapting to existing data layouts](#adapting-to-existing-data-layouts) at
the bottom of this page.

:::tip Column-name mapping

If your data already lives in Delta with different physical column names
than the contract below, you do not need to rewrite it. Impulse supports a
per-table physical-to-internal column-name mapping for every silver table
via `SolverConfig`. See
[Column-name remapping with `SolverConfig`](#column-name-remapping-with-solverconfig)
below.

:::

---

## 1. The contract

The full schema is on the [ER diagram page](silver_layer_schema.md). When
ingesting your own data, the required invariants are:

- **`container_id` is the primary key on `container_metrics`** and the
  foreign key on every other table. One container is one recording (one
  test drive, one bench run, one telemetry session). Pick a stable
  integer/long ID per recording.
- **`(container_id, channel_id)` identifies a channel within a container.**
  Channel IDs are local to their container — `channel_id = 1` in container A
  has nothing to do with `channel_id = 1` in container B.
- **`channels` supports two formats.** The query engine accepts either:
  - **Raw** — one row per sample: `(container_id, channel_id, timestamp,
    value)`. Set `data_type: "RAW"` in the report config; the engine
    converts the samples to intervals at query time, either by run-length
    encoding them (`raw_encoder: "RLE"`, the default) or by plain interval
    derivation without merging equal-valued runs (`raw_encoder: "INTERVAL"`)
    — see [query_engine](../config/configuration.md#query_engine-optional).
    Impulse considers a time series **valid within the calculated
    intervals**, which is relevant for operations like time-series
    synchronization — see
    [How Impulse interprets intervals](silver_layer_schema.md#raw-format).
  - **RLE** — one row per stable interval: `(container_id, channel_id,
    tstart, tend, value)`. Run-length encoded data, where identical consecutive values are collapsed into intervals to significantly reduce processing time during analysis.

  An optional boolean `is_plausible` column lets the solver drop implausible
  samples when configured to (`drop_implausible_data=True` on `DefaultSolver`).

The **tag tables are optional, strict EAV** — add them only if you want
tag-based selection. `container_tags` is `(container_id, key, value)`;
`channel_tags` is `(container_id, channel_id, key, value)`. TSAL then selects
recordings and signals by tag key, e.g. `query.channel(channel_name="Engine_RPM")`
looks up `channel_tags.value` where `key = 'channel_name'`. Without
`channel_tags`, channel selectors match columns on `channel_metrics` instead.

The remaining columns on `container_metrics` and `channel_metrics`
(timestamps, durations, mean/min/max, etc.) are *not* fixed by the engine —
they are surfaced into the gold-layer dimensions through your
[report configuration](../config/configuration.md). Add the columns your
queries need; you do not have to match the demo schema column-for-column.

---

## 2. Worked example: the demo CSVs

The repository ships pre-shaped silver-layer fixtures at
[`demos/data/reporting/`](https://github.com/databrickslabs/impulse/tree/main/demos/data/reporting):

```
container_metrics.csv
container_tags.csv
channel_metrics.csv
channel_tags.csv
channels.csv     # raw format: (container_id, channel_id, timestamp, value)
```

The Getting Started notebook
([`demos/getting_started.ipynb`](https://github.com/databrickslabs/impulse/blob/main/demos/getting_started.ipynb))
loads them into Delta tables in five lines:

```python
import os, pandas as pd
csv_dir = os.path.join(DEMOS_DIR, "data", "reporting")
for t in ["container_metrics", "container_tags",
          "channel_metrics", "channel_tags", "channels"]:
    (spark.createDataFrame(pd.read_csv(f"{csv_dir}/{t}.csv"))
          .write.mode("overwrite")
          .saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE_PREFIX}_{t}"))
```

If your data is already in this shape, that is your ingestion. The rest of
this page is for the cases where it isn't.

---

## 3. The general pipeline shape

Real-world ingestion of measurement data on Databricks tends to follow the
same skeleton, regardless of input format:

1. **File detection.** Raw files arrive in a Unity Catalog Volume. Use
   [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader)
   (`cloudFiles`) to detect them and append a discovery row to a `status`
   Delta table you control.
2. **Format-specific decode.** A Spark job picks up unprocessed rows from
   `status`, opens each file with the appropriate reader (asammdf for MDF4,
   the CSV reader for CSV, a vendor SDK for proprietary binary), and writes
   decoded numeric samples to a **bronze** Delta table.
3. **Bronze → silver.** Either write samples directly as raw `channels`, or
   collapse consecutive identical samples per `(container_id, channel_id)`
   into intervals (RLE). Derive the four metadata tables (`*_tags`,
   `*_metrics`) from per-recording and per-channel attributes captured during
   decode.
4. **Run-status tracking.** Mark each `run_id` succeeded or failed in
   `status`. On failure, roll back any partial silver writes for that
   `run_id` so the silver layer stays transactional with respect to source
   files.
5. **Maintenance.** Periodically `OPTIMIZE` the silver tables. `channels`
   is by far the largest — cluster or Z-order it on `container_id`,
   `channel_id`.

This is a pattern, not a recipe. Implement only the steps your situation
needs (e.g. one-shot loads can skip Auto Loader and the `status` table
entirely).

---

## 4. Format-specific notes

### CSV

The five-line loader in section 2 works as-is when the CSVs already match
the silver-layer shape. If your CSV uses different column names, rename
them in a `select(...)` before `saveAsTable`. If columns are spread across
multiple files (e.g. one CSV per signal), reshape during decode so each
container's samples land in `channels` together.

### MDF4 (ASAM)

A Databricks solutions accelerator for ingesting raw MDF4 data into the
silver-layer model is in preparation. The pattern below describes the
underlying approach.

Decode each file with [asammdf](https://github.com/danielhrisca/asammdf) in
a Spark UDF. For each numeric channel, emit
`(container_id, channel_id, timestamp, value)` rows into a bronze Delta
table, then run a Spark job that derives `channels` (raw or RLE) and the
metadata tables. Honor MDF4's per-sample invalidation bits — drop or mark
invalid samples before RLE encoding (the `is_plausible` column on `channels`
is the natural place to record them).

### Already in Delta but in a different shape

Write a one-shot ETL: `SELECT` from your existing tables and `saveAsTable`
into the five silver tables. The most common gap is missing tags. If your
source data carries metadata as wide columns on the recordings table
(`vehicle_brand`, `vehicle_model`, ...), unpivot them into
`(container_id, key, value)` rows before writing to `container_tags`.

### Vendor-specific binary

The MDF4 pattern generalises: decode with the vendor SDK, emit numeric
samples to bronze, collapse to silver. If the vendor SDK is not Spark-native,
run the decode in a `mapPartitions` UDF and accept that the decode stage is
your throughput bottleneck.

---

## Adapting to existing data layouts

Reshaping into the silver-layer shape during ingest is the recommended
path for new deployments. If your data already lives in Delta tables with
different column names or a fundamentally different layout — and rewriting
that data is impractical — Impulse offers two escape hatches.

### Column-name remapping with `SolverConfig`

[`SolverConfig`](../references/api/impulse_query_engine/analyze/query/solvers/solver_config.md)
declares **per-table** mappings from your physical column names to the
engine's internal names (`container_id`, `channel_id`, `tstart`, `tend`,
`value`, `key`, ...). Each silver table has its own `TableConfig`
section with a `column_name_mapping` dict and an optional `filters` dict
for equality scoping (project/toolbox/etc.). The mapping is applied
**once**, when each table is read; everything downstream uses the
internal names.

Use this when the **logical shape** of your silver layer matches
Impulse's expectations — same set of tables and relationships — but the
**column names** differ. See
[Solver column mappings and filters](../config/configuration.md#solver-column-mappings-and-filters)
for the full schema.

Set `query_engine.solver_config` in your report config. `DefaultSolver`
consumes every section that applies to the tables you have configured —
column mappings, per-table `filters`, `project_id`, and the
`channel_mapping` / `unit_conversion` sections.

Trade-off either way: this gives you naming flexibility and per-table
scoping filters without writing code, but the underlying tables must
still follow the silver-layer relationships (EAV tag tables,
per-`(container_id, channel_id)` channels rows, etc.) and the internal
key names (`container_id`, `channel_id`) themselves are fixed
constants. Their column *types* are not fixed, though — `container_id`
in particular may be a `long`, `int`, or `string`; the engine adopts
whatever type your tables use, as long as it is consistent across them.
For different relationships or composite keys, see custom solvers below.

### Custom solvers

For physical layouts that do not match the silver-layer relationships at
all — no EAV tag tables, alias lookup tables instead of `channel_tags`,
computed-column joins, JSON-encoded values, multi-column composite keys
that need pre-processing, etc. — you can implement a custom solver by
subclassing
[`QuerySolver`](../references/api/impulse_query_engine/analyze/query/solvers/query_solver.md)
(or one of the existing solvers, usually `DefaultSolver`) and selecting it
by name in your report config.

This is significantly more invested than the `SolverConfig` path: you take
on responsibility for the four solver pipeline stages
(`filter_container_tags`, `filter_container_metrics`, `filter_channel_tags`,
`filter_channel_metrics`) and the `solve` method. Some advanced deployments
do this — e.g. when the customer's silver layer pre-dates Impulse and
synthesises Impulse-shaped views via SQL CTEs at query time. If you find
yourself heading down this path, it is usually worth first asking whether
a one-time ETL job to produce the standard silver-layer shape would be
cheaper.

#### Registering a custom solver

Decorate your subclass with `@register_solver(name)`. The report config then
selects it by that name via the existing `query_engine.solver` field — the
same field used for the built-in `DefaultSolver`.

```python
from impulse_query_engine.analyze.query.solvers import (
    register_solver,
    SolverConfig,
)
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver


class CustomSolverConfig(SolverConfig):
    raw_signal_table: str            # required — validated when the config is parsed
    position_signal_name: str = "POSITION"  # optional, with a default


@register_solver("CustomSolver", CustomSolverConfig)
class CustomSolver(DefaultSolver):
    """Reshapes a custom raw table into the Impulse silver schema."""
    ...
```

```json
{
  "query_engine": {
    "solver": "CustomSolver",
    "solver_config": {
      "raw_signal_table": "my_catalog.my_schema.raw_signals",
      "channels": { "column_name_mapping": { "signal_value": "value" } }
    }
  }
}
```

Passing `CustomSolverConfig` to `register_solver` makes Impulse validate the
`solver_config` block through that subclass, so its extra fields are checked
at config-load time: a missing required `raw_signal_table`, or a mistyped
key, raises a `ValidationError` up front rather than being silently dropped.
Inside the solver you read them with normal typed attribute access
(`self.config.raw_signal_table`).

**Loading the solver.** `@register_solver` runs only when its module is
imported, so import your solver package once in the driver before building
the report — that import is what registers the name:

```python
import sys
sys.path.append("/Workspace/Repos/me/my_solvers_root")  # dir containing my_solvers/

import my_solvers.custom_solver       # runs @register_solver("CustomSolver")

from impulse_reporting.core.report import Report

report = Report(name="my_report", spark=spark, workspace_client=ws,
                config_path="report_config.json")
report.determine_report()
```

If the name is not registered when the config is parsed, Impulse raises an
error listing the known names — a quick signal that the import is missing or
misspelled. (For an installed wheel the `sys.path` line is unnecessary; the
`import` alone suffices. In a notebook you can also just define the solver in
a cell — running that cell registers it.)

Selection is by registered name only: a report config can never, on its own,
cause Impulse to import an arbitrary class. Which solvers exist is governed
entirely by what the driver imports.

#### Example: a column-redaction solver

A solver override plus one config field gives a config-switched masking layer:

```python
import pyspark.sql.functions as F
from impulse_query_engine.analyze.query.solvers import register_solver, SolverConfig
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver


class RedactConfig(SolverConfig):
    redact_columns: list[str] = []


@register_solver("RedactingSolver", RedactConfig)
class RedactingSolver(DefaultSolver):
    def solve(self, query, channels_df, selections, dtypes):
        df = super().solve(query, channels_df, selections, dtypes)
        for col in self.config.redact_columns:
            if col in df.columns:
                df = df.withColumn(col, F.lit(None).cast(df.schema[col].dataType))
        return df
```

```json
{
  "query_engine": {
    "solver": "RedactingSolver",
    "solver_config": { "redact_columns": ["vin", "gps_lat", "gps_lon"] }
  }
}
```

The general rule: **`SolverConfig` for naming differences, custom solver
for structural differences, ETL into the standard shape for everything
else.**
