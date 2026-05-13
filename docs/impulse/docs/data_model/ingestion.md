---
sidebar_position: 2
title: Ingestion
---

# Ingestion

Impulse's [default solvers](../references/query_engine.md) read from a
silver layer composed of a minimum of three tables: `container_metrics`,
`channel_metrics`, and `channels`. Two additional tables, `container_tags`
and `channel_tags`, are optional but strongly recommended. They carry the contextual metadata that the
user-facing channel selection API (`query.channel(channel_name="Engine_RPM")`)
and tag-based container filtering rely on. The full schema is on the
[Silver Layer ER Diagram](silver_layer_schema.md). This page is for engineers
who already have measurement data (CSV, MDF4, a vendor-specific binary, or
Delta with a different shape) and need a starting point for landing it in
that layout.

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
ingesting your own data, four invariants matter most:

- **`container_id` is the primary key on `container_metrics`** and the
  foreign key on every other table. One container is one recording (one
  test drive, one bench run, one telemetry session). Pick a stable
  integer/long ID per recording.
- **`(container_id, channel_id)` identifies a channel within a container.**
  Channel IDs are local to their container — `channel_id = 1` in container A
  has nothing to do with `channel_id = 1` in container B.
- **Tag tables are strict EAV.** `container_tags` is `(container_id, key,
  value)`; `channel_tags` is `(container_id, channel_id, key, value)`. TSAL
  selects recordings and signals by tag key, e.g. `query.channel(channel_name=
  "Engine_RPM")` looks up `channel_tags.value` where `key = 'channel_name'`.
  If a key is not in the tag table, no expression can find it.
- **`channels` supports two formats.** The query engine accepts either:
  - **Raw** — one row per sample: `(container_id, channel_id, timestamp,
    value)`.
  - **RLE** — one row per stable interval: `(container_id, channel_id,
    tstart, tend, value)`. Run-length encoded data, where identical consecutive values are collapsed into intervals to significantly reduce processing time during analysis.

  An optional boolean `is_plausible` column lets the solver drop implausible
  samples when configured to (`drop_implausible_data=True` on `DeltaSolver`).

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

[`SolverConfig`](../references/api/mda_query_engine/analyze/query/solvers/solver_config.md)
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

How it gets wired in depends on which solver you use:

- **`KeyValueStoreSolver`** and **`DeltaSolver`** — set
  `query_engine.solver_config` in your report config. The `Report`
  factory forwards it to both solvers. `KeyValueStoreSolver` consumes
  every section (column mappings, per-table `filters`, `project_id`,
  `channel_mapping`); `DeltaSolver` consumes only the per-table
  `column_name_mapping` entries and silently ignores the rest.

Trade-off either way: this gives you naming flexibility and per-table
scoping filters without writing code, but the underlying tables must
still follow the silver-layer relationships (EAV tag tables,
per-`(container_id, channel_id)` channels rows, etc.) and the internal
key names (`container_id`, `channel_id`) themselves are fixed
constants. For different relationships or composite keys, see custom
solvers below.

### Custom solvers

For physical layouts that do not match the silver-layer relationships at
all — no EAV tag tables, alias lookup tables instead of `channel_tags`,
computed-column joins, JSON-encoded values, multi-column composite keys
that need pre-processing, etc. — you can implement a custom solver by
subclassing
[`QuerySolver`](../references/api/mda_query_engine/analyze/query/solvers/query_solver.md)
(or one of the existing solvers) and registering it in your report config.

This is significantly more invested than the `SolverConfig` path: you take
on responsibility for the four solver pipeline stages
(`filter_container_tags`, `filter_container_metrics`, `filter_channel_tags`,
`filter_channel_metrics`) and the `solve` method. Some advanced deployments
do this — e.g. when the customer's silver layer pre-dates Impulse and
synthesises Impulse-shaped views via SQL CTEs at query time. If you find
yourself heading down this path, it is usually worth first asking whether
a one-time ETL job to produce the standard silver-layer shape would be
cheaper.

The general rule: **`SolverConfig` for naming differences, custom solver
for structural differences, ETL into the standard shape for everything
else.**
