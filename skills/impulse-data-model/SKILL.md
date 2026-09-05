---
name: impulse-data-model
description: >
  Understand and prepare the data Impulse reads and writes. Use when the user asks "what tables does
  Impulse need", how to land / ingest measurement data into the silver layer, what the gold-layer
  output looks like, how fact and dimension tables join, or how to point Impulse at existing tables
  whose column names differ (via SolverConfig column mappings). Covers the three required silver
  tables, the optional tag/mapping/unit tables, RLE vs RAW channel formats, the gold star schema, and
  the SolverConfig / custom-solver escape hatches.
---

# Impulse — data model (silver input, gold output)

Impulse reads a **silver layer** of measurement tables and writes a **gold layer** star schema. It
does not ship an ingestion component — producing the silver layer is your responsibility, and landing
data in the shape below is the simplest path.

## Silver layer — the input Impulse reads

`DefaultSolver` needs **only three tables**. The rest are optional add-ons, used only when configured
in `source` (see `impulse-config`).

| Table               | Required? | Purpose                                                                                          |
|---------------------|-----------|--------------------------------------------------------------------------------------------------|
| `container_metrics` | **Yes**   | One row per recording — timestamps, duration, channel count, and any container-level columns.     |
| `channel_metrics`   | **Yes**   | One row per `(container_id, channel_id)` — per-channel statistics; also holds channel-selection columns (e.g. `channel_name`) in the wide model. |
| `channels`          | **Yes**   | The time-series sample data (RLE or RAW — see below).                                            |
| `poi_channels`      | Optional  | Points-in-Time (POI) channel data — a time series only defined at the given timestamps (discrete events). Add to select POI channels via `poi_channel()`. |
| `container_tags`    | Optional  | EAV `(container_id, key, value)`. Add for tag-based container filtering.                          |
| `channel_tags`      | Optional  | EAV `(container_id, channel_id, key, value)`. Add for EAV channel selection.                     |
| `channel_mapping`   | Optional  | Logical→physical channel alias table (enables `channel_with_alias()`).                           |
| `unit_conversion`   | Optional  | Per-unit conversion factors (used with `channel_mapping`).                                       |

### Key invariants when landing your own data

- **`container_id` is the primary key** on `container_metrics` and the foreign key everywhere else.
  One container = one recording. Pick a stable integer/long (or string) per recording; the engine
  adopts whatever type your tables use, as long as it is consistent across them.
- **`(container_id, channel_id)` identifies a channel within a container.** Channel IDs are local to
  their container.
- **`channels` supports two formats:**
  - **RAW** — one row per sample: `(container_id, channel_id, timestamp, value)`.
  - **RLE** — one row per stable interval: `(container_id, channel_id, tstart, tend, value)`.
    Run-length encoding collapses consecutive identical values into intervals and greatly reduces
    processing time. Set `query_engine.data_type` to `"RAW"` or `"RLE"` to match (see `impulse-config`).
  - An optional boolean `is_plausible` column lets the solver drop implausible samples when
    `drop_implausible_data=True` (requires RAW).
  - Extra columns on `channels` are ignored — the engine projects down to the columns above before
    solving, so it is safe to keep additional bookkeeping columns on the table.
- **`poi_channels` holds Points-in-Time (POI) data** — a value defined *only at* its timestamp. Schema: `(container_id long, channel_id int,
  timestamp long [epoch µs], value_double double nullable, value_string string nullable)`.
- **Tag tables are strict EAV.** `query.channel(channel_name="Engine RPM")` looks up
  `channel_tags.value` where `key = 'channel_name'`. Without `channel_tags`, channel selectors match
  columns on `channel_metrics` instead.
- The remaining metric columns (durations, min/max/mean, …) are **not fixed** by the engine — add the
  columns your queries and gold dimensions need. You do not have to match the demo schema column-for-column.

### Channel selection is metadata-driven

Channels are always selected by signal metadata (tags or `channel_metrics` columns), never by fixed
column positions — so the same schema supports arbitrary signal sets across projects. How the solver
resolves a selection depends on which optional tables you configured:

- With `channel_tags` configured → channels selected from its EAV rows (pivoted on the fly).
- Without it → channels selected from columns on `channel_metrics`.
- With `container_tags` configured → containers filtered from EAV rows; without it, from
  `container_metrics` columns (wide-only model).

Beyond filtering, these container tables also feed **container-level metadata into UDFs** at solve
time: a UDF declaring `container_metrics=[...]` (columns) or `container_tags=[...]` (EAV keys, which
require a `container_tags_table`) receives their per-container values as keyword arguments — see
`impulse-tsal`.

### Landing data (ingestion pattern)

If your CSVs already match the shape, loading is a few lines:

```python
import os, pandas as pd
csv_dir = "/Volumes/my_catalog/silver/raw/reporting"
for t in ["container_metrics", "container_tags", "channel_metrics", "channel_tags", "channels"]:
    (spark.createDataFrame(pd.read_csv(f"{csv_dir}/{t}.csv"))
          .write.mode("overwrite")
          .saveAsTable(f"my_catalog.silver.{t}"))
```

For real ingestion of MDF4 / vendor binaries, the typical Databricks skeleton is: detect files with
Auto Loader → decode per-file in a Spark UDF into a **bronze** samples table → collapse bronze into
`channels` (RAW or RLE) and derive the `*_metrics` / `*_tags` tables → track run status → periodically
`OPTIMIZE` (cluster/Z-order `channels` on `container_id, channel_id`, since it is by far the largest).
Implement only the steps your situation needs.

## Gold layer — the output Impulse writes

A star schema. Every table is prefixed with your configured `table_prefix` (e.g.
`my_report_histogram_fact`).

**Fact tables**

| Table                   | Grain                                    |
|-------------------------|------------------------------------------|
| `event_instance_fact`   | One row per event instance per container |
| `histogram_fact`        | One row per bin per container            |
| `histogram2d_fact`      | One row per (x, y) bin per container     |
| `stats_aggregator_fact` | One row per statistic label per signal per event instance |
| `calculated_channel_fact` | One row per sample interval per container (a derived signal, silver `channels` shape) |
| `calculated_channel_metrics` | One row per calculated channel per container (optional; silver `channel_metrics` shape). Written only when `config.calculated_channels.emit_channel_metrics` is set. See `impulse-channels`. |

**Dimension tables**

| Table                        | Holds                                                       |
|------------------------------|-------------------------------------------------------------|
| `measurement_dimension`      | Container metadata selected via `measurement_dimensions`.   |
| `event_dimension`            | Event definitions (name, TSAL expression, required channels). |
| `histogram_dimension`        | Histogram metadata (bins, signal info, units).              |
| `histogram2d_dimension`      | 2D histogram metadata.                                      |
| `stats_aggregator_dimension` | Statistics metadata (channel names, statistic labels incl. custom). |
| `calculated_channel_dimension` | Calculated-channel definitions (name, expression, identity). See `impulse-channels`. |

**Join pattern** — key columns connect facts to dimensions:

- `container_id` → links every fact to `measurement_dimension`.
- `event_id` → links `event_instance_fact`, `histogram_fact`, `histogram2d_fact` to `event_dimension`.
- `visual_id` → links each aggregation fact to its own dimension table.
- `channel_id` → links `calculated_channel_fact` to `calculated_channel_dimension`.

`stats_aggregator_fact` additionally joins to `event_instance_fact` via `event_instance_id` for
per-interval breakdowns.

The reporting mode writes all of these; see `impulse-reporting` for column-level fact/dimension
schemas and `impulse-config` for how `measurement_dimensions` picks the container columns.

## Adapting to an existing layout

Reshaping into the silver shape at ingest is recommended. If the data already lives in Delta with
different names and rewriting is impractical, there are two escape hatches. The rule of thumb:

- **`SolverConfig` for naming differences** — same tables and relationships, different column names.
- **Custom solver for structural differences** — no EAV tags, alias lookups, composite keys.
- **ETL into the standard shape** for everything else.

### Column-name remapping with SolverConfig

Declare a per-table mapping from your physical names to the engine's fixed internal names
(`container_id`, `channel_id`, `tstart`, `tend`, `value`, `key`, …). The mapping is applied once, when
the table is read; everything downstream uses the internal names. Set it under
`query_engine.solver_config` in your report config:

```python
"query_engine": {
    "solver": "DefaultSolver",
    "solver_config": {
        "container_metrics": {"column_name_mapping": {"my_measurement_id": "container_id"}},
        "channels": {"column_name_mapping": {"start_us": "tstart", "end_us": "tend"}}
    }
}
```

In RAW mode the `channels` internal names are `timestamp` and (optionally) `is_plausible` — remap
them the same way (e.g. `{"ts_raw": "timestamp"}`).

The underlying tables must still follow the silver-layer relationships (EAV tag tables,
per-`(container_id, channel_id)` channel rows). The full `SolverConfig` schema — per-table `filters`,
`project_id` scoping, `channel_mapping.join_keys`, and unit conversion — is in `impulse-config`.

### Custom solver

For layouts that don't match the relationships at all, subclass `QuerySolver` — or, more commonly,
`DefaultSolver` (both from `impulse_query_engine.analyze.query.solvers`) — override the pipeline
stage(s) that differ, and register it so a report config can select it **by name**.

**Register it.** Decorate the subclass with `@register_solver("Name", MyConfigCls)`. The report config
then picks it via the existing `query_engine.solver` field (the same field that takes the built-in
`"DefaultSolver"`. Selection is
**by registered name only** — a config can never cause Impulse to import an arbitrary class; which
solvers exist is governed entirely by what the driver imports.

```python
from impulse_query_engine.analyze.query.solvers import register_solver, SolverConfig
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver


class MyConfig(SolverConfig):          # optional: extra fields, validated at config-parse time
    raw_signal_table: str              # required — a missing/mistyped key raises up front

@register_solver("MySolver", MyConfig)
class MySolver(DefaultSolver):
    def solve(self, query, channels_df, selections, dtypes):
        df = super().solve(query, channels_df, selections, dtypes)   # reuse the pipeline
        ...                                                          # then adjust the result
        return df
```

```json
{"query_engine": {"solver": "MySolver", "solver_config": {"raw_signal_table": "cat.sch.raw"}}}
```

- **What to override.** Override the specific stage that differs — one of `filter_container_tags`,
  `filter_container_metrics`, `filter_channel_tags`, `filter_channel_metrics`, or `solve` — and
  delegate the rest with `super()`. Subclassing `DefaultSolver` (not bare `QuerySolver`) means you
  reimplement only what changes; a bare `QuerySolver` requires all stages.
- **Extra config fields.** Passing `MyConfig` to `@register_solver` makes Impulse validate the
  `solver_config` block through that subclass — extra/required fields are enforced at config-load
  time. Read them via typed attribute access (`self.config.raw_signal_table`).
- **Loading.** `@register_solver` runs only when its module is imported, so import the solver's
  package once in the driver **before** building the report — that import is what registers the name.
  An unregistered name fails config parse with an error listing the known names.

This is a large investment — you own the overridden stages. First check whether a one-time ETL into
the standard shape is cheaper.
