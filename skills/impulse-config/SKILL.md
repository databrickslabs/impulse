---
name: impulse-config
description: >
  Write the Impulse report configuration (`ImpulseConfig`) — the JSON/dict that points Impulse at its
  silver input tables, the gold output location, and tunes the solver. Use when the user asks to
  "configure an Impulse report", set the source/sink tables, filter which containers are processed,
  choose RLE vs RAW, turn on incremental processing, run without writing (sinkless), remap column
  names, or scope by project. Covers source, unity_sink, container_filters, query_engine, solver_config,
  incremental, and measurement_dimensions, all validated by Pydantic.
---

# Impulse — configuration

`ImpulseConfig` configures a report: silver input, gold output, container filters, the solver,
incremental processing, and which container columns land in gold. It is a JSON file or an equivalent
Python `dict`, validated by Pydantic. Pass it to `Report` as `config=<dict>` or `config_path=<json path>`
(see `impulse-reporting`).

## Full example

```python
config = {
    "source": {
        "container_metrics_table": "my_catalog.silver.container_metrics",
        "channel_metrics_table": "my_catalog.silver.channel_metrics",
        "channels_uri": "my_catalog.silver.channels",
        "container_tags_table": "my_catalog.silver.container_tags",   # optional
        "channel_tags_table": "my_catalog.silver.channel_tags",       # optional
    },
    "unity_sink": {"catalog": "my_catalog", "schema": "gold", "table_prefix": "my_report"},
    "query_engine": {"solver": "DefaultSolver", "data_type": "RLE"},
    "container_filters": {
        "tag_filters": [
            [{"tag_name": "uut_id", "comparator": "==", "value": "ABC123", "cast_type": "string"}]
        ],
        "metric_filters": [
            [{"column_name": "start_dt", "comparator": ">=",
              "value": "2025-04-27T05:20:54.000Z", "value_type": "timestamp"}]
        ],
    },
    "incremental": {"enabled": True},
    "measurement_dimensions": ["container_id", "vehicle_key", "start_ts", "stop_ts"],
}
```

## source (required)

Maps the silver-layer input tables. Values are full Unity Catalog paths (`catalog.schema.table`). See
`impulse-data-model` for the shape of each table.

| Field                     | Required | Description                                                         |
|---------------------------|----------|---------------------------------------------------------------------|
| `container_metrics_table` | Yes      | Container metadata (timestamps, duration).                          |
| `channel_metrics_table`   | Yes      | Per-channel statistics; channel-selection columns in the wide model. |
| `channels_uri`            | Yes      | Time-series sample data.                                            |
| `container_tags_table`    | No       | Container EAV tags. Required to use `tag_filters`.                   |
| `channel_tags_table`      | No       | Channel EAV tags. Required to select channels by tag.               |
| `channel_mapping_table`   | No       | Logical→physical alias table. Required for `channel_with_alias()`.  |
| `unit_conversion_table`   | No       | Per-unit conversion factors (used with `channel_mapping_table`).    |

## unity_sink (optional — omit for sinkless mode)

Where gold tables are written. Output tables are named `{table_prefix}_{entity}`.

| Field          | Required | Description        |
|----------------|----------|--------------------|
| `catalog`      | Yes      | Target catalog.    |
| `schema`       | Yes      | Target schema.     |
| `table_prefix` | Yes      | Prefix for tables. |

**Sinkless mode:** omit `unity_sink` entirely. `determine_report()` still computes everything and
exposes it on the report object, but `persist_results()` becomes a no-op. Use it for ad-hoc analysis,
ML feature extraction, notebooks, and tests. (See `impulse-analyze` and `impulse-ml`.)

## container_filters (optional)

Restricts which containers are processed. Both families are **disjunctive normal form** — the outer
list is OR-combined, each inner list is AND-combined.

- `tag_filters` — applied on `container_tags_table` (EAV). Requires that table.
- `metric_filters` — applied on `container_metrics_table` (columns).

**TagFilter** fields: `tag_name` (str), `comparator` (one of `== != > >= < <=`), `value`, and optional
`cast_type` (`string` default, `int`, `double`, `timestamp`).

**MetricFilter** fields: `column_name` (**internal** name, after any `column_name_mapping`),
`comparator`, `value`, and optional `value_type` (`string`, `int`, `double`, `timestamp`).

## query_engine (optional)

Omit entirely for `DefaultSolver` + `data_type="RLE"`.

| Field                   | Default            | Description                                                                                       |
|-------------------------|--------------------|---------------------------------------------------------------------------------------------------|
| `solver`                | `"DefaultSolver"`  | The solver. `"DeltaSolver"` / `"KeyValueStoreSolver"` are **deprecated aliases** for `DefaultSolver`. |
| `data_type`             | `"RLE"`            | `"RLE"` for interval-encoded `[tstart, tend)` samples; `"RAW"` for `(timestamp, value)` samples.  |
| `raw_encoder`           | `null` (→ `"RLE"`) | How RAW point data becomes intervals. `"RLE"` collapses equal-valued runs (default); `"INTERVAL"` keeps every sample, only deriving `tend` + dropping exact duplicates. Only used when `data_type="RAW"`. |
| `drop_implausible_data` | `false`            | Drop `channels` rows where `is_plausible = false`. **Requires `data_type="RAW"`** (RLE raises).   |
| `batch_size`            | `500`              | Max selectors solved per batch.                                                                   |
| `solver_config`         | `null`             | Per-table column mappings, per-table filters, and project scoping (below).                        |

## solver_config (optional) — adapt to an existing layout

The engine references columns by fixed **internal names** (`container_id`, `channel_id`, `tstart`,
`tend`, `value`, `key`, …). When your physical column names differ, declare the mapping so the solver
renames each table at read time. Each silver table has a section with:

- `column_name_mapping` (`{physical: internal}`) — applied once, when the table is read.
- `filters` (`{internal: literal}`) — equality filters applied **after** renaming (e.g. project scoping).

With `data_type="RAW"`, the `channels` table additionally uses the internal names `timestamp` (the
per-sample timestamp) and — only when `drop_implausible_data` is on — `is_plausible`. Remap them the
same way, e.g. `"channels": {"column_name_mapping": {"ts_raw": "timestamp"}}`.

Top-level `project_id` (str, optional) applies an equality filter on the `project_id` column of every
table that has one (`container_tags`, `container_metrics`, `channel_mapping`). Omit if not needed.

```python
"query_engine": {
    "solver": "DefaultSolver",
    "solver_config": {
        "project_id": "my_project",
        "container_tags": {
            "column_name_mapping": {"entity_id": "container_id"},
            "filters": {"parent_id": "my_parent_id"}
        },
        "container_metrics": {"column_name_mapping": {"start_dt": "tstart", "stop_dt": "tend"}},
        "channel_mapping": {"filters": {"toolbox_id": "my_toolbox"}}
    }
}
```

Sections you don't customize can be omitted (defaults: empty mapping, no filters). Sections apply per
table: `container_tags`, `container_metrics`, `channel_tags`, `channel_metrics`, `channel_mapping`,
`channels`, `unit_conversion`.

**Alias-resolution join keys.** When resolving `channel_with_alias()`, the solver joins
`channel_mapping` to `channel_metrics` on `(source_channel, channel_name)` + `(data_key, data_key)` by
default. Override arity/columns with `channel_mapping.join_keys` — each pair references **internal**
names (as the solver sees them after `column_name_mapping`):

```python
"channel_mapping": {
    "join_keys": [{"mapping_col": "source_channel", "metrics_col": "channel_name"}]
}
```

The contract is "kwarg name == column name as the solver sees it": if a join key or missing rename
leaves a column under a non-default name, pass that same name as the `query.channel(...)` kwarg.

**Unit conversion.** Set `source.unit_conversion_table` and add `source_unit` / `target_unit` columns
to `channel_mapping` so aliased selectors auto-convert during `solve()`. Constants in expressions over
an aliased selector must then be in the target unit. Direct `channel(...)` selectors are never
converted — conversion is a property of the alias.

## incremental (optional)

Reuses prior results for unchanged definitions and reprocesses only new/updated containers. See
`impulse-reporting` for mode resolution and what counts as a definition change.

| Field                         | Default         | Description                                     |
|-------------------------------|-----------------|-------------------------------------------------|
| `enabled`                     | `false`         | Turn incremental processing on.                 |
| `silver_last_modified_column` | `"timestamp"`   | Silver column used to detect container updates. |
| `gold_last_modified_column`   | `"_created_at"` | Gold column used to detect prior-run freshness. |

## measurement_dimensions (optional)

List of `container_metrics` columns (post-mapping **internal** names) to surface into the gold
`measurement_dimension` table. Each name passes through verbatim as the gold column name.

Default: `["container_id", "start_ts", "stop_ts"]`. Keep `container_id` — it is the incremental upsert
key and the join key to fact tables. Any column present in your post-mapping `container_metrics`
DataFrame is valid; a missing one fails the run fast with a `ValueError` naming it.
