# Configuration

`ImpulseConfig` configures everything about a report: the silver-layer input
tables, the gold-layer output location, container-level filters, the
query-engine solver, incremental processing, and which container columns
get surfaced into the gold-layer measurement dimension. Configuration is
defined as JSON (or an equivalent Python dictionary) and validated using
Pydantic models. The canonical schema lives in
[`src/impulse_reporting/config/config_parser.py`](https://github.com/databrickslabs/impulse/blob/main/src/impulse_reporting/config/config_parser.py).

## Quick example

```json
{
  "source": {
    "container_metrics_table": "my_catalog.silver.container_metrics",
    "channel_metrics_table": "my_catalog.silver.channel_metrics",
    "channels_uri": "my_catalog.silver.channels",
    "container_tags_table": "my_catalog.silver.container_tags",
    "channel_tags_table": "my_catalog.silver.channel_tags"
  },
  "unity_sink": {
    "catalog": "my_catalog",
    "schema": "gold",
    "table_prefix": "my_report"
  },
  "query_engine": {
    "solver": "DeltaSolver",
    "data_type": "RAW"
  },
  "container_filters": {
    "tag_filters": [
      [
        { "tag_name": "uut_id", "comparator": "==", "value": "ABC123", "cast_type": "string" }
      ]
    ],
    "metric_filters": [
      [
        { "column_name": "start_dt", "comparator": ">=", "value": "2025-04-27T05:20:54.000Z", "value_type": "timestamp" }
      ]
    ]
  },
  "measurement_dimensions": ["container_id", "vehicle_key", "start_ts", "stop_ts"]
}
```

A configuration is passed to `Report` either as a Python `dict`
(`config=...`) or as a JSON file path (`config_path=...`). Sinkless
mode is also supported — see [Sinkless reports](#sinkless-reports).

---

## source

Maps the silver-layer input tables.

| Field                     | Type  | Required | Description                                              |
|---------------------------|-------|----------|----------------------------------------------------------|
| `container_metrics_table` | `str` | Yes      | Full Unity Catalog path. Container metadata (timestamps, duration). |
| `channel_metrics_table`   | `str` | Yes      | Full Unity Catalog path. Channel-level statistics.        |
| `channels_uri`            | `str` | Yes      | Full Unity Catalog path. Time-series sample data.         |
| `container_tags_table`    | `str` | No       | Full Unity Catalog path. Container EAV tags.              |
| `channel_tags_table`      | `str` | No       | Full Unity Catalog path. Channel EAV tags.                |
| `channel_mapping_table`   | `str` | No       | Full Unity Catalog path. Logical-to-physical channel alias table. Required when using `QueryBuilder.channel_with_alias()` (currently supported by `KeyValueStoreSolver`). |
| `unit_conversion_table`   | `str` | No       | Full Unity Catalog path. Per-unit-family conversion factors. When configured together with a `channel_mapping_table` whose rows carry `source_unit` / `target_unit` columns, aliased selectors auto-convert values from source to target unit during `solve()` (currently supported by `KeyValueStoreSolver`). |

Tag tables are required for solvers that consume tag-based filters
(`DeltaSolver` with tag filters, `KeyValueStoreSolver`).

---

## unity_sink

Defines where gold-layer tables are written.

| Field          | Type  | Required | Description                           |
|----------------|-------|----------|---------------------------------------|
| `catalog`      | `str` | Yes      | Target catalog name.                  |
| `schema`       | `str` | Yes      | Target schema name.                   |
| `table_prefix` | `str` | Yes      | Prefix for all generated table names. |

Output tables are named `{table_prefix}_{entity}` (e.g.
`my_report_histogram_fact`).

### Sinkless reports

`unity_sink` is optional. When omitted, the report runs in **sinkless
mode**: `determine_report()` still computes events, aggregations, and
container dimensions and exposes them on the report object, but
`persist_results()` becomes a no-op. Useful for ad-hoc analysis,
notebooks, and tests where writing to Unity Catalog is not desired.

---

## container_filters (optional)

Restricts the set of processed containers. Filters are expressed in
**disjunctive normal form** (OR of ANDs): each inner list is AND-combined,
the outer list is OR-combined.

Two independent filter families:

- `tag_filters` — applied on `container_tags_table` (EAV key/value model).
- `metric_filters` — applied on `container_metrics_table` (columnar model).

| Field            | Type                       | Default | Description                                |
|------------------|----------------------------|---------|--------------------------------------------|
| `tag_filters`    | `list[list[TagFilter]]`    | `[]`    | Tag-based filter groups (DNF).             |
| `metric_filters` | `list[list[MetricFilter]]` | `[]`    | Metric-based filter groups (DNF).          |

### TagFilter

| Field        | Type  | Required | Description                                                                |
|--------------|-------|----------|----------------------------------------------------------------------------|
| `tag_name`   | `str` | Yes      | Tag key to filter on.                                                      |
| `comparator` | `str` | Yes      | One of `==`, `!=`, `>`, `>=`, `<`, `<=`.                                   |
| `value`      | any   | Yes      | Expected value. Must match `cast_type`.                                    |
| `cast_type`  | `str` | No       | `string` (default), `int`, `double`, or `timestamp` (ISO-format string).   |

### MetricFilter

| Field         | Type  | Required | Description                                                                            |
|---------------|-------|----------|----------------------------------------------------------------------------------------|
| `column_name` | `str` | Yes      | Column on `container_metrics_table` to filter on (e.g. `start_dt`, `stop_dt`). When `solver_config.container_metrics.column_name_mapping` is set, this refers to the **internal** name (after renaming). |
| `comparator`  | `str` | Yes      | One of `==`, `!=`, `>`, `>=`, `<`, `<=`.                                               |
| `value`       | any   | Yes      | Expected value. Must match `value_type` when provided.                                 |
| `value_type`  | `str` | No       | When provided, validates/converts the value: `string`, `int`, `double`, `timestamp`.   |

---

## query_engine (optional)

| Field                   | Type           | Default                  | Description                                                                                                                 |
|-------------------------|----------------|--------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `solver`                | `str`          | `"KeyValueStoreSolver"`  | One of `"DeltaSolver"`, `"KeyValueStoreSolver"`. `"KeyValueStoreSolver"` works either against a narrow EAV `container_tags` table or, when `source.container_tags_table` is omitted, against a wide-only data model where container attributes live directly on `container_metrics`. |
| `data_type`             | `str`          | `"RLE"`                  | `"RLE"` (intervals `[tstart, tend)`) or `"RAW"` (raw timestamps; converted to RLE before aggregation).                      |
| `drop_implausible_data` | `bool`         | `false`                  | When `true`, drops `channels` rows where `is_plausible = false`. Requires `data_type = "RAW"`; combining with `"RLE"` raises a validation error. |
| `batch_size`            | `int`          | `500`                    | Maximum number of selectors solved per batch.                                                                               |
| `solver_config`         | `SolverConfig` | `null`                   | Per-table column mappings, per-table equality filters, and project scoping. Set `project_id` to scope reads by project — it is applied to `container_tags` (if configured), `container_metrics`, and `channel_mapping` (if configured), so it works in both narrow EAV and wide-only data models. Omit it when you don't need project scoping. See [Solver column mappings and filters](#solver-column-mappings-and-filters). |

If `query_engine` is omitted, the default is `KeyValueStoreSolver` with
`data_type = "RLE"`.

---

## Solver column mappings and filters

The framework references columns by a fixed set of **internal names** (e.g. `container_id`, `channel_id`,
`tstart`, `tend`, `value`). When your silver-layer tables use different physical names, declare the mapping
in `solver_config` so the solver renames each table's columns at read time.

`SolverConfig` has one section per silver table. Each section is a `TableConfig` with two fields:

- `column_name_mapping` (`dict[str, str]`): `{ "physical_column": "internal_column" }`. The mapping is
  applied **once**, when the table is read. All downstream processing (filters, joins, aggregations) uses
  the internal names.
- `filters` (`dict[str, str]`): equality filters applied **after** renaming. Keys are internal column
  names; values are literals to match. Useful for project/toolbox scoping where a single value should
  always be enforced.

Top-level fields on `SolverConfig`:

- `project_id` (str, optional): Project scope. When set, the solver applies an equality filter on
  the `project_id` column (after column-name mapping) of every table it reads that carries one —
  `container_tags` (if configured), `container_metrics`, and `channel_mapping` (if configured).
  Omit it if you don't need project-level scoping; the solver does not require it.

Per-table sections (each a `TableConfig`):

| Section            | Used by                              | Typical mappings                                                  |
|--------------------|--------------------------------------|-------------------------------------------------------------------|
| `container_tags`   | DeltaSolver, KeyValueStoreSolver     | `entity_id → container_id`, custom EAV `key`/`value` columns      |
| `container_metrics`| All solvers                          | Custom container_id column, custom timestamp columns              |
| `channel_tags`     | DeltaSolver                          | Tag key/value column renames                                      |
| `channel_metrics`  | All solvers                          | Custom channel_id column, custom value/timestamp columns          |
| `channel_mapping`  | KeyValueStoreSolver                  | Alias-table column renames; `priority` column; optional `join_keys` for non-default alias-resolution composite keys |
| `channels`         | All solvers                          | RLE column renames (`tstart`/`tend`/`value`)                      |
| `unit_conversion`  | KeyValueStoreSolver                  | Unit-conversion table column renames (`unit`, `group_id`, `conversion_factor`) |

Internal column names that mappings can target:

| Internal name   | Description                                              |
|-----------------|----------------------------------------------------------|
| `container_id`  | Container identifier                                     |
| `channel_id`    | Channel identifier                                       |
| `tstart`, `tend`| Sample interval start/end (RLE)                          |
| `value`         | Sample value (or attribute value on the EAV tag table)   |
| `key`           | Attribute key on the EAV `container_tags` table          |
| `priority`      | Tie-breaker column on the `channel_mapping` table        |
| `project_id`    | Project scoping column                                   |
| `parent_id`     | Parent/scope identifier                                  |
| `source_channel`| Source-channel identifier on the `channel_mapping` table |
| `data_key`      | Data-key identifier (default present on both `channel_mapping` and `channel_metrics`) |
| `channel_alias` | Alias identifier on the `channel_mapping` table          |
| `channel_name`  | Channel-name identifier on the `channel_metrics` table   |
| `source_unit`, `target_unit` | Source/target unit columns on the `channel_mapping` table |
| `unit`          | Unit name column on the `unit_conversion` table          |
| `group_id`      | Unit-family identifier on the `unit_conversion` table    |
| `conversion_factor` | Per-unit factor on `unit_conversion`; also the per-channel factor name carried into the solve UDF |

:::note Per-solver feature support

`solver_config` in your JSON config is forwarded to **both**
`KeyValueStoreSolver` and `DeltaSolver` by the `Report` factory.
However, only the parts each solver supports are actually consumed:

- `KeyValueStoreSolver` uses all sections: per-table
  `column_name_mapping`, per-table `filters`, and top-level
  `project_id`.
- `DeltaSolver` uses only the per-table `column_name_mapping` entries
  on `container_tags`, `container_metrics`, `channel_tags`,
  `channel_metrics`, and `channels`. Per-table `filters`, top-level
  `project_id`, and the `channel_mapping` section (alias resolution)
  are **silently ignored** — the solver class does not read them.

:::

### Example: KeyValueStoreSolver with renamed columns and per-table filters

```python
"query_engine": {
    "solver": "KeyValueStoreSolver",
    "solver_config": {
        "project_id": "my_project",
        "container_tags": {
            "column_name_mapping": {"entity_id": "container_id"},
            "filters": {"parent_id": "my_parent_id"}
        },
        "container_metrics": {
            "column_name_mapping": {"start_dt": "tstart", "stop_dt": "tend"}
        },
        "channel_metrics": {
            "column_name_mapping": {}
        },
        "channel_mapping": {
            "column_name_mapping": {},
            "filters": {"toolbox_id": "my_toolbox"}
        },
        "channels": {
            "column_name_mapping": {}
        }
    }
}
```

Sections you don't customize can be omitted; defaults are an empty mapping and no filters.

### Unit conversion (optional)

Set `source.unit_conversion_table` and extend `channel_mapping` with `source_unit` / `target_unit` columns
to have aliased selectors auto-convert values from source to target unit during `solve()`. Direct selectors
via `query.channel(...)` always return raw values, even on a channel that an aliased sibling converts —
conversion is a property of the alias, not of the channel. See
[`unit_conversion`](../data_model/silver_layer_schema.md#unit_conversion-optional) for the table schema.

```python
"source": {
    "container_metrics_table": "my_catalog.silver.container_metrics",
    "channel_metrics_table": "my_catalog.silver.channel_metrics",
    "channels_uri": "my_catalog.silver.channels",
    "channel_mapping_table": "my_catalog.silver.channel_mapping",
    "unit_conversion_table": "my_catalog.silver.unit_conversion"
},
"query_engine": {
    "solver": "KeyValueStoreSolver",
    "solver_config": {
        "unit_conversion": {
            "column_name_mapping": {}
        }
    }
}
```

### Alias-resolution join keys (optional)

`KeyValueStoreSolver.filter_aliased_channel_metrics` joins `channel_mapping`
to `channel_metrics` to resolve aliased selectors. The default composite key
is `(source_channel, channel_name) + (data_key, data_key)`. Override
`channel_mapping.join_keys` to change the arity or column choice — for
example, a single-column join when `data_key` is not part of the channel
identity in your silver layout:

```python
"solver_config": {
    "channel_mapping": {
        "join_keys": [
            {"mapping_col": "source_channel", "metrics_col": "channel_name"}
        ]
    }
}
```

Each `mapping_col` / `metrics_col` is an **internal** name (the name as the
solver sees the column **after** `column_name_mapping` has been applied on
the respective table). The two sides of a pair are independent, so the same
column can carry different names on the two tables. For instance, a layout
where the data-key column has different physical names on the two tables
has two equivalent paths:

```python
# Path 1 — rename both physical columns to the same internal name; the
# default join_keys then works unchanged.
"solver_config": {
    "channel_mapping": {
        "column_name_mapping": {"mapping_data_key": "data_key"}
    },
    "channel_metrics": {
        "column_name_mapping": {"metrics_data_key": "data_key"}
    }
}

# Path 2 — leave the physical names as-is and reference them directly.
"solver_config": {
    "channel_mapping": {
        "join_keys": [
            {"mapping_col": "source_channel", "metrics_col": "channel_name"},
            {"mapping_col": "mapping_data_key", "metrics_col": "metrics_data_key"}
        ]
    }
}
```

`query.channel(...)` and `query.channel_with_alias(...)` kwargs are column
references against the **post-`column_name_mapping`** schema. If you
override `join_keys` (or skip renames) so that the solver sees a column
under a non-default name, the same name must be used as the kwarg. Example:
if `join_keys` references `metrics_col: "my_chan_name"` and the column is
not renamed via `column_name_mapping`, call
`query.channel(my_chan_name=...)`. The internal-name properties on
`SolverConfig` exist primarily to remove magic strings from the solver
code; the user-facing contract is "kwarg name == column name as the solver
sees it".

### When to use what

- **`solver_config.<table>.column_name_mapping`** — your silver-layer column is named differently from
  the framework's internal name (e.g. `entity_id` instead of `container_id`).
- **`container_filters.tag_filters` / `metric_filters`** — choose which containers participate in this
  particular run (supports comparators, OR/AND combinations, and type casting). Refer to internal column
  names when `solver_config` rewrites them.

---

## incremental (optional)

Incremental processing reuses results from prior runs for unchanged
definitions and reprocesses only containers that are new or have been
updated in silver. See the
[Report reference](../references/report.md#incremental-processing) for
mode-resolution rules and what counts as a definition change.

| Field                         | Type   | Default         | Description                                                            |
|-------------------------------|--------|-----------------|------------------------------------------------------------------------|
| `enabled`                     | `bool` | `false`         | Turns incremental processing on.                                       |
| `silver_last_modified_column` | `str`  | `"timestamp"`   | Silver-side column used to detect container updates.                   |
| `gold_last_modified_column`   | `str`  | `"_created_at"` | Gold-side column used to detect prior-run freshness.                   |

---

## measurement_dimensions (optional)

List of `container_metrics` columns to surface into the gold-layer
`measurement_dimension` table.

**Allowed values:**

| Value              | Description                                  |
|--------------------|----------------------------------------------|
| `container_id`     | Container identifier.                        |
| `uut_id`           | Unit-under-test identifier.                  |
| `project_id`       | Project identifier.                          |
| `vehicle_key`      | Vehicle identifier.                          |
| `file_name`        | Source measurement file name.                |
| `source_file_path` | Full path to the source file.                |
| `start_ts`         | Measurement start timestamp.                 |
| `stop_ts`          | Measurement stop timestamp.                  |
| `environment`      | Recording environment (e.g. PUMA, datalogger). |

**Default:**

```json
[
  "container_id",
  "uut_id",
  "file_name",
  "source_file_path",
  "start_ts",
  "stop_ts",
  "project_id",
  "environment"
]
```

Pick the entries that match the columns actually present in your
`container_metrics_table`. Columns referenced here must exist in your
silver schema; columns that don't appear here are ignored even if they
exist in silver.
