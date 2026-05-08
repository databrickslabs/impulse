# Configuration

`MdaConfig` allows you to configure the generated report by Impulse.
It allows you to specify data sources, output sinks, container filters, and the query engine solver.

## Configuration Fields

- **source** (`Source`):
  Configuration for input data tables.
    - `container_metrics_table` (str, required): Full Unity Catalog path (e.g., `catalog.schema.table`).
    - `channel_metrics_table` (str, required): Full Unity Catalog path.
    - `channels_uri` (str, required): Full Unity Catalog path for the channels table.
    - `container_tags_table` (str, optional): Full Unity Catalog path to the container tags table. Required when using `DeltaSolver` with tag filters or `KeyValueStoreSolver`.
    - `channel_tags_table` (str, optional): Full Unity Catalog path to the channel tags table. Used by `DeltaSolver` for tag-based channel selection.
    - `channel_mapping_table` (str, optional): Full Unity Catalog path to the channel-alias mapping table. Required when using `QueryBuilder.channel_with_alias()` (currently supported by `KeyValueStoreSolver`).

- **unity_sink** (`UnitySink`):
  Configuration for output data location.
    - `catalog` (str): Target catalog name.
    - `schema` (str): Target schema name.
    - `table_prefix` (str): Prefix for output tables.


- **container_filters** (`ContainerFilters`, optional):
  Restricts the set of processed containers. Filters are expressed in **disjunctive normal form** (OR of ANDs): each inner list is AND-combined; the outer list is OR-combined.

  Two independent filter families:

    - `tag_filters` (`list[list[TagFilter]]`): applied on `container_tags_table`.
        - `tag_name` (str): Tag key to filter on.
        - `comparator` (str): One of `==`, `!=`, `>`, `>=`, `<`, `<=`.
        - `value` (str | int | float): Expected value; must match `cast_type`.
        - `cast_type` (str, optional): `string` (default), `int`, `double`, or `timestamp` (ISO-format string).

    - `metric_filters` (`list[list[MetricFilter]]`): applied on `container_metrics_table`.
        - `column_name` (str): Column on `container_metrics_table` (e.g. `start_dt`, `stop_dt`, `uut_id`). When `solver_config.container_metrics.column_name_mapping` is set, this refers to the **internal** name (after renaming).
        - `comparator` (str): One of `==`, `!=`, `>`, `>=`, `<`, `<=`.
        - `value` (str | int | float): Expected value.
        - `value_type` (str, optional): `string`, `int`, `double`, or `timestamp` — when provided, validates/converts the value.



- **query_engine** (`QueryEngine`, optional):
  Query engine configuration.
    - `solver` (`Solvers`): Solver type. Defaults to `BASIC_NARROW_SOLVER`. Accepted values: `BASIC_NARROW_SOLVER`, `DELTA_SOLVER`, `KEY_VALUE_STORE_SOLVER`.
    - `data_type` (`DataType`, optional): The format of the channel data. Defaults to `RLE`. Accepted values:
        - `RLE`: Channel data is pre-encoded with Run-Length Encoding (intervals with `tstart`/`tend`).
        - `RAW`: Channel data contains raw timestamps (a `timestamp` column). The framework automatically converts it to RLE interval format before joins and aggregations.
    - `drop_implausible_data` (bool, optional): Whether to drop implausible data points before processing. Defaults to `false`. If `true`, data points marked as implausible (via the `is_plausible` column) will be dropped. **Only takes effect when `data_type = RAW`**: the filter is applied during the RAW -> RLE conversion path. Combining `drop_implausible_data=true` with `data_type=RLE` raises a validation error.
    - `batch_size` (int, optional): Maximum number of selectors solved per batch. Defaults to `500`.
    - `solver_config` (`SolverConfig`, optional): Per-table column mappings, per-table equality filters, and project scoping. Required (`project_id` field) when `solver = KEY_VALUE_STORE_SOLVER`. See [Solver column mappings and filters](#solver-column-mappings-and-filters) below.

- **incremental** (`IncrementalConfig`, optional):
  Incremental processing configuration. When `enabled` is `false` (default), every run reprocesses all matching containers in full. When `enabled` is `true`, `Report.determine_report()` reuses results from prior runs for unchanged definitions and only reprocesses containers that are new or have been updated in silver.
    - `enabled` (bool): Whether incremental processing is on. Defaults to `false`.
    - `silver_last_modified_column` (str): Silver-side column used to detect container updates. Defaults to `"timestamp"`.
    - `gold_last_modified_column` (str): Gold-side column used to detect prior-run freshness. Defaults to `"_created_at"`.


- **measurement_dimensions** (`list[str]`, optional):
  List of measurement dimension column names to include in the gold layer.
  Defaults to:
    - `container_id`
    - `vehicle_key`
    - `start_ts`
    - `stop_ts`

  Each entry should be a string matching a supported measurement dimension.
  Supported values:
    - `container_id`
    - `uut_id`
    - `project_id`
    - `vehicle_key`
    - `file_name`
    - `source_file_path`
    - `start_ts`
    - `stop_ts`
    - `environment`

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

- `project_id` (str, optional): Required when `solver = KEY_VALUE_STORE_SOLVER`. Applied as a filter
  on the `project_id` column of `container_tags` and `channel_mapping`.

Per-table sections (each a `TableConfig`):

| Section            | Used by                              | Typical mappings                                                  |
|--------------------|--------------------------------------|-------------------------------------------------------------------|
| `container_tags`   | DeltaSolver, KeyValueStoreSolver     | `entity_id → container_id`, custom EAV `key`/`value` columns      |
| `container_metrics`| All solvers                          | Custom container_id column, custom timestamp columns              |
| `channel_tags`     | DeltaSolver                          | Tag key/value column renames                                      |
| `channel_metrics`  | All solvers                          | Custom channel_id column, custom value/timestamp columns          |
| `channel_mapping`  | KeyValueStoreSolver                  | Alias-table column renames; `priority` column                     |
| `channels`         | All solvers                          | RLE column renames (`tstart`/`tend`/`value`)                      |

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

### Example: KeyValueStoreSolver with renamed columns and per-table filters

```python
"query_engine": {
    "solver": "KEY_VALUE_STORE_SOLVER",
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

### When to use what

- **`solver_config.<table>.column_name_mapping`** — your silver-layer column is named differently from
  the framework's internal name (e.g. `entity_id` instead of `container_id`).
- **`container_filters.tag_filters` / `metric_filters`** — choose which containers participate in this
  particular run (supports comparators, OR/AND combinations, and type casting). Refer to internal column
  names when `solver_config` rewrites them.

## Example

```python
config_data = {
    "source": {
        "container_metrics_table": "my_catalog.silver.container_metric",
        "channel_metrics_table": "my_catalog.silver.channel_metric",
        "channels_uri": "my_catalog.silver.channel_data"
    },
    "unity_sink": {
        "catalog": "my_catalog",
        "schema": "silver_refactored",
        "table_prefix": "evaluation"
    },
    # (uut_id == "AA" AND container_id >= 100) OR uut_id == "BB"
    # AND metric start_ts >= 2025-01-01
    "container_filters": {
        "tag_filters": [
            [
                {"tag_name": "uut_id", "comparator": "==", "value": "AA", "cast_type": "string"},
                {"tag_name": "container_id", "comparator": ">=", "value": 100, "cast_type": "int"}
            ],
            [
                {"tag_name": "uut_id", "comparator": "==", "value": "BB", "cast_type": "string"}
            ]
        ],
        "metric_filters": [
            [
                {"column_name": "start_ts", "comparator": ">=", "value": "2025-01-01"}
            ]
        ]
    },
    "query_engine": {
        "solver": "BASIC_NARROW_SOLVER",
        "data_type": "RAW"
    },
    "measurement_dimensions": [
        "container_id",
        "vehicle_key",
        "start_ts",
        "stop_ts"
    ]
}
```

If `query_engine` is omitted, it defaults to `BASIC_NARROW_SOLVER` with `data_type` set to `RLE`.
We support providing the configuration as a dictionary or as a JSON file path.
