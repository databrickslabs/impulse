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
        - `column_name` (str): Column on `container_metrics_table` (e.g. `start_dt`, `stop_dt`, `uut_id`).
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
    - `project_id` (str, optional): Required when `solver = KEY_VALUE_STORE_SOLVER`. Used to isolate entities belonging to a specific project in concept-entities tables.
    - `parent_id` (str, optional): Optional parent-entity filter on the concept-entities table (KVS solver only), e.g. `"uut_concept"`. Default `null`.
    - `entity_maps_to` (str, optional): How `entity_id` in concept-entities maps to `container_metrics`. `"uut_id"` (default, 1-to-many vehicle→files) or `"container_id"` (1-to-1 file mapping).
    - `solver_config` (`SolverConfig`, optional): Column-name overrides for custom silver schemas. Fields: `container_id_col`, `channel_id_cols`, `channel_data_mapping`, `container_meta_data_mapping`, `entity_id_col`, `parent_id_col`.

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
