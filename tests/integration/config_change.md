# changing the config structure

# Description

The config structure for the `query_engine` key has been simplified.
`channel_alias_config`, `channel_metrics_config`, `col_name_mapping`, and
top-level `project_id`/`toolbox_id` have all been removed.

Channel-mapping and channel-metrics column names are now **hardcoded** to their
standard values. Project/toolbox filtering is configured through the flat
`filter_mapping` dict on `SolverConfig`.

## Current config structure

```python
"query_engine": {
    "solver":         "KeyValueStoreSolver",
    "entity_maps_to": "container_id",
    "solver_config": {
        "filter_mapping": {
            "project_id":  PROJECT_ID,
            "toolbox_id":  TOOLBOX_ID,
        },
    },
}
```

## What was done

1. Removed `channel_alias_config` (with `col_name_mapping` and `additional_filters`) from `SolverConfig`.
2. Removed `channel_metrics_config` (with `col_name_mapping`) from `SolverConfig`.
3. Removed top-level `project_id` and `parent_id` from `QueryEngine`.
4. All column names for `channel_mapping` and `channel_metrics` tables are hardcoded.
5. Filtering is handled by `filter_mapping` on `SolverConfig` — any key/value pair is applied as an equality filter on the `channel_mapping` table. If empty, no filtering is applied.



