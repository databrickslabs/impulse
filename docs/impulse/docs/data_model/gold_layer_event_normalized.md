---
sidebar_position: 4
title: Gold Layer Schema
---

# Gold Layer Schema

The Gold layer follows a star schema optimizing storage cost and query
performance. Each aggregation type (histogram, histogram2d, statistics)
has its own fact/dimension pair linked by `visual_id`. Fact tables join
back to containers via `container_id` and to events via `event_id` or
`event_instance_id`.

All gold-layer table names are prefixed with the configured
`table_prefix` from the report's sink config (e.g. with
`table_prefix: "my_report"` the histogram fact table becomes
`my_report_histogram_fact`).

## Entity-relationship diagram

```mermaid
erDiagram

measurement_dimension {
    long container_id PK
    long config_hash
    timestamp _created_at
}

event_dimension {
    int event_id PK
    int report_id FK
    string event_type
    string event_name
    string event_description
    array[str] required_channels
    string event_expression
    long definition_hash
    map[str_str] attributes
    timestamp _created_at
}

event_instance_fact {
    int container_id FK
    long event_instance_id PK
    int event_id FK
    long start_ts
    long end_ts
    timestamp _created_at
}

stats_aggregator_fact {
    long event_instance_id FK
    int container_id FK
    int visual_id FK
    string channel_name
    int event_id FK
    string aggregation_label
    double statistic_value
    timestamp _created_at
}

stats_aggregator_dimension {
    int visual_id PK
    int report_id
    int page_number
    string name
    string description
    string agg_type
    array[str] statistics
    array[str] channel_names
    array[str] signal_expressions
    string values_unit
    long definition_hash
    timestamp _created_at
}

histogram_dimension {
    int visual_id PK
    int report_id
    int page_number
    string name
    string description
    string agg_type
    double[] bins
    string channel_name
    string signal_expression
    string weights_channel_name
    string weights_expression
    string values_unit
    string bins_unit
    long definition_hash
    timestamp _created_at
}

histogram_fact {
    int container_id FK
    int visual_id FK
    int event_id FK
    int bin_id
    double hist_value
    double lower_bound
    double upper_bound
    string bin_name
    timestamp _created_at
}

histogram2d_dimension {
    int visual_id PK
    int report_id
    int page_number
    string name
    string description
    string agg_type
    double[] x_bins
    double[] y_bins
    string x_channel_name
    string x_signal_expression
    string y_channel_name
    string y_signal_expression
    string weights_channel_name
    string weights_expression
    string values_unit
    string x_bins_unit
    string y_bins_unit
    long definition_hash
    timestamp _created_at
}

histogram2d_fact {
    int container_id FK
    int visual_id FK
    int event_id FK
    int x_bin_id
    int y_bin_id
    double hist_value
    double x_lower_bound
    double x_upper_bound
    double y_lower_bound
    double y_upper_bound
    string x_bin_name
    string y_bin_name
    timestamp _created_at
}

channel_mapping_resolution_dimension {
    long container_id FK
    long channel_id
    string channel_name
    string data_key
    string channel_alias
    string priority "nullable"
    string source_unit "optional"
    string target_unit "optional"
    timestamp _created_at
}

histogram_fact }o--|| event_dimension: event_id
histogram2d_fact }o--|| event_dimension: event_id
stats_aggregator_fact }o--|| event_instance_fact: event_instance_id

histogram_dimension ||--o{ histogram_fact : by_visual_id
histogram2d_dimension ||--o{ histogram2d_fact : by_visual_id
stats_aggregator_dimension ||--o{ stats_aggregator_fact: by_visual_id

measurement_dimension ||--o{ histogram_fact : container_id
measurement_dimension ||--o{ histogram2d_fact : container_id
measurement_dimension ||--o{ stats_aggregator_fact : container_id

measurement_dimension ||--o{ channel_mapping_resolution_dimension : container_id

event_instance_fact }o--|| event_dimension: event_id
```

The `measurement_dimension` table also contains additional columns
selected dynamically from
[`config.measurement_dimensions`](../config/configuration.md#measurement_dimensions-optional)
at run time — only `container_id`, `config_hash`, and `_created_at` are
guaranteed.

---

## Fact tables

| Table                            | Key columns                                                                            | Description                                                  |
|----------------------------------|----------------------------------------------------------------------------------------|--------------------------------------------------------------|
| `{prefix}_histogram_fact`        | `container_id`, `visual_id`, `event_id`, `bin_id`                                      | 1D histogram bin values per container.                       |
| `{prefix}_histogram2d_fact`      | `container_id`, `visual_id`, `event_id`, `x_bin_id`, `y_bin_id`                        | 2D histogram bin values per container.                       |
| `{prefix}_stats_aggregator_fact` | `container_id`, `visual_id`, `event_instance_id`, `channel_name`, `aggregation_label`  | Statistics values per signal, event instance, and container. |
| `{prefix}_event_instance_fact`   | `container_id`, `event_id`, `event_instance_id`                                        | Materialized event occurrences with start/end timestamps.    |

---

## Dimension tables

| Table                                 | Key columns              | Description                                                |
|---------------------------------------|--------------------------|------------------------------------------------------------|
| `{prefix}_histogram_dimension`        | `visual_id`, `report_id` | Histogram metadata (name, bins, signal info, units).       |
| `{prefix}_histogram2d_dimension`      | `visual_id`, `report_id` | 2D histogram metadata (axes, bins, signal info, units).    |
| `{prefix}_stats_aggregator_dimension` | `visual_id`, `report_id` | Statistics metadata (signals, aggregation labels).         |
| `{prefix}_event_dimension`            | `event_id`, `report_id`  | Event definitions (name, expression, required channels).   |
| `{prefix}_measurement_dimension`      | `container_id`           | Container metadata. Always carries `container_id`, `config_hash`, `_created_at`; additional columns are populated from [`config.measurement_dimensions`](../config/configuration.md#measurement_dimensions-optional). |
| `{prefix}_channel_mapping_resolution_dimension` | `container_id`, `channel_id`, `channel_alias` | Resolves each channel alias to its physical channel per container (physical join keys, alias `priority`). Written only when the report uses aliased selectors. The `source_unit` / `target_unit` columns are present only when a [`config.unit_conversion_table`](../config/configuration.md) is configured. |

The `channel_mapping_resolution_dimension` table lets BI consumers join a fact
back to the physical channel that an alias resolved to: join on
`(container_id, channel_id, channel_alias)`. The join-key, `channel_alias`,
`priority`, and `source_unit` / `target_unit` column names follow the
[`channel_mapping` solver config](../config/configuration.md) — see the column
reference there for how each maps to the alias and metrics tables.
