# Gold Layer - ER Diagram

The Gold layer follows a star schema optimizing storage cost and query performance. 
Each aggregation type (histogram, histogram2d, statistics) has its own fact/dimension pair linked by `visual_id`. 

Fact tables join back to containers via `container_id` and to events via `event_id` or `event_instance_id`.

```mermaid
erDiagram

measurement_dimension {
    int container_id PK
    int uut_id FK
    int project_id FK
    string uut_name
    string project_name
    string file_name
    string source_file_path
    long first_datapoint_ts
    long last_datapoint_ts
    double odo_start
    double odo_stop
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

histogram_fact }o--|| event_dimension: event_id
histogram2d_fact }o--|| event_dimension: event_id
stats_aggregator_fact }o--|| event_instance_fact: event_instance_id

histogram_dimension ||--o{ histogram_fact : by_visual_id
histogram2d_dimension ||--o{ histogram2d_fact : by_visual_id
stats_aggregator_dimension ||--o{ stats_aggregator_fact: by_visual_id

measurement_dimension ||--o{ histogram_fact : container_id
measurement_dimension ||--o{ histogram2d_fact : container_id
measurement_dimension ||--o{ stats_aggregator_fact : container_id

event_instance_fact }o--|| event_dimension: event_id
```
