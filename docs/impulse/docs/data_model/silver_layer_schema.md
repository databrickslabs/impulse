# Silver Layer - ER Diagram

The Silver layer stores measurement data in a tag-based normalized model. Core time-series data lives in `channels`,
while metadata is split across tag tables (key-value pairs) and metric tables (pre-computed statistics). 

```mermaid
erDiagram

uut_dim {
    int uut_id PK
    string uut_name
    string uut_desc
    data_type TBD
}

project_dim {
    int project_id PK
    string project_name
    string project_desc
    data_type TBD
}

container_metrics {
    int container_id PK
    int uut_id FK
    int project_id FK
    string uut_name
    string project_name
    string file_name
    timestamp start_ts
    timestamp stop_ts
    double odo_start
    double odo_stop
    string environment
    string experiment
    long original_file_size
    string original_file_source
}

container_tags {
    int container_id
    string key
    string value
}

channel_metrics {
    int project_id FK
    int container_id FK
    int channel_id FK
    string channel_name
    string data_key
    string source_unit
    int sample_count
    double first_ts
    double last_ts
    long duration
    double min
    double max
    double mean
}

channel_tags {
    int container_id
    int channel_id
    string key
    string value
}

channels {
    int container_id FK
    int channel_id FK
    long start
    long end "optional"
    double value
    boolean is_plausible "optional"
}

channel_mapping {
    int project_id FK
    int concept_id FK
    int element_id FK
    string project_name
    string element_name
    string channel_name
    string data_key
    int priority

}

channel_conversion_mapping {
    int project_id
    int element_id
    string source_unit
    string target_unit
    string conversion_formula
    double conversion_factor
}

container_metrics ||--o{ channels : container_id
container_tags }o--o{ channels: container_id

channel_metrics ||--o{ channels : "container_id,channel_id"
channel_tags }o--o{ channels: "container_id, channel_id"
channel_mapping ||--o{ channel_metrics: "project_id, data_key, channel_name"
channel_mapping ||--o{ channel_conversion_mapping: "project_id, element_id"

uut_dim ||--o{ container_metrics : uut_id
project_dim ||--o{ container_metrics : project_id
```

> **`channels` format variants:**
> - **RLE format** -- both `start` and `end` are present (run-length encoded)
> - **Raw format** -- only `start` is present (`end` is computed on-the-fly via the query engine)