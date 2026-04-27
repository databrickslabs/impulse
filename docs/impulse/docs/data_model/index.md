---
title: Data Model
---

# Data Model

Impulse operates on Databricks Medallion Architecture.

Bronze data conissts out of the raw measurement files ingested into the lakehouse, which are then processed and transformed into a normalized Silver layer.
Gold Layer contains the final analytics results in a star schema optimized for querying and reporting.

All layers are stored as Delta tables in Unity Catalog, which makes them easy to govern, secure, and queryable by various personas across the organization.


---

## Silver Layer (Input)

The Silver layer uses a **tag-based model** where metadata is separated from time-series data. Five tables
represent measurement data:

| Table               | Purpose                                                                                |
|---------------------|----------------------------------------------------------------------------------------|
| `container_metrics` | One row per measurement container with timestamps, duration, and channel count.        |
| `container_tags`    | Key-value metadata tags for containers (e.g. `vehicle_key`, `project_id`).             |
| `channel_metrics`   | Pre-computed statistics per channel (min, max, mean, percentiles, sample count).        |
| `channel_tags`      | Key-value metadata tags per channel (e.g. `channel_name`, `brand`, `model`).           |
| `channels`          | Time-series sample data stored as intervals `[tstart, tend)` with a constant value.    |

Channels are selected by querying `channel_tags` (e.g. `channel_name = "Engine RPM"`) rather than by fixed column names.
This allows the same schema to support arbitrary signal sets across different projects.

See the [Silver Layer ER Diagram](silver_layer_schema.md) for table relationships.

---

## Gold Layer (Output)

The Gold layer uses a **star schema** with fact and dimension tables. All table names are prefixed with a configurable
`table_prefix` (e.g. `my_report_histogram_fact`).

### Fact tables

| Table                  | Grain                                    | Description                                              |
|------------------------|------------------------------------------|----------------------------------------------------------|
| `event_instance_fact`  | One row per event instance per container | Materialized time windows where an event condition holds. |
| `histogram_fact`       | One row per bin per container            | 1D histogram bin values, duration-weighted.               |
| `histogram2d_fact`     | One row per (x, y) bin per container     | 2D histogram bin values, duration-weighted.               |
| `stats_aggregator_fact` | One row per signal per event instance    | Descriptive statistics (min, max, mean, median).          |

### Dimension tables

| Table                    | Description                                                        |
|--------------------------|--------------------------------------------------------------------|
| `measurement_dimension`  | Container metadata selected from `container_metrics` via config.   |
| `event_dimension`        | Event definitions (name, TSAL expression, required channels).      |
| `histogram_dimension`    | Histogram metadata (bins, signal info, units).                     |
| `histogram2d_dimension`  | 2D histogram metadata (axes, bins, signal info, units).            |
| `stats_aggregator_dimension` | Statistics metadata (channel names, aggregation labels).       |

### Join pattern

Fact and dimension tables are connected through three key columns:

- **`container_id`** -- links all fact tables to `measurement_dimension`
- **`event_id`** -- links `event_instance_fact`, `histogram_fact`, and `histogram2d_fact` to `event_dimension`
- **`visual_id`** -- links each aggregation fact table to its corresponding dimension table

`stats_aggregator_fact` additionally joins to `event_instance_fact` via `event_instance_id`, enabling per-interval breakdowns.


---

## Key Concepts

| Concept         | Definition                                                                                          | Tables                                           |
|-----------------|-----------------------------------------------------------------------------------------------------|--------------------------------------------------|
| **Container**   | A single measurement recording (e.g. one test drive). Identified by `container_id`.                 | `container_metrics`, `container_tags`             |
| **Channel**     | A time-series signal within a container (e.g. "Engine RPM"). Identified by `(container_id, channel_id)`. | `channels`, `channel_metrics`, `channel_tags` |
| **Event**       | A time window of interest, defined by a condition or spanning the full recording. | `event_dimension`, `event_instance_fact`          |
| **Aggregation** | A computation over channel data within event windows (histogram, 2D histogram, or statistics).      | `*_fact`, `*_dimension`                          |
