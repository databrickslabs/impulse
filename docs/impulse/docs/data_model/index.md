---
title: Data Model
---

# Data Model

:::tip Start with three tables

[`DefaultSolver`](../references/query_engine.md) needs **only three tables**:
`container_metrics`, `channel_metrics`, and `channels`. The tag tables
(`container_tags`, `channel_tags`) and the `channel_mapping` / `unit_conversion`
tables are **fully optional** add-ons, used only when configured — see the
[Query Engine table requirements](../references/query_engine.md#table-requirements).

The rest of this page documents the full shape. Landing your data in it during
ingest is the simplest path (see the [Ingestion guide](ingestion.md)); if you
can't reshape, a [`SolverConfig`](ingestion.md#adapting-to-existing-data-layouts)
can remap column names, or you can implement a custom solver.

:::

Impulse operates on Databricks Medallion Architecture.

Raw measurement files are ingested into the lakehouse in the bronze layer. These are then processed and transformed into a normalized Silver layer.
Gold Layer contains the final analytics results in a star schema optimized for querying and reporting.

All layers are stored as Delta tables in Unity Catalog, which makes them easy to govern, secure, and queryable by various personas across the organization.


---

## Silver Layer (Input)

**Only three tables are required** — `container_metrics`, `channel_metrics`, and `channels`. The two tag tables (`container_tags`, `channel_tags`) are **fully optional**: add them only when you want tag-based container filtering or EAV channel selection.

| Table               | Required? | Purpose                                                                                |
|---------------------|-----------|----------------------------------------------------------------------------------------|
| `container_metrics` | **Yes**   | One row per measurement container with timestamps, duration, and channel count.        |
| `channel_metrics`   | **Yes**   | Pre-computed statistics per channel (min, max, mean, percentiles, sample count). Also carries channel-selection columns (e.g. `channel_name`) in the wide model. |
| `channels`          | **Yes**   | Time-series sample data, either as raw `(timestamp, value)` samples or as run-length-encoded intervals `[tstart, tend)`. |
| `container_tags`    | Optional  | Key-value metadata tags for containers (e.g. `vehicle_key`, `project_id`).             |
| `channel_tags`      | Optional  | Key-value metadata tags per channel (e.g. `channel_name`, `brand`, `model`).           |

Channels are selected either from an EAV `channel_tags` table (e.g. `channel_name = "Engine RPM"`) or directly from columns on `channel_metrics` — in both cases by signal metadata rather than fixed column positions, so the same schema supports arbitrary signal sets across projects.

See the [Silver Layer ER Diagram](silver_layer_schema.md) for table relationships.
For background on the design, see the [Databricks blog post on revolutionizing car measurement data storage and analysis](https://www.databricks.com/blog/revolutionizing-car-measurement-data-storage-and-analysis-mercedes-benzs-petabyte-scale).

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
