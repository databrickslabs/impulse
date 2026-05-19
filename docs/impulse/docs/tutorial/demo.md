---
sidebar_position: 1
sidebar_label: Reporting
title: Reporting walkthrough
---

# Reporting walkthrough

This guide walks through the `demos/reporting_pipeline.ipynb` notebook step by step. It covers the full workflow:
configuring a report, defining signals and events, adding aggregations, computing results, and inspecting the output
tables.

If you have not run Impulse before, start with [Getting Started](../getting_started.md) — it covers install, demo-data
upload, and a minimal one-event, one-histogram report. This page picks up from there with a richer example.

## Prerequisites

- Completed [Getting Started](../getting_started.md) — Impulse installed, demo CSVs loaded as silver-layer Delta tables.
- A configuration file at `./config/config.json` relative to the notebook (included in the `demos/config/` directory).

## Configuration

The demo uses a JSON configuration file that points to the Silver and Gold layer tables:

```json
{
  "source": {
    "container_metrics_table": "impulse_demo.default.container_metrics",
    "channel_metrics_table": "impulse_demo.default.channel_metrics",
    "channels_uri": "impulse_demo.default.channels",
    "container_tags_table": "impulse_demo.default.container_tags",
    "channel_tags_table": "impulse_demo.default.channel_tags"
  },
  "unity_sink": {
    "catalog": "impulse_demo",
    "schema": "gold",
    "table_prefix": "t"
  },
  "query_engine": {
    "solver": "DeltaSolver"
  },
  "measurement_dimensions": [
    "container_id",
    "vehicle_key",
    "start_ts",
    "stop_ts"
  ]
}
```

This tells the framework to read from `impulse_demo.default.*` Silver tables, use the `DeltaSolver`, and write Gold layer
results to `impulse_demo.gold.t_*`.

---

## Step 1: Imports and report initialization

Import the required classes and create a `Report` instance:

```python
import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient

from impulse_reporting.core.report import Report
from impulse_reporting.core.page import Page
from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.aggregations.histogram2d import Histogram2DDuration
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.events.container_event import ContainerEvent

ws = WorkspaceClient()
my_report = Report(
    name="my_report",
    spark=spark,
    workspace_client=ws,
    config_path="./config/config.json",
)
db = my_report.get_db()
```

The `Report` loads the configuration and sets up everything for ease of use.

---

## Step 2: Define physical and virtual channels

### Physical channels

Select channels by their metadata tags:

```python
eng_rpm = db.query.channel(channel_name='Engine RPM', brand='Seat', model='Leon')
veh_spd = db.query.channel(channel_name='Vehicle Speed Sensor', brand='Seat', model='Leon')
amb_air_temp = db.query.channel(channel_name='Ambient Air Temperature', brand='Seat', model='Leon')
intake_air_temp = db.query.channel(channel_name='Intake Air Temperature', brand='Seat', model='Leon')
```

### Virtual channels

Derive new channels using arithmetic:

```python
avg_temp = (amb_air_temp + intake_air_temp) / 2

distance_km = veh_spd.resample(1e6).cumtrapz() / 3600 / 1e6
```

- `avg_temp` computes the average of two temperature channels.
- `distance_km` resamples the speed signal to 1 Hz, integrates over time to get distance, and converts units from m/s to
  km.

### Event expressions

Define boolean expressions and interval-based conditions:

```python
rpm_event_expr = (eng_rpm > 2000) & (eng_rpm < 5000)

distance_event_expr = (distance_km % 10).intervals_between_falling_edges()
```

- `rpm_event_expr` identifies time windows where engine RPM is between 2000 and 5000.
- `distance_event_expr` creates intervals every 10 km of travel using modulo arithmetic and falling edge detection.

---

## Step 3: Define and register events

### BasicEvent

Wraps a TSAL expression into a named event with metadata:

```python
eng_rpm_event = BasicEvent(
    name="eng_rpm_event",
    expr=rpm_event_expr,
    desc="engine rpm > 2000 & engine rpm < 5000",
    required_channels=["Engine RPM"],
)
my_report.add_event(eng_rpm_event)

distance_event = BasicEvent(
    name="distance_event",
    expr=distance_event_expr,
    desc="Distance event",
)
my_report.add_event(distance_event)
```

### ContainerEvent

Spans the entire measurement container (no expression needed):

```python
container_event = ContainerEvent(name="container_event", desc="Container event")
my_report.add_event(container_event)
```

All the events are registered with `add_event()` so they can be referenced by aggregations.

---

## Step 4: Add aggregations to a page

Create a page and attach aggregations to it:

```python
my_first_page = Page(page_number=1)
my_report.add_page(my_first_page)
```

### 1D Histograms

```python
hist1 = HistogramDuration(
    name="rpm_hist_p1",
    base_expr=eng_rpm,
    event=eng_rpm_event,
    bins=[float(i) for i in range(0, 5000, 250)],
    desc="Engine RPM histogram within RPM events",
)
my_first_page.add_aggregation(hist1)

hist2 = HistogramDuration(
    name="speed_hist_p1",
    base_expr=veh_spd,
    event=eng_rpm_event,
    bins=[float(i) for i in range(0, 200, 10)],
    desc="Vehicle Speed histogram within RPM events",
)
my_first_page.add_aggregation(hist2)
```

Both histograms are scoped to `eng_rpm_event`, so only data within the RPM 2000-5000 band is included.

### 2D Histogram (heatmap)

```python
hist_3 = Histogram2DDuration(
    name="rpm_veh_spd_heatmap",
    x_expr=eng_rpm,
    y_expr=veh_spd,
    event=eng_rpm_event,
    x_bins=[float(i) for i in range(2000, 5000, 250)],
    y_bins=[float(i) for i in range(0, 200, 10)],
    desc="Engine RPM vs. vehicle speed heatmap within RPM events",
    x_channel_name="Engine RPM",
    y_channel_name="Vehicle Speed Sensor",
)
my_first_page.add_aggregation(hist_3)
```

### StatsAggregator

The demo creates three `StatsAggregator` aggregations, each using a different event:

```python
# Statistics within RPM events
stats_1 = StatsAggregator(
    name="stats_1",
    input_expressions=[eng_rpm, veh_spd, amb_air_temp, intake_air_temp, avg_temp, distance_km],
    channel_names=["EngRPM", "VehSpd", "Ambient_Air_Temp", "Intake_Air_Temp",
                  "Avg_Ambient_Intake_Temp", "Distance"],
    event=eng_rpm_event,
    desc="Statistics within RPM events",
    statistics=["min", "median", "mean", "max"],
)
my_first_page.add_aggregation(stats_1)

# Statistics on container level (full measurement)
stats_2 = StatsAggregator(
    name="stats_2",
    input_expressions=[eng_rpm, veh_spd, amb_air_temp, intake_air_temp, avg_temp, distance_km],
    channel_names=["EngRPM", "VehSpd", "Ambient_Air_Temp", "Intake_Air_Temp",
                  "Avg_Ambient_Intake_Temp", "Distance"],
    event=container_event,
    desc="Statistics on container level",
    statistics=["min", "median", "mean", "max"],
)
my_first_page.add_aggregation(stats_2)

# Statistics for 10 km distance bins
stats_3 = StatsAggregator(
    name="stats_3",
    input_expressions=[eng_rpm, veh_spd, amb_air_temp, intake_air_temp, avg_temp, distance_km],
    channel_names=["EngRPM", "VehSpd", "Ambient_Air_Temp", "Intake_Air_Temp",
                  "Avg_Ambient_Intake_Temp", "Distance"],
    event=distance_event,
    desc="Statistics for 10km bins",
    statistics=["min", "median", "mean", "max"],
)
my_first_page.add_aggregation(stats_3)
```

This demonstrates the same signals analyzed under three different event scopes: RPM band, full container, and 10 km
distance bins.

---

## Step 5: Compute and persist

Two calls execute the full pipeline:

```python
my_report.determine_report()
my_report.persist_results()
```

- `determine_report()` resolves all TSAL expressions, computes event instances, and runs all aggregations across all
  containers matched by the configuration.
- `persist_results()` writes fact and dimension tables to the Gold layer in Unity Catalog.

---

## Step 6: Inspect results

### Read Gold layer tables

```python
catalog = my_report.get_sink_config().catalog_name
schema = my_report.get_sink_config().schema_name
table_prefix = my_report.get_sink_config().table_prefix

hist_dim = spark.read.table(f"{catalog}.{schema}.{table_prefix}_histogram_dimension")
hist_fact = spark.read.table(f"{catalog}.{schema}.{table_prefix}_histogram_fact")

hist2d_dim = spark.read.table(f"{catalog}.{schema}.{table_prefix}_histogram2d_dimension")
hist2d_fact = spark.read.table(f"{catalog}.{schema}.{table_prefix}_histogram2d_fact")

stats_dim = spark.read.table(f"{catalog}.{schema}.{table_prefix}_stats_aggregator_dimension")
stats_fact = spark.read.table(f"{catalog}.{schema}.{table_prefix}_stats_aggregator_fact")

event_dim = spark.read.table(f"{catalog}.{schema}.{table_prefix}_event_dimension")
event_fact = spark.read.table(f"{catalog}.{schema}.{table_prefix}_event_instance_fact")

measurement_dim = spark.read.table(f"{catalog}.{schema}.{table_prefix}_measurement_dimension")
```

### Aggregate histogram across containers

Sum histogram bins across all containers to get an overall RPM distribution. The division by `1e6` converts duration
from microseconds to seconds:

```python
display(
    hist_fact
    .join(hist_dim, on="visual_id", how="inner")
    .where(F.col("name") == "rpm_hist_p1")
    .groupBy("name", "bin_id", "lower_bound", "upper_bound", "bin_name")
    .agg(F.sum(F.col("hist_value") / F.lit(1e6)).alias("hist_value"))
    .orderBy("bin_id")
)
```

### Pivot statistics into a wide table

Join statistics facts with event instances to create a per-event-instance pivot table:

```python
display(
    stats_fact.alias("l")
    .join(stats_dim.alias("d"), on="visual_id", how="inner")
    .join(event_fact.alias("e"), on="event_instance_id", how="inner")
    .where(F.col("d.name") == "stats_3")
    .withColumn("signal_agg_label",
                F.concat_ws("::", F.col("channel_name"), F.col("aggregation_label")))
    .groupBy("l.container_id", "event_instance_id", "start_ts", "end_ts")
    .pivot("signal_agg_label")
    .agg(F.first(F.col("statistic_value")).alias("value"))
    .orderBy(F.col("container_id").asc(), F.col("start_ts").asc())
)
```

### Visualize 2D histogram

```python
display(
    hist2d_fact
    .join(hist2d_dim, on="visual_id", how="inner")
    .where(F.col("name") == "rpm_veh_spd_heatmap")
    .groupBy("name", "x_bin_id", "y_bin_id", "x_lower_bound",
             "x_upper_bound", "y_lower_bound", "y_upper_bound",
             "x_bin_name", "y_bin_name")
    .agg(F.sum(F.col("hist_value")).alias("hist_value"))
    .withColumn("hist_value", F.col("hist_value") / F.lit(1e6))
    .orderBy("x_lower_bound", "y_lower_bound")
)
```

---

## Output tables

After running the notebook, the following tables are created in Unity Catalog under `impulse_demo.gold`:

| Table                     | Description                                                  |
|---------------------------|--------------------------------------------------------------|
| `t_measurement_dimension` | Container metadata (IDs, timestamps, vehicle keys).          |
| `t_event_dimension`       | Event definitions (3 events: RPM band, container, distance). |
| `t_event_instance_fact`   | Materialized event instances with start/end timestamps.      |
| `t_histogram_dimension`   | Histogram metadata (2 histograms: RPM and speed).            |
| `t_histogram_fact`        | Histogram bin values per container.                          |
| `t_histogram2d_dimension` | 2D histogram metadata (1 heatmap: RPM vs speed).             |
| `t_histogram2d_fact`      | 2D histogram bin values per container.                       |
| `t_stats_aggregator_dimension` | Statistics metadata (3 stats aggregations).                  |
| `t_stats_aggregator_fact`      | Statistics values per signal, event instance, and container. |

See the [Data Model](../data_model) for complete schema details.
