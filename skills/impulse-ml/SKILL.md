---
name: impulse-ml
description: >
  Turn Impulse measurement recordings into a machine-learning feature matrix. Use when the user wants
  to "make ML features from measurement data", "train a model on containers/recordings", build a
  per-drive / per-recording feature vector from sensor statistics, or feed Impulse output to MLflow or
  AutoML. Covers computing event-scoped statistics with a sinkless Report, reading them from
  `report.aggregation_dfs`, and pivoting to one row per container.
---

# Impulse — ML feature engineering

ML mode extracts event-scoped statistics as a flat feature matrix (one row per container, one column
per feature) that you pass to MLflow, AutoML, or scikit-learn. It reuses the reporting machinery but
does **not** persist to gold — you read the computed statistics straight off the report object.

The flow: compute a `StatsAggregator` over an event (usually a `ContainerEvent`) → pull the stats
DataFrame from `report.aggregation_dfs` → pivot to one row per container → train.

## 1. Compute per-recording statistics (sinkless)

Build a report **without `unity_sink`** so nothing is written (see `impulse-config`), select the
channels that characterize your recordings (see `impulse-tsal`), and aggregate over a `ContainerEvent`
so every sample is in scope (see `impulse-events` / `impulse-aggregations`).

```python
from databricks.sdk import WorkspaceClient
from impulse_reporting.core.report import Report
from impulse_reporting.core.page import Page
from impulse_reporting.events.container_event import ContainerEvent
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator

ws = WorkspaceClient()
config = {
    "source": {
        "container_metrics_table": "my_catalog.silver.container_metrics",
        "channel_metrics_table": "my_catalog.silver.channel_metrics",
        "channels_uri": "my_catalog.silver.channels",
        "container_tags_table": "my_catalog.silver.container_tags",
        "channel_tags_table": "my_catalog.silver.channel_tags",
    },
    # no unity_sink -> sinkless: determine_report() computes, persist is a no-op
    "query_engine": {"solver": "DefaultSolver", "data_type": "RLE"},
    "measurement_dimensions": ["container_id", "vehicle_key", "start_ts", "stop_ts"],
}

report = Report(name="feature_engineering", spark=spark, workspace_client=ws, config=config)
db = report.get_db()

eng_rpm      = db.query.channel(channel_name="Engine RPM", brand="Seat", model="Leon")
coolant_temp = db.query.channel(channel_name="Engine Coolant Temperature", brand="Seat", model="Leon")
veh_spd      = db.query.channel(channel_name="Vehicle Speed Sensor", brand="Seat", model="Leon")

container_event = ContainerEvent(name="container_event", desc="Full measurement recording")
report.add_event(container_event)

page = Page(page_number=1)
report.add_page(page)
page.add_aggregation(StatsAggregator(
    name="drive_stats",
    input_expressions=[eng_rpm, coolant_temp, veh_spd],
    channel_names=["Engine RPM", "Coolant Temp", "Vehicle Speed"],
    statistics=["min", "mean", "max"],
    event=container_event,
    desc="Per-recording statistics",
))

report.determine_report()   # computes in parallel across containers; no gold write needed
```

## 2. Read the computed statistics

After `determine_report()`, the results live on the report under `aggregation_dfs`, keyed by
aggregation type. `StatsAggregator` results are under `"STATS_AGGREGATOR"`, returned as a bundle keyed
by whether the rows were freshly computed (`"changed"`) or reused from a prior incremental run
(`"unchanged"`). In a fresh sinkless run take `"changed"`:

```python
stats_bundle = report.aggregation_dfs["STATS_AGGREGATOR"]
stats_df = stats_bundle.get("changed") or stats_bundle.get("unchanged")
```

`stats_df` has one row per `(container_id, channel_name, aggregation_label)` with a `statistic_value`
column (the `stats_aggregator_fact` shape — see `impulse-aggregations`).

## 3. Pivot to a feature matrix

Combine `channel_name` + `aggregation_label` into a feature name and pivot to one row per container:

```python
import pyspark.sql.functions as F

features_df = (
    stats_df
    .withColumn("feature", F.concat(F.col("channel_name"), F.lit("_"), F.col("aggregation_label")))
    .groupBy("container_id")
    .pivot("feature")
    .agg(F.first("statistic_value"))
)
```

That yields columns like `Engine RPM_mean`, `Vehicle Speed_max`, one row per recording — a feature
vector ready for training.

## 4. Add labels and train

Labels typically come from container tags. Join them on `container_id`, then hand the columns to any
trainer. With MLflow:

```python
import mlflow
from sklearn.ensemble import RandomForestClassifier

mlflow.set_registry_uri("databricks-uc")
mlflow.sklearn.autolog()

feature_cols = sorted(c for c in labeled_df.columns if c not in ("container_id", "label"))
pdf = labeled_df.toPandas().dropna(subset=feature_cols)

with mlflow.start_run(run_name="my_model"):
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(pdf[feature_cols], pdf["label"])
```

Register and deploy with the standard MLflow / Databricks Model Serving APIs from here — Impulse's role
ends once the feature matrix is built.

## Notes

- **Use a `ContainerEvent`** for per-recording features (whole recording in scope). Use a `BasicEvent`
  or `PointsInTimeEvent` when you want per-interval or per-instant features instead — the stats then
  carry an `event_instance_id` you can aggregate over (see `impulse-events`).
- **`StatsAggregator` inputs must be `SampleSeries`** (channels or arithmetic over them), not
  comparisons.
- To materialize the same statistics to gold instead of reading them inline (e.g. for a shared feature
  store), add a `unity_sink` and call `persist_results()` — that is `impulse-reporting`.
