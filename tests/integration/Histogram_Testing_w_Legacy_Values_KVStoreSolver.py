# Databricks notebook source
# MAGIC %md
# MAGIC ## Histogram Testing with Legacy Values – KeyValueStoreSolver
# MAGIC
# MAGIC This notebook tests the `KeyValueStoreSolver` architecture using
# MAGIC **two signals only**: `is1_eng_speed` and `can_vehicle_speed`.
# MAGIC
# MAGIC | Config Key | Table | Role |
# MAGIC |---|---|---|
# MAGIC | `container_tags_table` | `avl_meta.data_model.concept_entities` | metadata table (entity_id → container_id) |
# MAGIC | `container_metrics_table` | Bronze container metrics | Container-level metrics |
# MAGIC | `channel_metrics_table` | Bronze channel metrics | Channel-level metrics |
# MAGIC | `channels_uri` | Bronze channel data | Time-series data |

# COMMAND ----------

# MAGIC %md
# MAGIC ### General Setup

# COMMAND ----------

import json
import matplotlib.pyplot as plt
import numpy as np
import pyspark.sql.functions as F
from pytest import approx
from mda_query_engine.analyze.metadata.tag_expression import *
from mda_query_engine.analyze.query.solvers.basic_narrow_solver import BasicNarrowSolver
from mda_query_engine.analyze.query.solvers.key_value_store_solver import KeyValueStoreSolver
from mda_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from mda_query_engine.model.series.sample_series import SampleSeries
from mda_reporting.aggregations.histogram import HistogramDuration
from mda_reporting.core.page import Page
from mda_reporting.core.report import Report
from mda_reporting.events.basic_event import BasicEvent

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialization

# COMMAND ----------

dbutils.widgets.text("catalog_in", "development")
dbutils.widgets.text("schema_in", "bronze_e2e")
dbutils.widgets.text("catalog_out", "development")
dbutils.widgets.text("schema_out", "gold_e2e")
dbutils.widgets.text("prefix_out", "legacy_kvs")
dbutils.widgets.text("reset", "false")

catalog_in = dbutils.widgets.get("catalog_in")
schema_in = dbutils.widgets.get("schema_in")
catalog_out = dbutils.widgets.get("catalog_out")
schema_out = dbutils.widgets.get("schema_out")
prefix_out = dbutils.widgets.get("prefix_out")
reset = dbutils.widgets.get("reset")

if reset == "true":
    spark.sql(f"DROP TABLE IF EXISTS {catalog_out}.{schema_out}.{prefix_out}_histogram_fact")
    spark.sql(f"DROP TABLE IF EXISTS {catalog_out}.{schema_out}.{prefix_out}_histogram_dimension")
    spark.sql(f"DROP TABLE IF EXISTS {catalog_out}.{schema_out}.{prefix_out}_event_instance_fact")
    spark.sql(f"DROP TABLE IF EXISTS {catalog_out}.{schema_out}.{prefix_out}_event_dimension")
    spark.sql(
        f"DROP TABLE IF EXISTS {catalog_out}.{schema_out}.{prefix_out}_measurement_dimension"
    )

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_out}.{schema_out}")

# COMMAND ----------

config = json.load(open("./config/config_kv.json"))
config["source"]["container_metrics_table"] = f"{catalog_in}.{schema_in}.container_metric"
config["source"]["channel_metrics_table"] = f"{catalog_in}.{schema_in}.channel_metric"
config["source"]["channels_uri"] = f"{catalog_in}.{schema_in}.channel_data"

config["unity_sink"]["catalog"] = catalog_out
config["unity_sink"]["schema"] = schema_out
config["unity_sink"]["table_prefix"] = prefix_out

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "auto")
my_report = Report(name="my_report_kvs", spark=spark, config=config)
db = my_report.get_db()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate KeyValueStoreSolver Setup

# COMMAND ----------

solver = my_report.get_solver()

print(f"Solver type        : {type(solver).__name__}")
assert isinstance(solver, KeyValueStoreSolver), "Expected KeyValueStoreSolver"

print(f"Project ID         : {solver.config.project_id}")
print(f"container_id_col   : {solver.config.container_id_col}")
print(f"project_id_col     : {solver.config.project_id_col}")

print(f"\nContainer tags table    : {db.config.container_tags_table}")
print(f"Container metrics table : {db.config.container_metrics_table}")
print(f"Channel metrics table   : {db.config.channel_metrics_table}")
print(f"Channels URI            : {db.config.channels_uri}")

assert db.config.container_tags_table is not None, "container_tags_table must be set"
assert (
    db.config.container_tags_table != db.config.container_metrics_table
), "container_tags and container_metrics must be different tables"

print("\n[OK] KeyValueStoreSolver setup validated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Signal Queries (eng_rpm + veh_spd only)

# COMMAND ----------

# Physical signals — only two channels
eng_rpm = db.query.channel(channel_name="is1_eng_speed", data_key="TM")
veh_spd = db.query.channel(channel_name="can_vehicle_speed", data_key="TM")

# Event expressions
veh_spd_event = veh_spd > -1
eng_rpm_event = eng_rpm > -1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Event Definitions
# MAGIC All events return _true_ for all datapoints → effectively a 'Measurement' event.

# COMMAND ----------

veh_spd_event = BasicEvent(
    name="speed_event",
    expr=veh_spd_event,
    desc="Vehicle speed > -1",
    required_channels=["can_vehicle_speed"],
)
rpm_event = BasicEvent(
    name="eng_rpm_event",
    expr=eng_rpm_event,
    desc="Engine RPM > -1",
    required_channels=["is1_eng_speed"],
)

my_report.add_event(veh_spd_event)
my_report.add_event(rpm_event)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Histogram Definition (RPM + Speed only)

# COMMAND ----------

my_first_page = Page(page_number=1)
my_report.add_page(my_first_page)

# RPM histogram within RPM events
hist1_name = "rpm_hist_p1"
hist1_desc = "Engine RPM histogram within RPM events"
hist1_bins = [float(i) for i in range(0, 2750, 250)]
hist1 = HistogramDuration(
    name=hist1_name,
    base_expr=eng_rpm,
    event=rpm_event,
    bins=hist1_bins,
    desc=hist1_desc,
    channel_name="is1_eng_speed",
)
my_first_page.add_aggregation(hist1)

# Speed histogram within RPM events
hist2_name = "speed_hist_p1"
hist2_desc = "Vehicle speed histogram within RPM events"
hist2_bins = [float(i) for i in range(0, 100, 5)]
hist2 = HistogramDuration(
    name=hist2_name,
    base_expr=veh_spd,
    event=rpm_event,
    bins=hist2_bins,
    desc=hist2_desc,
    channel_name="can_vehicle_speed",
)
my_first_page.add_aggregation(hist2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Calculation and Extract Results from Gold Layer

# COMMAND ----------

my_report.determine_report()
my_report.persist_results()

hist_df = spark.read.table(f"{catalog_out}.{schema_out}.{prefix_out}_histogram_fact")
hist_meta_df = spark.read.table(f"{catalog_out}.{schema_out}.{prefix_out}_histogram_dimension")
measurement_dim_df = spark.read.table(
    f"{catalog_out}.{schema_out}.{prefix_out}_measurement_dimension"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RPM Histogram

# COMMAND ----------

interesting_files = [
    "AA550642_Continuous_20240617_121025_20240617_122404.MF4",
]
result_histogram_rpm_duration = (
    hist_df.join(hist_meta_df, on="visual_id", how="inner")
    .where(F.col("name") == F.lit("rpm_hist_p1"))
    .join(
        F.broadcast(measurement_dim_df.where(F.col("file_name").isin(interesting_files))),
        on="container_id",
        how="inner",
    )
    .groupBy(
        F.col("name"),
        F.col("bin_ID"),
        F.col("lower_bound"),
        F.col("upper_bound"),
        F.col("bin_name"),
    )
    .agg(F.sum(F.col("hist_value")).alias("hist_value"))
    .orderBy(F.col("bin_id").asc())
    .collect()
)

actual_values_histogram_rpm_duration = [row["hist_value"] for row in result_histogram_rpm_duration]

# --------- Legacy Values ---------

legacy_values_histogram_rpm_duration = [
    3.9,
    0.49999999999993205,
    398.60000000000326,
    202.30000000001803,
    159.80000000001235,
    34.5000000000033,
    16.900000000000773,
    3.0,
    0.0,
    0.0,
]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Speed Histogram

# COMMAND ----------

result_histogram_speed_duration = (
    hist_df.join(hist_meta_df, on="visual_id", how="inner")
    .where(F.col("name") == F.lit("speed_hist_p1"))
    .join(
        F.broadcast(measurement_dim_df.where(F.col("file_name").isin(interesting_files))),
        on="container_id",
        how="inner",
    )
    .groupBy(
        F.col("name"),
        F.col("bin_ID"),
        F.col("lower_bound"),
        F.col("upper_bound"),
        F.col("bin_name"),
    )
    .agg(F.sum(F.col("hist_value")).alias("hist_value"))
    .orderBy(F.col("bin_id").asc())
    .select("hist_value")
    .collect()
)

actual_values_histogram_speed_duration = [
    row["hist_value"] for row in result_histogram_speed_duration
]

# --------- Legacy Values ---------

legacy_values_histogram_speed_duration = [
    354.0000000000007,
    36.00000000000142,
    43.80000000000183,
    28.900000000001114,
    26.800000000000864,
    26.70000000000084,
    28.60000000000025,
    34.70000000000209,
    56.60000000000787,
    130.00000000001785,
    50.50000000000546,
    2.89999999999975,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assertions

# COMMAND ----------

print("Actual RPM histogram values:", actual_values_histogram_rpm_duration)
print("Legacy RPM histogram values:", legacy_values_histogram_rpm_duration)
print("Actual Speed histogram values:", actual_values_histogram_speed_duration)
print("Legacy Speed histogram values:", legacy_values_histogram_speed_duration)

assert actual_values_histogram_rpm_duration == approx(
    legacy_values_histogram_rpm_duration, rel=0.01
)
assert actual_values_histogram_speed_duration == approx(
    legacy_values_histogram_speed_duration, rel=0.001
)

print("[OK] RPM histogram values match legacy values")
print("[OK] Speed histogram values match legacy values")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Comparison

# COMMAND ----------

histogram_values = [
    [actual_values_histogram_rpm_duration, legacy_values_histogram_rpm_duration],
    [actual_values_histogram_speed_duration, legacy_values_histogram_speed_duration],
]

names = [
    "RPM Duration",
    "Speed Duration",
]
for elem, name in zip(histogram_values, names, strict=False):
    print(f"\n----------------------------  {name}  -----------------------\n")
    for i, (actual, legacy) in enumerate(zip(elem[0], elem[1], strict=False)):
        if legacy == 0:
            rel_diff = float("inf") if actual != 0 else 0
        else:
            rel_diff = abs(actual - legacy) / abs(legacy)
        print(
            f"Index {i}: actual={actual}, legacy={legacy}, rel_diff={rel_diff}, diff={(actual - legacy)}"
        )
