# Databricks notebook source
# MAGIC %md
# MAGIC ### General Setup

# COMMAND ----------

import json

from mda_reporting.aggregations.histogram import (
    HistogramDuration,
    HistogramCustomWeights,
    HistogramDistance,
)
from mda_reporting.aggregations.histogram2d import (
    Histogram2DDuration,
    Histogram2DCustomWeights,
    Histogram2DDistance,
)
from mda_reporting.core.page import Page
from mda_reporting.core.report import Report
from mda_reporting.events.basic_event import BasicEvent

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialization

# COMMAND ----------

dbutils.widgets.text("reset", "false")

reset = dbutils.widgets.get("reset")
config = json.load(open("./config/config_kvss.json"))

# Load config from external JSON

# Derive table references from config
catalog_out = config["unity_sink"]["catalog"]
schema_out = config["unity_sink"]["schema"]
prefix_out = config["unity_sink"]["table_prefix"]

# Gold target: reset tables if requested
if reset == "true":
    for suffix in [
        "histogram_fact",
        "histogram2d_fact",
        "histogram_dimension",
        "histogram2d_dimension",
        "event_instance_fact",
        "event_dimension",
        "measurement_dimension",
    ]:
        spark.sql(f"DROP TABLE IF EXISTS {catalog_out}.{schema_out}.{prefix_out}_{suffix}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_out}.{schema_out}")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "auto")
my_report = Report(name="my_report", spark=spark, config=config)
db = my_report.get_db()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Signal Queries & Virtual Signal Creation

# COMMAND ----------

# physical signals
eng_rpm = db.query.channel(channel_name="is1_eng_speed", data_key="TM")
veh_spd = db.query.channel(channel_name="can_vehicle_speed", data_key="TM")
odo_km = db.query.channel(channel_name="can_in_CGW_C1_TotalVehDist_Cval_DIAG", data_key="TM")

# event expressions
veh_spd_event = veh_spd > 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Event Definitions
# MAGIC All Events are defined in a way where all datapoints return _true_, effectively leading to a '_Measurement_' event.

# COMMAND ----------

veh_spd_event = BasicEvent(
    name="speed_event",
    expr=veh_spd_event,
    desc="Vehicle speed > 10",
    required_channels=["can_vehicle_speed"],
)

my_report.add_event(veh_spd_event)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Histogram Definition
# MAGIC
# MAGIC The ranges for the histograms were set to reproduce the same bin values as the legacy solution which uses automatic bin definition via bin size alone.

# COMMAND ----------

# Definition of 1st page
my_first_page = Page(page_number=1)
my_report.add_page(my_first_page)

# --- 1D Histograms ---

hist1 = HistogramDuration(
    name="speed_hist",
    base_expr=veh_spd,
    event=veh_spd_event,
    bins=[float(i) for i in range(0, 100, 5)],
    desc="Vehicle speed histogram within speed events",
    channel_name="can_vehicle_speed",
)
my_first_page.add_aggregation(hist1)

hist2 = HistogramDuration(
    name="rpm_hist",
    base_expr=eng_rpm,
    event=veh_spd_event,
    bins=[float(i) for i in range(0, 2400, 50)],
    desc="Engine speed histogram within speed events",
    channel_name="is1_eng_speed",
)
my_first_page.add_aggregation(hist2)

hist3 = HistogramDistance(
    name="histogram_1d_distance",
    base_expr=veh_spd,
    event=veh_spd_event,
    weights_expr=odo_km,
    bins=[float(i) for i in range(0, 120, 5)],
    desc="histogram 1d distance values",
    channel_name="can_in_CGW_C1_TotalVehDist_Cval_DIAG",
)
my_first_page.add_aggregation(hist3)

# --- 2D Histograms / Heatmaps ---

hist4 = Histogram2DDuration(
    name="rpm_torque_heatmap",
    x_expr=eng_rpm,
    y_expr=veh_spd,
    event=veh_spd_event,
    x_bins=[float(i) for i in range(0, 2400, 50)],
    y_bins=[float(i) for i in range(0, 120, 5)],
    desc="Engine RPM vs. vehicle speed heatmap within speed events",
    x_channel_name="is1_eng_speed",
    y_channel_name="can_vehicle_speed",
)
my_first_page.add_aggregation(hist4)

hist5 = Histogram2DCustomWeights(
    name="histogram_2d_custom_weights",
    x_expr=eng_rpm,
    y_expr=veh_spd,
    event=veh_spd_event,
    weights_expr=odo_km,
    x_bins=[float(i) for i in range(0, 2400, 50)],
    y_bins=[float(i) for i in range(0, 125, 5)],
    desc="histogram 2d custom weights",
    x_channel_name="is1_eng_speed",
    y_channel_name="can_vehicle_speed",
    weights_channel_name="can_in_CGW_C1_TotalVehDist_Cval_DIAG",
    math_fct_for_weights="diff",
)
my_first_page.add_aggregation(hist5)

hist6 = Histogram2DDistance(
    name="histogram_2d_distance",
    x_expr=eng_rpm,
    y_expr=veh_spd,
    event=veh_spd_event,
    weights_expr=odo_km,
    x_bins=[float(i) for i in range(0, 2400, 50)],
    y_bins=[float(i) for i in range(0, 125, 5)],
    desc="histogram 2d distance",
    x_channel_name="is1_eng_speed",
    y_channel_name="can_vehicle_speed",
)
my_first_page.add_aggregation(hist6)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Calculation and Extract Results from Gold Layer

# COMMAND ----------

my_report.determine_report()
my_report.persist_results()

hist_df = spark.read.table(f"{catalog_out}.{schema_out}.{prefix_out}_histogram_fact")
hist2d_df = spark.read.table(f"{catalog_out}.{schema_out}.{prefix_out}_histogram2d_fact")

hist_meta_df = spark.read.table(f"{catalog_out}.{schema_out}.{prefix_out}_histogram_dimension")
hist2d_meta_df = spark.read.table(f"{catalog_out}.{schema_out}.{prefix_out}_histogram2d_dimension")

event_df = spark.read.table(f"{catalog_out}.{schema_out}.{prefix_out}_event_instance_fact")
event_meta_df = spark.read.table(f"{catalog_out}.{schema_out}.{prefix_out}_event_dimension")
measurement_dim_df = spark.read.table(
    f"{catalog_out}.{schema_out}.{prefix_out}_measurement_dimension"
)
