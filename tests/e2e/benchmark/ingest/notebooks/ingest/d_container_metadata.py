# Databricks notebook source
# MAGIC %md
# MAGIC ### Container metadata for the ingested containers of the current batch
# MAGIC * `container_metrics`: one row per trip — time bounds and channel count (from the batch's
# MAGIC   `channel_metrics`), enriched with `dim_trip_info`, `dim_vehicle_info` and
# MAGIC   `dim_vehicle_state`. `vehicle_key` = vehicle model name. `_ingested_at` drives
# MAGIC   incremental Impulse reports
# MAGIC * `container_tags`: the descriptive `container_metrics` columns as EAV tags

# COMMAND ----------

# MAGIC %run ./bundle_bootstrap

# COMMAND ----------

import pyspark.sql.functions as F

import utils
from utils.constants import (
    CHANNEL_METRICS_TABLE,
    CONTAINER_METRICS_TABLE,
    CONTAINER_TAGS_TABLE,
    DIM_TRIP_INFO_DIR,
    DIM_VEHICLE_INFO_DIR,
    DIM_VEHICLE_STATE_DIR,
)
from utils.notebook_helpers import (
    get_batch_context,
    read_catalog_schema_widgets,
    read_dim,
    read_source_path_widget,
    require_status_table,
)
from utils.transforms import container_metrics_df, container_tags_df, use_utc_session

use_utc_session(spark)

# COMMAND ----------

catalog, schema = read_catalog_schema_widgets(dbutils)
source_path = read_source_path_widget(dbutils)
status_table_df = require_status_table(spark, dbutils, catalog, schema)

channel_metrics_table = f"{catalog}.{schema}.{CHANNEL_METRICS_TABLE}"
container_metrics_table = f"{catalog}.{schema}.{CONTAINER_METRICS_TABLE}"
container_tags_table = f"{catalog}.{schema}.{CONTAINER_TAGS_TABLE}"

current_run_id = dbutils.jobs.taskValues.get(
    taskKey="get_next_batch", key="next_run_id", debugValue="NA"
)
batch = get_batch_context(status_table_df, current_run_id)
container_ids = batch["container_ids"]
print("containers:", container_ids)
if not container_ids:
    dbutils.notebook.exit("Nothing to do.")

# COMMAND ----------

channel_metrics = spark.read.table(channel_metrics_table).where(
    F.col("container_id").isin(container_ids)
)
metrics_df = container_metrics_df(
    channel_metrics,
    batch["batch_df"],
    read_dim(spark, source_path, DIM_TRIP_INFO_DIR),
    read_dim(spark, source_path, DIM_VEHICLE_INFO_DIR),
    read_dim(spark, source_path, DIM_VEHICLE_STATE_DIR),
)
utils.replace_containers(metrics_df, container_metrics_table, container_ids)

# COMMAND ----------

container_metrics = spark.read.table(container_metrics_table).where(
    F.col("container_id").isin(container_ids)
)
if container_metrics.count() != len(container_ids):
    raise ValueError("container_metrics row count does not match the batch size")

utils.replace_containers(container_tags_df(container_metrics), container_tags_table, container_ids)
display(
    container_metrics.select(
        "container_id", "vehicle_key", "trip_name", "duration_s", "num_channels"
    )
)
