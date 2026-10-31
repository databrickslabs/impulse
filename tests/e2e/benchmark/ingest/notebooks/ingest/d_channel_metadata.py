# Databricks notebook source
# MAGIC %md
# MAGIC ### Channel metadata for the ingested containers of the current batch
# MAGIC * `channel_metrics`: per-(container, channel) statistics computed from `channels`
# MAGIC   (count, NaN ratio, min/max/mean/std, p1/p10/p90/p99, time bounds, rate) plus
# MAGIC   `channel_name` / `unit` from `dim_signal_info` for wide channel selection
# MAGIC * `channel_tags`: EAV tags from `dim_signal_info` (`channel_name`, `unit`, `category`,
# MAGIC   `subsystem`, `physical_quantity`, `signal_mapping`, …)
# MAGIC * Only channels that actually have data in an ingested container get metadata

# COMMAND ----------

# MAGIC %run ./bundle_bootstrap

# COMMAND ----------

import pyspark.sql.functions as F

import utils
from utils.constants import (
    CHANNEL_METRICS_TABLE,
    CHANNEL_TAGS_TABLE,
    CHANNELS_TABLE,
    DIM_SIGNAL_INFO_DIR,
)
from utils.notebook_helpers import (
    get_batch_context,
    read_catalog_schema_widgets,
    read_channels_format_widget,
    read_dim,
    read_source_path_widget,
    require_status_table,
)
from utils.transforms import channel_metrics_df, channel_tags_df, use_utc_session

use_utc_session(spark)

# COMMAND ----------

catalog, schema = read_catalog_schema_widgets(dbutils)
source_path = read_source_path_widget(dbutils)
channels_format = read_channels_format_widget(dbutils)
status_table_df = require_status_table(spark, dbutils, catalog, schema)

channels_table = f"{catalog}.{schema}.{CHANNELS_TABLE}"
channel_metrics_table = f"{catalog}.{schema}.{CHANNEL_METRICS_TABLE}"
channel_tags_table = f"{catalog}.{schema}.{CHANNEL_TAGS_TABLE}"

current_run_id = dbutils.jobs.taskValues.get(
    taskKey="get_next_batch", key="next_run_id", debugValue="NA"
)
container_ids = get_batch_context(status_table_df, current_run_id)["container_ids"]
print("containers:", container_ids)
if not container_ids:
    dbutils.notebook.exit("Nothing to do.")

# COMMAND ----------

channels_df = spark.read.table(channels_table).where(F.col("container_id").isin(container_ids))
signal_info_df = read_dim(spark, source_path, DIM_SIGNAL_INFO_DIR)

metrics_df = channel_metrics_df(channels_df, channels_format, signal_info_df)
utils.replace_containers(metrics_df, channel_metrics_table, container_ids)

# COMMAND ----------

ingested_channels_df = (
    spark.read.table(channel_metrics_table)
    .where(F.col("container_id").isin(container_ids))
    .select("container_id", "channel_id")
)
tags_df = channel_tags_df(ingested_channels_df, signal_info_df)
utils.replace_containers(tags_df, channel_tags_table, container_ids)

print(f"channel metadata written for {len(container_ids)} containers")
