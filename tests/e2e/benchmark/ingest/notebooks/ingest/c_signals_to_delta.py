# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest the fact signals of the current batch into `channels`
# MAGIC * Reads only the `trip_hash=*` directories of the containers in the current run
# MAGIC * Reads with an explicit schema of the 11 common columns and `recursiveFileLookup`, so the
# MAGIC   `trip_hash=`/`partition=` directories aren't inferred as partition columns
# MAGIC * Maps `session_id → container_id`, `signal_id → channel_id`, timestamps → epoch µs and
# MAGIC   `coalesce(float_value, int_value) → value`
# MAGIC * `channels_format=RLE` (default) writes `[tstart, tend)` intervals; `RAW` writes samples
# MAGIC * Replaces the batch's containers atomically (`replaceWhere`), so re-runs are idempotent

# COMMAND ----------

# MAGIC %run ./bundle_bootstrap

# COMMAND ----------

import pyspark.sql.functions as F

import utils
from utils import schema_definitions
from utils.constants import CHANNELS_FORMAT_RLE, CHANNELS_TABLE
from utils.notebook_helpers import (
    get_batch_context,
    read_catalog_schema_widgets,
    read_channels_format_widget,
    require_status_table,
)
from utils.transforms import rle_encode, samples_df, use_utc_session

use_utc_session(spark)

# COMMAND ----------

catalog, schema = read_catalog_schema_widgets(dbutils)
channels_format = read_channels_format_widget(dbutils)
channels_table = f"{catalog}.{schema}.{CHANNELS_TABLE}"
status_table_df = require_status_table(spark, dbutils, catalog, schema)

current_run_id = dbutils.jobs.taskValues.get(
    taskKey="get_next_batch", key="next_run_id", debugValue="NA"
)
batch = get_batch_context(status_table_df, current_run_id)
container_ids = batch["container_ids"]

print("current_run_id:", current_run_id)
print("containers:", container_ids)
print("channels_format:", channels_format, "->", channels_table)
if not container_ids:
    dbutils.notebook.exit("Nothing to do.")

# COMMAND ----------

fact_df = (
    spark.read.schema(schema_definitions.FACT_SOURCE_SCHEMA)
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.parquet")
    .parquet(*batch["source_dirs"])
)

samples = samples_df(fact_df)
channels_df = rle_encode(samples) if channels_format == CHANNELS_FORMAT_RLE else samples

expected_columns = schema_definitions.channels_schema(channels_format).fieldNames()
channels_df = channels_df.select(*expected_columns)

# COMMAND ----------

utils.replace_containers(channels_df, channels_table, container_ids)

# COMMAND ----------

# Guard: every container of the batch must have produced data, and nothing else may
# have been written (session_id in the files must match the status table).
written = {
    int(r["container_id"])
    for r in spark.read.table(channels_table)
    .where(F.col("container_id").isin(container_ids))
    .select("container_id")
    .distinct()
    .collect()
}
missing = sorted(set(container_ids) - written)
if missing:
    raise ValueError(f"no signal data written for containers {missing}")
print(f"wrote channels for {len(written)} containers")
