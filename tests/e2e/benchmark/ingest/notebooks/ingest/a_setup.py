# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup for ingesting the Renumics fleet telemetry dataset into the Impulse silver layer
# MAGIC
# MAGIC * Creates the schema (if missing), the `status` table and the empty silver tables
# MAGIC   (`channels`, `channel_metrics`, `channel_tags`, `container_metrics`, `container_tags`)
# MAGIC * Verifies the source dataset (`source_path`) is present — download it first with the
# MAGIC   `Renumics_HF_Download` job
# MAGIC * `reset=True` drops only this bundle's tables, never the schema (it holds the raw volume)

# COMMAND ----------

# MAGIC %run ./bundle_bootstrap

# COMMAND ----------

import utils
from utils import schema_definitions
from utils.constants import CHANNELS_TABLE, FACT_DIR, STATUS_TABLE
from utils.notebook_helpers import (
    read_catalog_schema_widgets,
    read_channels_format_widget,
    read_source_path_widget,
)

# COMMAND ----------

# DBTITLE 1,Define widgets
dbutils.widgets.dropdown("reset", "False", ["True", "False"], "drop the ingest tables")
catalog, schema = read_catalog_schema_widgets(dbutils)
source_path = read_source_path_widget(dbutils)
channels_format = read_channels_format_widget(dbutils)
reset = dbutils.widgets.get("reset").lower() == "true"

print(f"target: {catalog}.{schema}, source: {source_path}, channels_format: {channels_format}")

# COMMAND ----------

# DBTITLE 1,Check source data
try:
    trip_dirs = dbutils.fs.ls(f"{source_path}/{FACT_DIR}")
except Exception as e:
    raise FileNotFoundError(
        f"Source data not found under {source_path}/{FACT_DIR}. "
        "Run the Renumics_HF_Download job first or point source_path at the dataset's assets/ folder."
    ) from e
print(f"source trip directories: {len(trip_dirs)}")

# COMMAND ----------

# DBTITLE 1,Create schema and tables
if reset:
    print(f"reset=True: dropping the ingest tables in {catalog}.{schema}")
    utils.drop_owned_tables(spark, catalog, schema)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

utils.create_table_if_missing(
    spark, f"{catalog}.{schema}.{STATUS_TABLE}", schema_definitions.STATUS_SCHEMA, ["container_id"]
)

channels_table = f"{catalog}.{schema}.{CHANNELS_TABLE}"
expected_channels_schema = schema_definitions.channels_schema(channels_format)
if not utils.create_table_if_missing(
    spark, channels_table, expected_channels_schema, ["container_id", "channel_id"]
):
    existing_cols = set(spark.read.table(channels_table).columns)
    expected_cols = set(expected_channels_schema.fieldNames())
    if existing_cols != expected_cols:
        raise ValueError(
            f"{channels_table} has columns {sorted(existing_cols)}, but channels_format={channels_format} "
            f"expects {sorted(expected_cols)}. Use the format the table was created with, or reset."
        )

for table_name, table_schema, cluster_cols in schema_definitions.SILVER_TABLE_SPECS:
    utils.create_table_if_missing(
        spark, f"{catalog}.{schema}.{table_name}", table_schema, cluster_cols
    )

print("setup complete")
