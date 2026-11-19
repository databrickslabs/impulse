# Databricks notebook source
# MAGIC %md
# MAGIC ### Mark the current run as succeeded and compact the silver tables

# COMMAND ----------

# MAGIC %run ./bundle_bootstrap

# COMMAND ----------

import pyspark.sql.functions as F

import utils
from utils.constants import STATUS_SUCCEEDED
from utils.notebook_helpers import read_catalog_schema_widgets

# COMMAND ----------

catalog, schema = read_catalog_schema_widgets(dbutils)
status_table_name = f"{catalog}.{schema}.status"
current_run_id = dbutils.jobs.taskValues.get(
    taskKey="get_next_batch", key="next_run_id", debugValue="NA"
)

# COMMAND ----------

utils.update_status(spark, status_table_name, current_run_id, STATUS_SUCCEEDED)

# COMMAND ----------

utils.optimize_tables(spark, catalog, schema)

remaining = (
    spark.read.table(status_table_name).where(F.col("status") != F.lit(STATUS_SUCCEEDED)).count()
)
print(f"ALL DONE — {remaining} containers not yet ingested")
