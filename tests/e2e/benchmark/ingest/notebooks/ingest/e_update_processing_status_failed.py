# Databricks notebook source
# MAGIC %md
# MAGIC ### Update workflow run status to 'failed' in the status table.
# MAGIC * Trigger: on-failure step in the ingestion workflow
# MAGIC * Rolls back partial writes for the current batch and marks the run failed

# COMMAND ----------

# MAGIC %run ./bundle_bootstrap

# COMMAND ----------

import utils
from utils.constants import STATUS_FAILED
from utils.notebook_helpers import (
    get_batch_context,
    read_catalog_schema_widgets,
    require_status_table,
)

# COMMAND ----------

catalog, schema = read_catalog_schema_widgets(dbutils)
status_table_df = require_status_table(spark, dbutils, catalog, schema)

current_run_id = dbutils.jobs.taskValues.get(
    taskKey="get_next_batch", key="next_run_id", debugValue="NA"
)
container_ids = get_batch_context(status_table_df, current_run_id)["container_ids"]

# COMMAND ----------

utils.rollback_batch_tables(spark, catalog, schema, container_ids)
utils.update_status(spark, f"{catalog}.{schema}.status", current_run_id, STATUS_FAILED)
print(f"run {current_run_id} marked failed; rolled back containers {container_ids}")
