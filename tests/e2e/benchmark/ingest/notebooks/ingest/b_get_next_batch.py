# Databricks notebook source
# MAGIC %md
# MAGIC ### Select the next batch of containers to ingest
# MAGIC
# MAGIC Determines the next `run_id` and marks its containers `in_progress`. Selection priority:
# MAGIC 1. An existing `in_progress` run (re-run when `reprocess_current_run=True`, else fail)
# MAGIC 2. The latest `failed` run (when `reprocess_last_failed_run=True`)
# MAGIC 3. Up to `max_containers_per_run` `unprocessed` containers (lowest `container_id` first),
# MAGIC    assigned a fresh `run_id`
# MAGIC
# MAGIC `max_containers_per_run` bounds how much fact data one job run reads (~0.5 GB of source
# MAGIC Parquet / ~50M samples per container) so the 52 GB dataset is loaded incrementally.
# MAGIC Emits task value `next_run_id` (`noop` when there is nothing to do).

# COMMAND ----------

# MAGIC %run ./bundle_bootstrap

# COMMAND ----------

import hashlib
import uuid
from datetime import datetime

import pyspark.sql.functions as F

import utils
from utils.constants import (
    DEFAULT_MAX_CONTAINERS_PER_RUN,
    STATUS_FAILED,
    STATUS_IN_PROGRESS,
    STATUS_UNPROCESSED,
)
from utils.notebook_helpers import (
    parse_positive_int,
    read_catalog_schema_widgets,
    require_status_table,
)

# COMMAND ----------

# DBTITLE 1,Define widgets
catalog, schema = read_catalog_schema_widgets(dbutils)
dbutils.widgets.text("max_containers_per_run", str(DEFAULT_MAX_CONTAINERS_PER_RUN))
dbutils.widgets.dropdown("reprocess_current_run", "True", ["True", "False"])
dbutils.widgets.dropdown("reprocess_last_failed_run", "True", ["True", "False"])

max_containers_per_run = parse_positive_int(
    dbutils.widgets.get("max_containers_per_run"), "max_containers_per_run"
)
reprocess_current_run = dbutils.widgets.get("reprocess_current_run").lower() == "true"
reprocess_last_failed_run = dbutils.widgets.get("reprocess_last_failed_run").lower() == "true"

status_table_name = f"{catalog}.{schema}.status"
status_table_df = require_status_table(spark, dbutils, catalog, schema)

# COMMAND ----------

# DBTITLE 1,Resolve which batch to run
next_run_id = "noop"
logging_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

status_counts = (
    status_table_df.groupBy("status")
    .agg(
        F.countDistinct("run_id").alias("run_count"),
        F.count(F.lit(1)).alias("container_count"),
    )
    .collect()
)
runs_by_status = {row["status"]: row["run_count"] for row in status_counts}
containers_by_status = {row["status"]: row["container_count"] for row in status_counts}
print(f"containers by status: {containers_by_status}")

current_runs_count = runs_by_status.get(STATUS_IN_PROGRESS, 0)
current_failure_count = runs_by_status.get(STATUS_FAILED, 0)
unprocessed_count = containers_by_status.get(STATUS_UNPROCESSED, 0)

if current_runs_count > 1:
    raise Exception(f"""
                  [{logging_time}] It is not allowed to have more than 1 active run (status 'in_progress').
                  Please fix the status table. Exiting.
                  """)
elif current_runs_count == 1:
    if reprocess_current_run:
        next_run_id = utils.get_files_by_status(status_table_df, STATUS_IN_PROGRESS).first()[
            "run_id"
        ]
        print(f"[{logging_time}] Reprocessing last active run: {next_run_id}")
    else:
        raise Exception(f"""
                    [{logging_time}] There is already an active run in the status table.
                    If this run should be reprocessed, please set parameter 'reprocess_current_run' to 'True'
                    """)

elif current_failure_count > 0 and reprocess_last_failed_run:
    next_run_id = (
        utils.get_files_by_status(status_table_df, STATUS_FAILED)
        .orderBy(F.col("_load_ts").desc(), F.col("container_id").desc())
        .first()["run_id"]
    )
    print(f"[{logging_time}] Reprocessing last failed run: {next_run_id}")
    utils.update_status(spark, status_table_name, next_run_id, STATUS_IN_PROGRESS)

elif unprocessed_count > 0:
    if current_failure_count > 0:
        print(
            f"[{logging_time}] Warning: {current_failure_count} failed runs are not reprocessed "
            "(reprocess_last_failed_run=False)."
        )
    container_ids = [
        int(r["container_id"])
        for r in utils.get_files_by_status(status_table_df, STATUS_UNPROCESSED)
        .orderBy("container_id")
        .limit(max_containers_per_run)
        .select("container_id")
        .collect()
    ]
    next_run_id = hashlib.sha256(f"{logging_time}-{uuid.uuid4()}".encode()).hexdigest()
    utils.assign_run(spark, status_table_name, container_ids, next_run_id, STATUS_IN_PROGRESS)
    print(
        f"[{logging_time}] New run {next_run_id} with {len(container_ids)} of "
        f"{unprocessed_count} unprocessed containers: {container_ids}"
    )

print(f"run_id: {next_run_id}")

# COMMAND ----------

# DBTITLE 1,Expose next_run_id
dbutils.jobs.taskValues.set(key="next_run_id", value=next_run_id)
if next_run_id == "noop":
    dbutils.notebook.exit("Nothing to do.")
