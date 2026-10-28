# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: incremental
# MAGIC Loads the source containers into a copy of the silver tables in two steps and runs the
# MAGIC baseline report incrementally after each, in batches of 15 containers. The second load
# MAGIC must leave the first load's rows untouched and commit one batch at a time. The result
# MAGIC must equal the full run in `expected/rpm_histogram`.

# COMMAND ----------

import math

from e2e_utils import (
    add_rpm_histogram,
    assert_unchanged,
    check,
    commits_since,
    config,
    copy_silver,
    gold,
    load_silver,
    new_report,
    params,
    spark,
    versions,
)

p = params()
silver = copy_silver(p)
cap = 15
report = new_report(
    config(
        p,
        source=silver,
        incremental={"enabled": True},
        query_engine={"max_containers_per_batch": cap},
    )
)
add_rpm_histogram(report)
tables = ["histogram_fact", "event_instance_fact"]

# COMMAND ----------

load_silver(p, silver, "container_id <= 40")
report.run()

# COMMAND ----------

before = versions(p, tables)
load_silver(p, silver, "container_id > 40")
report.run()

# Only the new containers were processed.
assert_unchanged(p, before, "container_id <= 40")

# One MERGE per batch of at most `cap` containers (10 histogram bins each).
new = spark.read.table(f"{silver}container_metrics").where("container_id > 40").count()
commits = commits_since(p, "histogram_fact", before["histogram_fact"])
assert commits.where("operation != 'MERGE'").count() == 0, "a batch is not a single MERGE"
batches = commits.count()
assert batches == math.ceil(new / cap), f"{batches} batches for {new} containers"
inserted = commits.selectExpr("int(operationMetrics['numTargetRowsInserted']) AS n")
assert inserted.where(f"n > {cap * 10}").count() == 0, "a batch exceeds the cap"

# COMMAND ----------

for table in tables:
    check(p, table, gold(p, table), test="rpm_histogram")
