# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: setup
# MAGIC Creates the test schema, drops test schemas left over from failed runs, and checks that
# MAGIC the source data still matches `expected/containers.csv`.

# COMMAND ----------

import pyspark.sql.functions as F
from e2e_utils import check, params, spark

p = params()
spark.sql(f"CREATE SCHEMA {p.catalog}.{p.schema}")

# COMMAND ----------

# Only this job's own schemas: e2e_<run id>, owned by the running principal, older than a day.
stale = spark.sql(
    f"SELECT schema_name FROM {p.catalog}.information_schema.schemata "
    "WHERE schema_name RLIKE '^e2e_[0-9]+$' AND schema_owner = current_user() "
    "AND created < current_timestamp() - INTERVAL 1 DAY"
)
for row in stale.collect():
    spark.sql(f"DROP SCHEMA {p.catalog}.{row.schema_name} CASCADE")

# COMMAND ----------

containers = (
    spark.read.table(f"{p.source}container_tags")
    .groupBy("container_id")
    .pivot("key", ["experiment_id", "filename"])
    .agg(F.first("value"))
)
check(p, "containers", containers, test="")
