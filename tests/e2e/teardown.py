# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: teardown
# MAGIC Drops the test schema. Runs only when all tests passed; otherwise the schema is kept for
# MAGIC debugging and removed by a later setup.

# COMMAND ----------

from e2e_utils import params, spark

p = params()
if not p.schema.startswith("e2e_"):
    raise ValueError(f"Refusing to drop non-e2e schema {p.schema!r}")
spark.sql(f"DROP SCHEMA IF EXISTS {p.catalog}.{p.schema} CASCADE")
