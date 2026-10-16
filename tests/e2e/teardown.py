# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: teardown
# MAGIC Drops the test schema. Runs even when an earlier step failed.

# COMMAND ----------

from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

if not schema.startswith("e2e_"):
    raise ValueError(f"Refusing to drop non-e2e schema {schema!r}")
SparkSession.builder.getOrCreate().sql(f"DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE")
