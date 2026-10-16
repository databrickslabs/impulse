# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: setup
# MAGIC Creates the schema the report writes to.

# COMMAND ----------

from databricks.sdk.runtime import dbutils
from pyspark.sql import SparkSession

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

SparkSession.builder.getOrCreate().sql(f"CREATE SCHEMA `{catalog}`.`{schema}`")
