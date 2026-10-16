# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: fail on error
# MAGIC Runs only when an earlier step failed, and fails the job run.

# COMMAND ----------

raise RuntimeError("An e2e step failed; see the failed task run.")
