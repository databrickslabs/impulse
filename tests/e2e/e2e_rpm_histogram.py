# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: RPM histogram
# MAGIC Runs the baseline report twice. The second run must update the gold tables in place.

# COMMAND ----------

from e2e_utils import add_rpm_histogram, check, config, gold, new_report, params

p = params()
report = new_report(config(p))
add_rpm_histogram(report)
report.run()
report.run()

# COMMAND ----------

for table in ["histogram_fact", "event_instance_fact"]:
    check(p, table, gold(p, table))
