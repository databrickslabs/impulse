# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: RPM histogram
# MAGIC Runs the baseline report compute-only (nothing may be written), then twice with
# MAGIC persistence. The second run must update the gold tables in place.

# COMMAND ----------

from e2e_utils import add_rpm_histogram, check, config, gold, new_report, params, spark

from impulse_reporting.persist.fact_schema import HISTOGRAM_FACT_SCHEMA

p = params()
report = new_report(config(p))
add_rpm_histogram(report)

# COMMAND ----------

report.run(persist_results=False)
tables = spark.sql(f"SHOW TABLES IN {p.catalog}.{p.schema} LIKE '{p.prefix}_*'").collect()
assert not tables, f"compute-only run wrote tables: {[t.tableName for t in tables]}"
histogram = report.aggregation_dfs["HISTOGRAM"]["changed"]
check(p, "histogram_fact", histogram.select(HISTOGRAM_FACT_SCHEMA.fieldNames()))

# COMMAND ----------

report.run()
report.run()
for table in ["histogram_fact", "event_instance_fact"]:
    check(p, table, gold(p, table))
