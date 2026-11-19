# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: RPM histogram
# MAGIC Runs the baseline report compute-only (nothing may be written), then twice with
# MAGIC persistence. The second run must update the gold tables in place. The histogram must
# MAGIC add up to the duration of the RPM events.

# COMMAND ----------

from e2e_utils import (
    add_rpm_histogram,
    assert_histogram_covers_events,
    check,
    commits_since,
    config,
    gold,
    new_report,
    params,
    spark,
    versions,
)

from impulse_reporting.persist.fact_schema import (
    EVENT_INSTANCE_FACT_SCHEMA,
    HISTOGRAM_FACT_SCHEMA,
)

p = params()
report = new_report(config(p))
add_rpm_histogram(report)
tables = ["histogram_fact", "event_instance_fact"]

# COMMAND ----------

report.run(persist_results=False)
written = spark.sql(f"SHOW TABLES IN {p.catalog}.{p.schema} LIKE '{p.prefix}_*'").collect()
assert not written, f"compute-only run wrote tables: {[t.tableName for t in written]}"
histogram = report.aggregation_dfs["HISTOGRAM"]["changed"]
check(p, "histogram_fact", histogram.select(HISTOGRAM_FACT_SCHEMA.fieldNames()))
events = report.event_dfs["BASIC_EVENT"]["changed"]
check(p, "event_instance_fact", events.select(EVENT_INSTANCE_FACT_SCHEMA.fieldNames()))

# COMMAND ----------


def table_ids():
    """Delta table ids, which change if a table is dropped and recreated."""
    return {
        t: spark.sql(f"DESCRIBE DETAIL {p.catalog}.{p.schema}.{p.prefix}_{t}").first().id
        for t in tables
    }


report.run()
ids, before = table_ids(), versions(p, tables)
report.run()
assert table_ids() == ids, "the second run recreated gold tables"
not_rewritten = [t for t in tables if not commits_since(p, t, before[t]).count()]
assert not not_rewritten, f"not rewritten: {not_rewritten}"

for table in tables:
    check(p, table, gold(p, table))
assert_histogram_covers_events(gold(p, "histogram_fact"), gold(p, "event_instance_fact"))
