# Databricks notebook source
# MAGIC %md
# MAGIC # e2e: incremental
# MAGIC Loads the source containers into a copy of the silver tables in two steps and runs the
# MAGIC baseline report incrementally after each, in batches of 15 containers. The result must
# MAGIC equal the full run in `expected/rpm_histogram`.

# COMMAND ----------

from e2e_utils import (
    SILVER_TABLES,
    add_rpm_histogram,
    check,
    config,
    expected,
    gold,
    new_report,
    params,
    spark,
)

p = params()
silver = f"{p.catalog}.{p.schema}.{p.prefix}_src_"
for table in SILVER_TABLES:
    spark.sql(f"CREATE TABLE {silver}{table} AS SELECT * FROM {p.source}{table} WHERE false")

report = new_report(
    config(
        p,
        source=silver,
        incremental={"enabled": True},
        query_engine={"max_containers_per_batch": 15},
    )
)
add_rpm_histogram(report)

# COMMAND ----------

for containers in ["container_id <= 40", "container_id > 40"]:
    for table in SILVER_TABLES:
        spark.sql(
            f"INSERT INTO {silver}{table} SELECT * FROM {p.source}{table} WHERE {containers}"
        )
    report.run()

# COMMAND ----------

for table in ["histogram_fact", "event_instance_fact"]:
    actual = gold(p, table)
    check(p, table, actual, expected(p, table, actual.schema, test="rpm_histogram"))
