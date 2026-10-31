# Databricks notebook source
# MAGIC %md
# MAGIC ### Detect new trips (containers) that need to be processed
# MAGIC
# MAGIC * Lists the fact files under `<source_path>/fact_signal_data/trip_hash=*/partition=*/`
# MAGIC   (file listing only — no data is read)
# MAGIC * Every new `trip_hash` directory becomes one container. Its `session_id`, `vehicle_id`
# MAGIC   and `vehicle_state_id` are read from one Parquet footer; `container_id = session_id`
# MAGIC   so the silver layer joins back to the source `dim_trip_info`
# MAGIC * New containers are added to the `status` table as `unprocessed` (insert-only, idempotent)

# COMMAND ----------

# MAGIC %run ./bundle_bootstrap

# COMMAND ----------

import pyspark.sql.functions as F
from delta.tables import DeltaTable

from utils import schema_definitions
from utils.constants import STATUS_TABLE, STATUS_UNPROCESSED
from utils.notebook_helpers import read_catalog_schema_widgets, read_source_path_widget
from utils.source_data import TRIP_HASH_PATTERN, fact_root, read_trip_keys, trip_dir

# COMMAND ----------

# DBTITLE 1,Resolve runtime parameters
catalog, schema = read_catalog_schema_widgets(dbutils)
source_path = read_source_path_widget(dbutils)
status_table_name = f"{catalog}.{schema}.{STATUS_TABLE}"
print("scanning:", fact_root(source_path))

# COMMAND ----------

# DBTITLE 1,List fact files per trip
# binaryFile with only path/length selected lists files without reading their content.
files_df = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.parquet")
    .option("recursiveFileLookup", "true")
    .load(fact_root(source_path))
    .select("path", "length")
    .withColumn("trip_hash", F.regexp_extract("path", TRIP_HASH_PATTERN, 1))
    .where(F.col("trip_hash") != "")
)

known_trips_df = spark.read.table(status_table_name).select("trip_hash")
new_trips = (
    files_df.groupBy("trip_hash")
    .agg(
        F.count(F.lit(1)).cast("int").alias("file_count"),
        F.sum("length").cast("bigint").alias("size_bytes"),
        F.min("path").alias("sample_file"),
    )
    .join(known_trips_df, on="trip_hash", how="left_anti")
    .orderBy("trip_hash")
    .collect()
)
print(f"new trips: {len(new_trips)}")
if not new_trips:
    dbutils.notebook.exit("No new trips.")

# COMMAND ----------

# DBTITLE 1,Resolve container ids from Parquet footers
rows = []
for trip in new_trips:
    keys = read_trip_keys(trip["sample_file"])
    rows.append(
        (
            keys["session_id"],
            trip["trip_hash"],
            trip_dir(source_path, trip["trip_hash"]),
            keys["vehicle_id"],
            keys["vehicle_state_id"],
            trip["file_count"],
            trip["size_bytes"],
            None,
            STATUS_UNPROCESSED,
            None,
            None,
        )
    )

new_status_df = spark.createDataFrame(rows, schema_definitions.STATUS_SCHEMA).withColumn(
    "_load_ts", F.current_timestamp()
)

clashes = (
    new_status_df.alias("n")
    .join(spark.read.table(status_table_name).alias("s"), on="container_id")
    .where(F.col("n.trip_hash") != F.col("s.trip_hash"))
    .count()
)
duplicate_ids = new_status_df.groupBy("container_id").count().where("count > 1").count()
if clashes or duplicate_ids:
    raise ValueError("session_id is not unique per trip_hash; cannot use it as container_id")

# COMMAND ----------

# DBTITLE 1,Register new containers
(
    DeltaTable.forName(spark, status_table_name)
    .alias("t")
    .merge(new_status_df.alias("s"), "t.trip_hash = s.trip_hash")
    .whenNotMatchedInsertAll()
    .execute()
)
display(new_status_df.select("container_id", "trip_hash", "file_count", "size_bytes"))
