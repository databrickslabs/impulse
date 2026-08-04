# MDF data sources — quickstart

Read ASAM MDF4 files as Spark DataFrames. Install the `impulse_data_sources` package on the
cluster, then register the three data sources once per session.

## Setup

```python
from databricks.sdk import WorkspaceClient
from impulse_data_sources.mdf import register_mdf_datasources

register_mdf_datasources(spark, WorkspaceClient())
```

Set `path` to the directory that contains your `.mf4` files (discovered
recursively). To read specific files only, add `files` as a comma-separated list
(relative to `path`, or absolute paths).

---

## `mdf_signals` — time-series samples

One row per sample: `(file_uri, channel_id, time, value)`.

```python
signals = (
    spark.read.format("mdf_signals")
    .option("path", "/Volumes/catalog/schema/mdf_data")
    .load()
)

signals.select("file_uri", "channel_id", "time", "value").show(5)
```

Read a single file and use smaller on-disk types:

```python
signals = (
    spark.read.format("mdf_signals")
    .option("path", "/Volumes/catalog/schema/mdf_data")
    .option("files", "run_001.mf4")
    .option("time_dtype", "float32")
    .option("value_dtype", "float32")
    .load()
)
```

---



## `mdf_metadata` — channel catalog

One row per signal channel: names, units, group indices, and comments.

```python
metadata = (
    spark.read.format("mdf_metadata")
    .option("path", "/Volumes/catalog/schema/mdf_data")
    .load()
)

metadata.select(
    "file_uri", "channel_id", "channel_name", "unit", "header_datetime"
).show(5)
```

Join signals to metadata on `(file_uri, channel_id)` to get human-readable names:

```python
(
    signals.join(metadata, on=["file_uri", "channel_id"])
    .select("channel_name", "unit", "time", "value")
    .show(5)
)
```

---



## `mdf_masters` — per-group time base

One row per original master-channel sample: `(file_uri, group_idx, timestamp)`.
Use with run-length-encoded signals to recover the full per-sample grid (see
[README.md](README.md) for the join predicate).

```python
masters = (
    spark.read.format("mdf_masters")
    .option("path", "/Volumes/catalog/schema/mdf_data")
    .load()
)

masters.select("file_uri", "group_idx", "timestamp").show(5)
```

RLE signals plus masters (use the same `time_dtype` / `absolute_time` on both):

```python
rle = (
    spark.read.format("mdf_signals")
    .option("path", "/Volumes/catalog/schema/mdf_data")
    .option("run_length_encoding", "true")
    .load()
)

masters = (
    spark.read.format("mdf_masters")
    .option("path", "/Volumes/catalog/schema/mdf_data")
    .load()
)

# Expand intervals onto the master time grid (simplified; see README for full predicate).
from pyspark.sql import functions as F

expanded = (
    rle.join(masters, on=["file_uri"])
    .where(
        (F.col("timestamp") >= F.col("tstart"))
        & (
            (F.col("timestamp") < F.col("tend"))
            | ((F.col("tstart") == F.col("tend")) & (F.col("timestamp") == F.col("tstart")))
        )
    )
    .select("file_uri", "channel_id", "timestamp", "value")
)
```

---



## Write to Impulse silver layer

Impulse's query engine expects five Delta tables defined in
`[impulse_query_engine/schema.py](../../impulse_query_engine/schema.py)`:


| Table               | Role                                                                                            |
| ------------------- | ----------------------------------------------------------------------------------------------- |
| `container_tags`    | Optional EAV tags per recording (`container_id`, `key`, `value`)                                |
| `container_metrics` | One row per recording (`container_id`, `start_dt`, `stop_dt`, …)                                |
| `channel_tags`      | Optional EAV tags per channel (`container_id`, `channel_id`, `key`, `value`)                    |
| `channel_metrics`   | Per-channel summary stats (`container_id`, `channel_id`, `min`, `max`, …)                       |
| `channels`          | Sample data — RLE `(container_id, channel_id, tstart, tend, value)` or raw `(timestamp, value)` |


Assign one `container_id` per `.mf4` file. Keep all time columns in **seconds
since epoch** as floats — the same unit the MDF reader returns with
`absolute_time=true` (`tstart`, `tend`, `begin_s`, `end_s`).

```python
from pyspark.sql import functions as F
import impulse_query_engine.schema as impulse_schema

CATALOG = "my_catalog"
SCHEMA = "silver"
MDF_PATH = "/Volumes/catalog/schema/mdf_data"
CONTAINER_ID = 1  # one recording; use a mapping table when ingesting many files

rle = (
    spark.read.format("mdf_signals")
    .option("path", MDF_PATH)
    .option("files", "run_001.mf4")
    .option("run_length_encoding", "true")
    .option("absolute_time", "true")
    .load()
    .withColumn("container_id", F.lit(CONTAINER_ID))
)

metadata = (
    spark.read.format("mdf_metadata")
    .option("path", MDF_PATH)
    .option("files", "run_001.mf4")
    .load()
    .withColumn("container_id", F.lit(CONTAINER_ID))
)

channels = rle.select(
    "container_id",
    F.col("channel_id").cast("int"),
    F.col("tstart").cast("double"),
    F.col("tend").cast("double"),
    F.col("value").cast("double"),
)

bounds = channels.agg(
    F.min("tstart").alias("start_s"),
    F.max("tend").alias("end_s"),
    F.countDistinct("channel_id").alias("num_channels"),
)
container_metrics = bounds.select(
    F.lit(CONTAINER_ID).alias("container_id"),
    F.to_timestamp("start_s").alias("start_dt"),
    F.to_timestamp("end_s").alias("stop_dt"),
    ((F.col("end_s") - F.col("start_s")) * 1000).cast("int").alias("duration_ms"),
    F.col("num_channels").cast("int"),
)

container_tags = spark.createDataFrame(
    [(CONTAINER_ID, "file_uri", f"{MDF_PATH}/run_001.mf4")],
    schema=impulse_schema.CONTAINER_TAGS,
)

channel_tags = metadata.select(
    "container_id",
    F.col("channel_id").cast("int"),
    F.lit("channel_name").alias("key"),
    F.col("channel_name").alias("value"),
)

channel_metrics = (
    channels.groupBy("container_id", "channel_id")
    .agg(
        F.count("*").cast("int").alias("sample_count"),
        F.min("value").cast("float").alias("min"),
        F.max("value").cast("float").alias("max"),
        F.avg("value").cast("float").alias("mean"),
        F.min("tstart").cast("float").alias("begin_s"),
        F.max("tend").cast("float").alias("end_s"),
    )
    .withColumn("value_type", F.lit("numerical"))
    .select(
        "container_id",
        F.col("channel_id").cast("int"),
        "value_type",
        "sample_count",
        F.lit(None).cast("float").alias("nan_ratio"),
        "begin_s",
        "end_s",
        F.lit(None).cast("int").alias("duration_ms"),
        F.lit(None).cast("int").alias("original_sample_count"),
        F.lit(None).cast("float").alias("original_sr"),
        "min",
        "max",
        "mean",
        F.lit(None).cast("float").alias("std"),
        F.lit(None).cast("float").alias("pz1"),
        F.lit(None).cast("float").alias("pz10"),
        F.lit(None).cast("float").alias("pz90"),
        F.lit(None).cast("float").alias("pz99"),
    )
)

for name, df in [
    ("container_tags", container_tags),
    ("container_metrics", container_metrics),
    ("channel_tags", channel_tags),
    ("channel_metrics", channel_metrics),
    ("channels", channels),
]:
    df.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.{name}")
```

Point a `MeasurementDB` at these tables (see the
[Impulse ingestion guide](../../../docs/impulse/docs/data_model/ingestion.md))
and run reports with `DefaultSolver`.

---

