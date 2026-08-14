---
sidebar_position: 1
title: MDF4 (ASAM)
---

# MDF4 (ASAM) data source

Read ASAM **MDF4** measurement files (`.mf4`) as Spark DataFrames, or convert them
straight to Delta Lake tables. The reader parses MDF4 binary blocks **directly** —
`asammdf` is *not* a runtime dependency — so decoding parallelises across Spark
workers, each reading only the bytes for its partition.

Every output row is identified by `file_uri`, the source file path.

The data source is provided by the `impulse_data_sources.mdf` package and exposes
three formats — [`mdf_signals`](#mdf_signals--time-series-samples),
[`mdf_metadata`](#mdf_metadata--channel-catalog), and
[`mdf_masters`](#mdf_masters--per-group-time-base) — plus a high-level
[`MDFToDeltaConverter`](#writing-delta-tables-with-mdftodeltaconverter).

:::warning Experimental

The MDF4 reader is **experimental** and does not yet implement the full ASAM MDF4
specification. Validate outputs against your files before relying on it in
production. See [Known limitations](#known-limitations) for the coverage gaps.

:::

## Install and register

Install the `impulse_data_sources` wheel on the cluster (the registered
data-source workers import the package), then register the three formats once per
session:

```python
from databricks.sdk import WorkspaceClient
from impulse_data_sources.mdf import register_mdf_datasources

register_mdf_datasources(spark, WorkspaceClient())
```

`register_mdf_datasources` is the recommended entry point: it verifies the workspace
client, tags API calls with `databricks-impulse` product info, and emits a
lightweight telemetry beacon each time Spark plans partitions for a read. If you
register the data-source classes manually instead, reads still work but no telemetry
is sent.

## Selecting files

`path` and `files` are shared by all three formats and control which `.mf4` files
are read:

| Behaviour | Example |
| --------- | ------- |
| Recursive scan (default) | `.option("path", "/Volumes/.../mdf").load()` |
| Relative paths under `path` | `.option("path", "/data").option("files", "batch_a/run.mf4,run_b/other.mf4")` |
| Absolute file URIs (no scan) | `.option("path", "/data").option("files", "/mnt/a.mf4,/mnt/b.mf4")` |

`path` is required and is used both for recursive discovery and as the base for
relative `files` entries. When `files` is set, only the listed paths are read (no
directory scan); each entry may be absolute or relative to `path`, and a mix of both
in one list is allowed.

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

| column | Spark type | nullable | notes |
| ------ | ---------- | -------- | ----- |
| `file_uri` | `string` | no | source `.mf4` path |
| `channel_id` | `int` | no | sequential signal id within the file |
| `time` | `double` or `float` | no | sample timestamp (relative seconds, or epoch seconds with `absolute_time`) |
| `value` | `double` or `float` | yes | decoded channel value |

`time` follows `time_dtype` (`float64` default, or `float32`); `value` follows
`value_dtype` independently. `absolute_time=true` forces the time columns to
`float64`.

With `run_length_encoding=true`, constant-value runs are collapsed into half-open
intervals and `time` is replaced by `tstart` / `tend`:

| column | Spark type | nullable | notes |
| ------ | ---------- | -------- | ----- |
| `tstart` | `double` or `float` | no | start of a constant-value interval (inclusive) |
| `tend` | `double` or `float` | no | end of the interval (exclusive), except the terminal point row per channel where `tstart = tend` |

Read a single file with smaller on-disk types:

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

### Options

`mdf_signals` and `mdf_masters` accept these options in addition to the shared
`path` / `files`:

| option | default | meaning |
| ------ | ------- | ------- |
| `target_partition_mb` | `64` | target output size per Spark task |
| `partitioning` | `group` | `group` (per-channel-group) or `stripe` (byte-offset; reads each file once to build a block map) |
| `stripe_target_mb` | `128` | compressed bytes per stripe (stripe mode) |
| `max_groups_per_partition` | `64` | cap on small groups coalesced into one task |
| `time_dtype` / `value_dtype` | `float64` | `float32` halves a column's on-disk size |
| `run_length_encoding` | `false` | collapse constant runs into `[tstart, tend)` intervals (+ a terminal point row per channel) |
| `absolute_time` | `false` | add the MDF start time so timestamps are UTC epoch seconds (forces the time columns to `float64`) |

## `mdf_metadata` — channel catalog

One row per signal channel: names, units, group indices, and comments. The schema
is fixed (unaffected by `time_dtype` / RLE options).

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

| column | Spark type | nullable | notes |
| ------ | ---------- | -------- | ----- |
| `file_uri` | `string` | no | source `.mf4` path |
| `channel_id` | `int` | no | sequential signal id within the file |
| `group_idx` | `int` | no | MDF channel-group index |
| `channel_idx` | `int` | no | channel index within the group |
| `channel_name` | `string` | no | CN block name |
| `unit` | `string` | yes | physical unit, if present |
| `header_datetime` | `timestamp` | yes | measurement start time from the HD block (UTC) |
| `md_comment` | `string` | yes | CN comment block (`##MD` XML or `##TX` text), if present |

Join signals to metadata on `(file_uri, channel_id)` for human-readable names:

```python
(
    signals.join(metadata, on=["file_uri", "channel_id"])
    .select("channel_name", "unit", "time", "value")
    .show(5)
)
```

## `mdf_masters` — per-group time base

One row per **original** master-channel sample: `(file_uri, group_idx, timestamp)`.
This is the time base of each acquisition group, used with run-length-encoded
signals to recover the full per-sample grid.

```python
masters = (
    spark.read.format("mdf_masters")
    .option("path", "/Volumes/catalog/schema/mdf_data")
    .load()
)

masters.select("file_uri", "group_idx", "timestamp").show(5)
```

| column | Spark type | nullable | notes |
| ------ | ---------- | -------- | ----- |
| `file_uri` | `string` | no | source `.mf4` path |
| `group_idx` | `int` | no | MDF channel-group index (matches `mdf_metadata.group_idx`) |
| `timestamp` | `double` or `float` | no | master time for one sample (relative seconds, or epoch seconds with `absolute_time`) |

`timestamp` follows `time_dtype`; `absolute_time=true` forces `float64`.
`mdf_masters` accepts the same options as `mdf_signals` (see
[above](#options)).

### Reversing run-length encoding

To recover per-sample rows from RLE signals, join the intervals against the master
time grid. Use the **same** `time_dtype` / `absolute_time` on both sources so the
timestamps line up exactly:

```python
from pyspark.sql import functions as F

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

## Writing Delta tables with `MDFToDeltaConverter`

For a one-call conversion to Delta — without wiring up the read/write yourself — use
`MDFToDeltaConverter`. It also works over **Databricks Connect** (no cluster install
needed; it uses the `mapInArrow` path with shipped artifacts).

```python
from impulse_data_sources.mdf import MDFToDeltaConverter

conv = MDFToDeltaConverter(
    spark,
    signals_table="cat.sch.signals",     # cluster on (file_uri, channel_id)
    metadata_table="cat.sch.metadata",   # cluster on file_uri
    target_partition_mb=64,
    time_dtype="float32",
    value_dtype="float32",
    run_length_encoding=False,
)

result = conv.convert("/Volumes/.../drive.mf4")          # one file
results = conv.convert_batch(["/Volumes/.../a.mf4", ...]) # many files, sequential
```

Each conversion returns a `ConversionResult` (`file_uri`, `num_channels`,
`total_samples`, `num_partitions`, `duration_seconds`, `signals_table`,
`metadata_table`).

## Writing to the Impulse silver layer

Impulse's query engine reads a silver layer of five Delta tables (see
[Data Model](../data_model/index.md) and the
[Ingestion guide](../data_model/ingestion.md)). To land MDF4 data there, assign one
`container_id` per `.mf4` file and keep all time columns in **seconds since epoch**
as floats — the unit the reader returns with `absolute_time=true`.

The example below reads one file as RLE signals plus metadata and derives the silver
tables:

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

Point a `MeasurementDB` at these tables and run reports with `DefaultSolver` — see
the [Ingestion guide](../data_model/ingestion.md).

## Low-level `MDF4Reader`

For advanced use, `MDF4Reader` exposes the block-level scan directly:

```python
from impulse_data_sources.mdf import MDF4Reader

r = MDF4Reader("/path/drive.mf4")   # or MDF4Reader(file_bytes=blob)
org = r.scan_channels_organized()   # masters / signals / channel_id_map
r.read_header_datetime()            # measurement start (UTC)
```

## Known limitations

The MDF4 reader is **experimental** and under active development. It does **not**
yet fully implement the
[ASAM MDF4](https://www.asam.net/standards/detail/mdf/) specification. Some block
types, encodings, compression modes, and edge cases may be missing or behave
differently than reference tools. Validate outputs against your files before relying
on this in production workflows.

- **MDF4 only.** Files must have an `MDF` identification block and an `##HD` header
  at offset 64. **MDF3** (and other legacy layouts) are not supported.
- **Numeric-first output.** Signal values are decoded to a single `double` / `float`
  column. String, byte-array, MIME, and complex channels are not represented in the
  output schema.

### Feature gaps (backlog)

| area | severity | status |
| ---- | -------- | ------ |
| **VLSD channels** (`cn_type = 1`) — variable-length signals store an offset in the fixed record; the `##SD` payload is not followed. Channels currently emit NaN. | MEDIUM | not implemented |
| **MLSD channels** (`cn_type = 5`) — maximum-length data lists are not decoded; bytes in the record are interpreted as fixed-width numeric data. | MEDIUM | not implemented |
| **CC type 3 (algebraic / formula)** — formula text is not read or evaluated. | LOW–MEDIUM | not implemented |
| **CC types 7–10 (text conversions)** — unsupported by design for the numeric-only `value` column. | LOW | not implemented (by design) |
| **CN composition** — the CN composition link is not resolved; composite / array channels are not expanded. | MEDIUM | not implemented |
| **CN virtual data** (`cn_type = 6`) — not synthesized from other channels; record bytes are decoded as if the channel were fixed-length. | MEDIUM | not implemented |
| **CN sync channels** (`cn_type = 4`) — included in `mdf_signals` like ordinary signals rather than used as a time/sync axis. | LOW | not implemented |
| **Source information (`##SI`)** — bus/protocol metadata is not surfaced. | LOW | not implemented |
| **Attachments** — `##AT` blocks are not loaded. | LOW | not implemented |
| **Events / global metadata** — `##EV`, `##FH`, `##CH`, and other non-DG block types are not parsed. | LOW | not implemented |

**Unsorted DGs:** reads filter interleaved records by `record_id` before decode.
Stripe mode concatenates sub-blocks, then filters once per channel group.

### Conversion (`##CC`) fields not applied

Only `cc_type` and the inline numeric parameters are used. `cc_precision`,
`cc_flags`, `cc_ref_count`, and `cc_phy_range_min` / `cc_phy_range_max` are read to
advance the file pointer but **not applied** to decoded values. CC reference links
(name, unit, comment, inverse CC, and the `cc_ref` `##TX` blocks used for formulas
and text tables) are skipped entirely; only inline numeric parameters are used for
conversion types 0–6, and the inverse-conversion link is not followed.

### Channel (`##CN`) fields not used

Only `cn_type`, `cn_data_type`, offsets, `cn_bit_count`, `cn_flags`, and
`cn_invalid_bit_pos` drive decoding. These are **not used** downstream:
`cn_sync_type`, `cn_precision`, `cn_attachment_count`, `cn_val_range_min` /
`cn_val_range_max`, `cn_limit_min` / `cn_limit_max`, and `cn_limit_ext_min` /
`cn_limit_ext_max`. The CG-level `cg_flags` and `cg_path_separator` fields are read
for layout only and not interpreted.

### Data types

Little- and big-endian integer and float types (types 0–5) are fully decoded for
common bit widths. All other `cn_data_type` values fall through to **zeros** (or NaN
for VLSD):

| `cn_data_type` | name | behaviour |
| -------------- | ---- | --------- |
| 6–9 | string (Latin / UTF-8 / UTF-16) | zeros emitted |
| 10 | byte array | zeros emitted |
| 11–12 | MIME sample / stream | zeros emitted |
| 13–14 | CANopen date / time | zeros emitted |
| 15–16 | complex LE / BE | zeros emitted |

Unsupported **float bit widths** (e.g. float16) within types 4–5 also produce zeros.

**Endianness / alignment:** fast strided decode paths are implemented for
little-endian, byte-aligned fields. Big-endian and unaligned (`bit_offset > 0`) types
use a slower generic path.

### Data blocks and I/O

Supported payload containers: `##DT`, `##DZ` (zlib deflate; `zip_type` 0 = plain,
1 = transposed deflate), and `##DL` / `##HL` chains.

| gap | notes |
| --- | ----- |
| **Unknown / future block types** at the DG data link | falls back to reading `record_size * sample_count` bytes with no structure validation |
| **Non-zlib `##DZ` compression** | only zlib (`zip_type` 0/1) is handled |
| **Malformed DL chains** | cyclic DL links stop traversal; truncated chains may yield partial data without error |

### Semantic / API limitations

| gap | notes |
| --- | ----- |
| **One master per group** | files with multiple masters per group are not modeled (the last master per group is kept) |
| **Fixed CN link indices** | name, CC, unit, and comment addresses assume the standard MDF4 link order; variant link counts / orderings may mis-resolve metadata |
| **Absolute time precision** | float64 epoch seconds (~0.3 µs resolution at current epoch); HD nanosecond start time is not preserved bit-for-bit |
| **Invalidation** | per-sample invalidation bits are applied when `CN_FLAG_INVALIDATION_PRESENT` is set; other CN/CG invalidation modes may differ from reference tools |

## See also

- [Ingestion guide](../data_model/ingestion.md) — landing data in the silver layer.
- [Data Model](../data_model/index.md) — the silver- and gold-layer schemas.
