# impulse_ds.mdf

Convert ASAM **MDF4** measurement files to **Delta Lake** tables with PySpark /
Databricks. The reader parses MDF4 binary blocks directly (no `asammdf` at
runtime), so conversion parallelises across Spark workers, each reading only the
bytes for its partition.

Every output row is identified by `file_uri` — the source file path.

**New to the data sources?** See [QUICKSTART.md](QUICKSTART.md) for minimal
examples of `mdf_signals`, `mdf_metadata`, and `mdf_masters`.

## Solution Accelerator

An end-to-end solution accelerator to convert mf4 files into the impulse schema will be available soon.

## Two ways to use it

### 1. Custom Spark data sources (read MDF4 as DataFrames)

See [QUICKSTART.md](QUICKSTART.md) for copy-paste examples of all three formats.

Requires the wheel installed on the cluster (the registered data-source workers
import the package).

```python
from databricks.sdk import WorkspaceClient
from impulse_ds.mdf import register_mdf_datasources

register_mdf_datasources(spark, WorkspaceClient())

signals = spark.read.format("mdf_signals").option("path", "/Volumes/.../mdf").load()
# discovers every *.mf4 under /Volumes/.../mdf, including subdirectories
```

**File selection** (`path` / `files` — shared by all three formats):


| behaviour                    | example                                                                       |
| ---------------------------- | ----------------------------------------------------------------------------- |
| Recursive scan (default)     | `.option("path", "/Volumes/.../mdf").load()`                                  |
| Relative paths under `path`  | `.option("path", "/data").option("files", "batch_a/run.mf4,run_b/other.mf4")` |
| Absolute file URIs (no scan) | `.option("path", "/data").option("files", "/mnt/a.mf4,/mnt/b.mf4")`           |


When `files` is set, only the listed paths are read. Each entry may be absolute or
relative to `path`; a mix of both in one list is allowed.

#### Output schemas



##### `mdf_signals`

One row per sample (default), or one row per constant-value interval when
`run_length_encoding=true`. `time` / `tstart` / `tend` follow `time_dtype`
(`float64` or `float32`); `absolute_time=true` forces time columns to `float64`.
`value` follows `value_dtype` independently.


| column       | Spark type          | nullable | notes                                                                      |
| ------------ | ------------------- | -------- | -------------------------------------------------------------------------- |
| `file_uri`   | `string`            | no       | source `.mf4` path                                                         |
| `channel_id` | `int`               | no       | sequential signal id within the file                                       |
| `time`       | `double` or `float` | no       | sample timestamp (relative seconds, or epoch seconds with `absolute_time`) |
| `value`      | `double` or `float` | yes      | decoded channel value                                                      |


With `run_length_encoding=true`, `time` is replaced by:


| column   | Spark type          | nullable | notes                                                                                |
| -------- | ------------------- | -------- | ------------------------------------------------------------------------------------ |
| `tstart` | `double` or `float` | no       | start of a constant-value interval (inclusive)                                       |
| `tend`   | `double` or `float` | no       | end of the interval (exclusive), except the terminal point row where `tstart = tend` |




##### `mdf_metadata`

One row per signal channel. Schema is fixed (not affected by `time_dtype` / RLE options).


| column            | Spark type  | nullable | notes                                                    |
| ----------------- | ----------- | -------- | -------------------------------------------------------- |
| `file_uri`        | `string`    | no       | source `.mf4` path                                       |
| `channel_id`      | `int`       | no       | sequential signal id within the file                     |
| `group_idx`       | `int`       | no       | MDF channel-group index                                  |
| `channel_idx`     | `int`       | no       | channel index within the group                           |
| `channel_name`    | `string`    | no       | CN block name                                            |
| `unit`            | `string`    | yes      | physical unit, if present                                |
| `header_datetime` | `timestamp` | yes      | measurement start time from the HD block (UTC)           |
| `md_comment`      | `string`    | yes      | CN comment block (`##MD` XML or `##TX` text), if present |




##### `mdf_masters`

One row per **original** master-channel sample — the time base of each acquisition
group. Used with RLE-encoded `mdf_signals` to recover the full per-sample grid.
`timestamp` follows `time_dtype` (`float64` or `float32`); `absolute_time=true`
forces `float64`.


| column      | Spark type          | nullable | notes                                                                                |
| ----------- | ------------------- | -------- | ------------------------------------------------------------------------------------ |
| `file_uri`  | `string`            | no       | source `.mf4` path                                                                   |
| `group_idx` | `int`               | no       | MDF channel-group index (matches `mdf_metadata.group_idx`)                           |
| `timestamp` | `double` or `float` | no       | master time for one sample (relative seconds, or epoch seconds with `absolute_time`) |


**Shared options** (`mdf_signals`, `mdf_metadata`, `mdf_masters`):


| option  | default                  | meaning                                                                                                                                      |
| ------- | ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`  | — (required)             | root directory; used for recursive discovery and as the base for relative `files` entries                                                    |
| `files` | all `*.mf4` under `path` | comma-separated file list; each entry may be an absolute path or relative to `path`. When set, only these files are read (no directory scan) |


`mdf_signals` **/** `mdf_masters` **options** (in addition to the shared options above):


| option                       | default   | meaning                                                                                           |
| ---------------------------- | --------- | ------------------------------------------------------------------------------------------------- |
| `target_partition_mb`        | 64        | target output size per Spark task                                                                 |
| `partitioning`               | `group`   | `group` (per-channel-group) or `stripe` (byte-offset; reads each file once to build a block map)  |
| `stripe_target_mb`           | 128       | compressed bytes per stripe (stripe mode)                                                         |
| `max_groups_per_partition`   | 64        | cap on small groups coalesced into one task                                                       |
| `time_dtype` / `value_dtype` | `float64` | `float32` halves a column's on-disk size                                                          |
| `run_length_encoding`        | `false`   | collapse constant runs into `[tstart, tend)` intervals (+ a terminal point row per channel)       |
| `absolute_time`              | `false`   | add the MDF start time so timestamps are UTC epoch seconds (forces the time columns to `float64`) |


> Reverse RLE: join RLE intervals against `mdf_masters` timestamps —
> `t >= tstart AND (t < tend OR (tstart = tend AND t = tstart))` — using the same
> `time_dtype`/`absolute_time` on both sources.



#### Telemetry

Call `register_mdf_datasources(spark, ws)` instead of registering the three data
source classes manually. It verifies the workspace client, tags API calls with
`databricks-impulse` product info, and emits a lightweight telemetry beacon
(`mdf` → `mdf_signals` / `mdf_metadata` / `mdf_masters`) each time Spark plans
partitions for a read. If the data sources are registered without
`register_mdf_datasources`, reads still work but no telemetry is sent.

### 2. The high-level converter (writes the Delta tables)

Also works over Databricks Connect (no cluster install needed — uses the
`mapInArrow` path with shipped artifacts).

```python
from impulse_ds.mdf import MDFToDeltaConverter
conv = MDFToDeltaConverter(
    spark,
    signals_table="cat.sch.signals",     # CLUSTER BY (file_uri, channel_id)
    metadata_table="cat.sch.metadata",   # CLUSTER BY file_uri
    target_partition_mb=64,
    time_dtype="float32", value_dtype="float32",
    run_length_encoding=False,
)
conv.convert("/Volumes/.../drive.mf4")          # one file
conv.convert_batch(["/Volumes/.../a.mf4", ...]) # many files, sequential
```



### Low-level reader

```python
from impulse_ds.mdf import MDF4Reader
r = MDF4Reader("/path/drive.mf4")            # or MDF4Reader(file_bytes=blob)
org = r.scan_channels_organized()            # masters / signals / channel_id_map
r.read_header_datetime()                     # measurement start (UTC)
```



## Module layout

Package path: `src/impulse_ds/mdf/`


| module           | responsibility                                                                            |
| ---------------- | ----------------------------------------------------------------------------------------- |
| `mdf4_reader.py` | parse MDF4 structure (HD/DG/CG/CN) → `ChannelInfo`; header datetime                       |
| `mdf_blocks.py`  | low-level data-block I/O (`##DT`/`##DZ`/`##DL`/`##HL`, sub-blocks)                        |
| `mdf_decode.py`  | raw-bytes → values/timestamps (data types, CC conversion, invalidation)                   |
| `arrow_emit.py`  | build Arrow batches (per-group, stripe, master) + run-length encoding                     |
| `udf_helpers.py` | re-export shim over the three modules above (stable import surface)                       |
| `bin_packer.py`  | partition planning (`plan_partitions`, `plan_stripes_for_file`, `plan_master_partitions`) |
| `converter.py`   | `MDFToDeltaConverter` orchestration + Delta writes                                        |
| `datasources.py` | the three Spark data sources                                                              |
| `schemas.py`     | shared Spark schemas (`SIGNALS_SCHEMA`, `METADATA_SCHEMA`)                                |




## Limitations (backlog)

Known gaps in the numeric-first reader/converter. **Sorted** data groups (`rec_id_size = 0`)
and CC types 0–2, 4–6 work as expected. **Unsorted** data groups (`rec_id_size > 0`)
with interleaved channel groups are supported via per-CG record filtering on read.


| area                                                                                                                                                                                             | severity   | status                      |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------- | --------------------------- |
| **VLSD channels** — variable-length signals store an offset in the fixed record; payload in SDBLOCK is not followed. Channels currently emit NaN. String/byte output would need schema changes.  | MEDIUM     | not implemented             |
| **CC type 3 (algebraic / formula)** — formula text in `cc_ref[0]` (TXBLOCK) is not read or evaluated; `apply_cc_conversion` has no handler for type 3.                                           | LOW–MEDIUM | not implemented             |
| **CC types 7–10 (text conversions)** — require TXBLOCK / `cc_ref` resolution. Unsupported by design for the numeric-only `value` column; `_parse_cc_block` returns `(-1, ())` for `cc_type > 6`. | LOW        | not implemented (by design) |


**CC types 7–10:** types 7, 8, 10 produce text (need a string extraction path / separate table); type 9 (text→value) could stay numeric but needs reference-text matching.

**Unsorted DGs:** reads filter interleaved records by `record_id` before decode (`filter_unsorted_records` in `mdf_decode.py`). Stripe mode concatenates sub-blocks, then filters once per channel group.

## Acknowledgments

The MDF data sources and low-level reader were implemented with reference to
[asammdf](https://github.com/danielhrisca/asammdf) by [Daniel Hrisca](https://github.com/danielhrisca).
`asammdf` is not a runtime dependency of this package; it is listed under test
dependencies only (see below) for synthesising small MDF4 fixtures.

## Dependencies

- Runtime: `numpy`, `pyarrow` (`pyspark` is provided by the Databricks runtime).
- Tests: `pytest`, `asammdf` (dev/test dependency only — synthesises small MDF4 files on the fly).

