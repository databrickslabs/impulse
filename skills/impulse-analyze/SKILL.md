---
name: impulse-analyze
description: >
  Run ad-hoc Impulse analysis in a notebook — evaluate TSAL expressions directly through the query
  engine and get a Spark or pandas DataFrame back, with no gold-layer write and no reporting setup. Use
  when the user wants to "explore signals interactively", "get a DataFrame from Impulse", compute a
  quick per-container mean/histogram, or prototype expressions before wiring them into a report. Covers
  building a query on a MeasurementDB, `select()`, `solve()` / `toPandas()`, and choosing the solver.
---

# Impulse — ad-hoc analysis

Ad-hoc mode evaluates TSAL directly through the query engine and returns a DataFrame per your selection
— one row per container, one column per selected expression. Nothing is written to the gold layer.

You need a `MeasurementDB` (which exposes `.query`, a `QueryBuilder`) and a solver. There are two ways
to get the `MeasurementDB`.

## Option A — construct MeasurementDB directly

Best when you only want to explore and don't need a report.

```python
from databricks.sdk import WorkspaceClient
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from impulse_query_engine.analyze.query.solvers import DefaultSolver

ws = WorkspaceClient()

cfg = MeasurementDBConfig(
    container_metrics_table="my_catalog.silver.container_metrics",
    channel_metrics_table="my_catalog.silver.channel_metrics",
    channels_uri="my_catalog.silver.channels",
    channel_tags_table="my_catalog.silver.channel_tags",       # optional
    container_tags_table="my_catalog.silver.container_tags",   # optional
    table_locations="unity_catalog",
)
db = MeasurementDB(cfg, ws)
```

If all your silver tables live under one catalog/schema and are literally named `container_metrics`,
`channel_metrics`, `channels`, etc., use the shortcut:

```python
cfg = MeasurementDBConfig.for_unity_catalog(catalog_name="my_catalog", core_schema_name="silver")
db = MeasurementDB(cfg, ws)
```

## Option B — reuse a sinkless Report

Best when you already have a report config, or want config-level features (container filters, solver
column mappings). Omit `unity_sink` so nothing is written (see `impulse-config`):

```python
from impulse_reporting.core.report import Report

report = Report(name="scratch", spark=spark, workspace_client=ws, config=config_without_sink)
db = report.get_db()
solver = report.get_solver()     # already configured from your query_engine settings
```

## Build and solve a query

Select channels and derive signals with TSAL (see `impulse-tsal`), then `select()` the expressions you
want as columns and `solve()`:

```python
eng_rpm = db.query.channel(channel_name="Engine RPM")
veh_spd = db.query.channel(channel_name="Vehicle Speed Sensor")

result = (
    db.query
      .select(
          eng_rpm.mean().alias("rpm_mean"),
          eng_rpm.max().alias("rpm_max"),
          veh_spd.max().alias("speed_max"),
      )
      .solve(spark, solver=DefaultSolver(spark))
)
result.show()
```

`.alias(name)` sets the output column name. Use `.toPandas(spark, solver=...)` instead of `.solve(...)`
to collect a pandas DataFrame directly:

```python
pdf = db.query.select(eng_rpm.mean().alias("rpm_mean")).toPandas(spark, solver=DefaultSolver(spark))
```

## Selecting Points-in-Time (POI) channels

A POI channel carries values defined *only at* their timestamp (no interval). Select one with
`poi_channel(...)` instead of `channel(...)` — identification (tags / `channel_metrics` columns) is
identical; only the built series type differs:

```python
# numeric POI channel (default dtype="double")
dtc_count = db.query.poi_channel(channel_name="DTC_count")
# string POI channel (e.g. DTC fault codes)
dtc = db.query.poi_channel(channel_name="DTC", dtype="string")
```

- `dtype` is `"double"` (default, numeric) or `"string"`, and accepts either the `SeriesValueType` enum
  or the plain string.
- **String** POI series support only equality (`== "P0301"` / `!=`) and sampling (`.where(...)`);
  arithmetic, ordering and numeric reductions (`sum`/`mean`/`min`/`max`) are rejected at build time.
- The declared `dtype` is validated against the silver data at solve time (a declared-vs-actual mismatch
  raises). See `impulse-data-model` for the `poi_channels` table and `impulse-tsal` for the algebra.

## Choosing the solver

`DefaultSolver(spark)` reads your silver layer. Constructor:

```python
DefaultSolver(spark, config=None, is_raw_data=False, drop_implausible_data=False, raw_encoder=RawEncoder.RLE)
```

- `is_raw_data=True` when `channels` stores raw `(timestamp, value)` samples (default `False` expects
  RLE `[tstart, tend)` intervals). This is the ad-hoc equivalent of `query_engine.data_type` in a report
  config.
- `raw_encoder` (`RawEncoder.RLE` default, or `RawEncoder.INTERVAL`) chooses how raw points become
  intervals — RLE collapses equal-valued runs, INTERVAL keeps every sample. Only used when
  `is_raw_data=True`; the ad-hoc equivalent of `query_engine.raw_encoder`. Import from
  `impulse_query_engine.analyze.query.solvers.solver_config`.
- `drop_implausible_data=True` drops rows where `is_plausible = false` (requires `is_raw_data=True`).
- `config` takes a `SolverConfig` for column-name remapping / project scoping — the same object
  described under `solver_config` in `impulse-config`.

With **Option B**, pass `report.get_solver()` instead of constructing one, so the solver honors your
report config.

## What comes back

The result DataFrame is `[container_id, <your aliases…>]`: **`container_id` is always the first column**
(one row per container), followed by one column per selected expression, named by its `.alias(...)`. So
you can join results back to container metadata or labels on `container_id` directly.

Each selected expression is typed by what it evaluates to (see `impulse-tsal` for the result types):

| Result type          | Spark column type                  | In `toPandas()`                     |
|----------------------|------------------------------------|-------------------------------------|
| `SampleSeries`       | `BinaryType` (pickle+lz4)          | deserialized back into an object    |
| `Intervals`          | `ArrayType(ArrayType(DoubleType))` | nested lists `[[tstart, tend], ...]` |
| `PointsInTime`       | `ArrayType(DoubleType)`            | list `[tstart, ...]`                |
| `PointsInTimeSeries` (numeric) | `ArrayType(ArrayType(DoubleType))` | nested lists `[[tstart, value], ...]` |
| `PointsInTimeSeries` (string)  | `ArrayType(StructType[tstart:double, value:string])` | list of `(tstart, value)` structs |
| scalar               | `DoubleType`                       | the value                           |

For scalar-per-container summaries (means, maxima, counts), `select()` the reducer expressions as
above. When you want the results persisted as a star schema instead of returned inline, use
`impulse-reporting`.

## Solving calculated channels

`solve()` returns one **wide** row per container. To get a **narrow**, silver-shaped result instead —
one row per sample interval — use `solve_calculated_channels()` with `CalculatedChannel` selections (see
`impulse-channels`). It requires a `DefaultSolver` (the default `BlobSolver` raises).

```python
from impulse_query_engine.analyze.query.channels.calculated_channel import CalculatedChannel

cc = CalculatedChannel(
    db.query.channel(channel_name="Vehicle Speed Sensor") * 3.6,
    {"channel_name": "speed_kmh", "data_key": "CALC"},
)
df = db.query.select(cc).solve_calculated_channels(spark, solver=DefaultSolver(spark))
# df: [container_id, channel_id, tstart, tend, value, identity]
#     identity is a map<string,string> holding the channel's identity dict
```

All selections must be `CalculatedChannel`s; identity keys are arbitrary and need not match across
selections (each row carries its own identity map).
