---
name: impulse-channels
description: >
  Compute calculated (derived) channels in Impulse — new time-series signals built from existing
  channels and materialized at the same per-sample grain. Use when the user wants to "add a calculated
  channel", derive/persist a signal (e.g. "speed in km/h", "power = rpm × torque"), materialize a virtual
  signal into a queryable table, or run `solve_calculated_channels`. Covers the reporting-layer
  CalculatedChannel, the ad-hoc `QueryBuilder.solve_calculated_channels` endpoint, and the
  calculated_channel_fact/dimension gold output.
---

# Impulse — calculated channels

A calculated channel is a **derived signal** computed from existing channels (see `impulse-tsal`) and
persisted at the same per-sample grain as the silver `channels` table — unlike an aggregation (see
`impulse-aggregations`), which summarizes a signal into bins or statistics. It is the *persisted, labeled*
form of a virtual signal.

**Every calculated channel must be registered with the report before computing:**

```python
report.add_calculated_channel(my_channel)
```

The wrapped TSAL expression **must evaluate to a `SampleSeries`** (a signal), not `Intervals` — validated
at construction (raises `ValueError`).

## CalculatedChannel

Couples a TSAL expression with an `identity` — the identifier columns emitted on every output row. The
expression is evaluated per container and exploded into narrow rows (one per sample interval).

```python
from impulse_reporting.channels.calculated_channel import CalculatedChannel

veh_spd = report.get_db().query.channel(channel_name="Vehicle Speed Sensor")

speed_kmh = CalculatedChannel(
    name="speed_kmh",
    expr=veh_spd * 3.6,
    identity={"channel_name": "speed_kmh", "data_key": "CALC"},
    desc="Vehicle speed converted to km/h",
)
report.add_calculated_channel(speed_kmh)
```

| Parameter    | Type                   | Required | Description                                                                                       |
|--------------|------------------------|----------|---------------------------------------------------------------------------------------------------|
| `name`       | `str`                  | Yes      | Human-readable channel name; stored in `calculated_channel_dimension`.                            |
| `expr`       | `TimeSeriesExpression` | Yes      | Derived signal. **Must evaluate to `SampleSeries`.**                                              |
| `identity`   | `dict[str, str]`       | Yes      | Output identifier columns. **Must contain exactly `{"channel_name", "data_key"}`.** Seeds `channel_id`. |
| `desc`       | `str`                  | No       | Description stored in `calculated_channel_dimension`.                                             |
| `attributes` | `Mapping[str, str]`    | No       | Free-form metadata; values coerced to strings.                                                    |

Multi-channel expressions (e.g. `eng_rpm * torque`) are time-base synchronized automatically; emitted
intervals are the intersection. The `channel_id` is a deterministic hash of the sorted `identity` — the
same value in both the fact and dimension tables, so they join on it.

## Ad-hoc: solve_calculated_channels

To compute channels without a report (see `impulse-analyze` for the ad-hoc query pattern), use the query
engine's narrow-solve endpoint directly. It returns a Spark DataFrame; it requires a `DefaultSolver` (the
default `BlobSolver` raises `NotImplementedError`).

```python
from impulse_query_engine.analyze.query.channels.calculated_channel import CalculatedChannel
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver

cc = CalculatedChannel(
    db.query.channel(channel_name="Vehicle Speed Sensor") * 3.6,
    {"channel_name": "speed_kmh", "data_key": "CALC"},
    # channel_id defaults to a deterministic hash; pass an int to override, or None to emit null.
)
df = db.query.select(cc).solve_calculated_channels(spark, solver=DefaultSolver(spark))
df.show()  # [container_id, channel_id, tstart, tend, value, channel_name, data_key]
```

All selections in one call must share the same identity key set (validated up front).

## Output schema

**calculated_channel_dimension** (one row per channel per report) — key columns: `channel_id`,
`report_id`, `channel_type` (`"CALCULATED_CHANNEL"`), `channel_name`, `channel_description`,
`channel_expression` (TSAL string), `identity` (map), `definition_hash`, `attributes` (map).

**calculated_channel_fact** (one row per sample interval per container) — `container_id`, `channel_id`,
`tstart`, `tend`, `value`, `channel_name`, `data_key`. Same narrow, run-length-encoded shape as the silver
`channels` table. Join to the dimension on `channel_id`, and to `measurement_dimension` on `container_id`
(see `impulse-data-model`).

## Incremental

Calculated channels reuse the report's incremental engine (see `impulse-reporting`). A definition change —
the `expr` string or the `identity` — reprocesses that channel across all containers (`replaceWhere` on
`channel_id`); unchanged definitions only recompute new/updated containers (`MERGE`). Renaming or editing
`desc`/`attributes` does not change the hash.
