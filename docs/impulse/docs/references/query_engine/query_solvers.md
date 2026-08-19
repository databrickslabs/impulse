---
sidebar_position: 2
title: Query Solvers
---

# Query Solvers

The query engine resolves channel selections and evaluates events and
aggregations against silver-layer data. It does this through a *solver*:
the component that knows how your silver tables are physically laid out
and how to read them. Impulse ships a single solver — `DefaultSolver` —
that adapts to the silver tables you have.

## One solver, three required tables

`DefaultSolver` runs on **just three silver tables**:

- `container_metrics` — one row per container (recording).
- `channel_metrics` — one row per `(container_id, channel_id)` channel.
- `channels` — the time-series sample data (RLE or raw).

**Everything else is optional.** In particular, the **tag tables
(`container_tags`, `channel_tags`) are not required** — supply them only
when you want EAV-style tag filtering/selection. The `channel_mapping`
and `unit_conversion` tables are likewise optional add-ons that unlock
channel aliasing and unit conversion when present.

## How `DefaultSolver` adapts

The solver chooses its behaviour per query from the tables you configure:

- **Channel selection.** With a `channel_tags` table configured, channels
  are selected from its narrow EAV `(key, value)` rows (pivoted on the
  fly). Without one, channels are selected directly from columns on
  `channel_metrics` — so an attribute such as `channel_name` lives as a
  column on `channel_metrics`. The presence of
  `source.channel_tags_table` selects the mode.
- **Container filtering.** With a `container_tags` table configured,
  containers are filtered from its narrow EAV rows. Without one, container
  attributes are read as columns on `container_metrics` (the wide-only
  model).
- **Channel aliasing.** When a `channel_mapping` table is configured,
  logical channel names (`channel_with_alias(...)`) resolve to physical
  channels, with optional per-alias **unit conversion** when a
  `unit_conversion` table is also configured.

Tag tables, when present, are always read in the narrow EAV layout
`(key, value)` and pivoted on the fly.

## Channel aliasing requires channel-identifying columns on `channel_metrics`

When you use aliasing (`channel_with_alias(...)` backed by a
`channel_mapping` table), `DefaultSolver` resolves each logical alias to a
physical channel by **joining `channel_mapping` to `channel_metrics`**. That
join needs columns on `channel_metrics` that identify a channel within a
container — by default **`channel_name` and `data_key`** (the
`channel_metrics` side of the alias-resolution join keys).

So when aliasing is in use, `channel_metrics` **must carry those
identifying columns**. This holds *regardless* of whether a `channel_tags`
table is configured: even if direct channel selection runs against the EAV
`channel_tags` table, alias resolution always joins `channel_mapping`
against `channel_metrics`.

You can change which columns are used via `channel_mapping.join_keys` (for
example, a single-column join when `data_key` is not part of the channel
identity in your layout) — see
[Alias-resolution join keys](../../config/configuration.md#alias-resolution-join-keys-optional).

## Container-level metadata in expressions

Expressions can pull **container-level metadata** into their evaluation. When a selected
expression — or a UDF wrapped inside an aggregation, event, or calculated channel — declares
container tags or metrics via `container_tags=` / `container_metrics=` (see
[Defining Expressions](tsal/defining_expressions.md#reading-container-level-metadata-inside-a-udf)),
`DefaultSolver` gathers the union across all selections, reads just those values from the silver
layer — **metrics** from `container_metrics` columns, **tags** from the EAV `container_tags` table
(pivoted) — and broadcast-joins them onto each container's evaluation frame so the UDF receives them.
The values are constant per container, and the same wiring applies to
[`solve_calculated_channels`](#calculated-channels).

Requesting a nonexistent `container_metrics` column, or any `container_tags` without a
`container_tags_table` configured, raises a `ValueError`.

## Table requirements

| Silver table        | Required?  | Notes                                                                                  |
|---------------------|------------|----------------------------------------------------------------------------------------|
| `container_metrics` | **yes**    | One row per container.                                                                  |
| `channel_metrics`   | **yes**    | One row per channel. Carries channel-selection columns in the wide model, and the channel-identifying columns (`channel_name`, `data_key`) required for aliasing. |
| `channels`          | **yes**    | Time-series sample data (RLE or raw).                                                  |
| `container_tags`    | optional   | Narrow EAV. Omit for the wide-only container model.                                    |
| `channel_tags`      | optional   | Narrow EAV. Omit when channel-selection attributes live on `channel_metrics`.          |
| `channel_mapping`   | optional   | Channel aliases. Requires channel-identifying columns on `channel_metrics` (see above).|
| `unit_conversion`   | optional   | Per-alias unit conversion (used together with `channel_mapping`).                      |

See the [Silver Layer Schema](../../data_model/silver_layer_schema.md) for
the columns each table is expected to carry.

## Configuring the solver

Solver tuning lives under the `query_engine` section of your report config:

- [`query_engine.solver`](../../config/configuration.md#query_engine-optional)
  — the solver to use. Defaults to `"DefaultSolver"`. The values
  `"DeltaSolver"` and `"KeyValueStoreSolver"` are **deprecated aliases**
  retained for backward compatibility; both resolve to `DefaultSolver`.
- [`query_engine.data_type` and `raw_encoder`](../../config/configuration.md#query_engine-optional)
  — declare whether `channels` holds pre-encoded `[tstart, tend)` intervals
  (`"RLE"`, the default) or raw point samples (`"RAW"`). For raw data,
  `raw_encoder` selects how samples are converted to intervals at query
  time: `"RLE"` (default) run-length encodes equal-valued runs into single
  intervals; `"INTERVAL"` only derives `tend` and drops exact duplicates.
  In both modes `tend` is derived from the **next sample's timestamp**
  (the last sample falls back to its own timestamp) — see
  [How Impulse interprets intervals](../../data_model/silver_layer_schema.md#raw-format)
  for validity semantics and the definition of a duplicate point.
- [Solver column mappings and filters](../../config/configuration.md#solver-column-mappings-and-filters)
  — adapt the solver to a silver layer whose physical column names
  diverge from Impulse's internal names, scope reads by `project_id`, or
  apply per-table equality filters.

If `query_engine` is omitted from your config entirely, the engine runs
with `DefaultSolver` and `data_type = "RLE"`.

## Calculated channels

The standard `solve()` produces a **wide** result — one row per container, one column per selection.
`DefaultSolver.solve_calculated_channels` is a parallel entry point that produces a **narrow**,
silver-shaped result instead: it explodes each selection's `SampleSeries` into one row per sample
interval, emitting `container_id, channel_id, tstart, tend, value` plus a single self-describing
`identity` `map` column.

Every selection must be a [`CalculatedChannel`](../report/channel.md) (identity keys are arbitrary and
need not match across selections), and each wrapped expression must evaluate to a `SampleSeries`. The stage reuses the same
metadata filter pipeline as `solve()` to resolve the input channels, then runs a grouped-map UDF per
container. Only `DefaultSolver` implements it — the base `QuerySolver` raises `NotImplementedError`.

```python
from impulse_query_engine.analyze.query.channels.calculated_channel import CalculatedChannel
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver

cc = CalculatedChannel(
    db.query.channel(channel_name="Vehicle Speed Sensor") * 3.6,
    {"channel_name": "speed_kmh", "data_key": "CALC"},
)
df = db.query.select(cc).solve_calculated_channels(spark, solver=DefaultSolver(spark))
# df: [container_id, channel_id, tstart, tend, value, channel_name, data_key]
```

The reporting layer builds on this stage to persist derived channels to the gold layer — see
[Channels](../report/channel.md).

## API reference

Auto-generated symbol-level docs:

- [`DefaultSolver`](../api/impulse_query_engine/analyze/query/solvers/default_solver.md)
- [`QuerySolver`](../api/impulse_query_engine/analyze/query/solvers/query_solver.md)
  — abstract base class defining the six-stage solver pipeline.
- [`SolverConfig`](../api/impulse_query_engine/analyze/query/solvers/solver_config.md)
  — per-table column mappings, filters, and project scoping.
