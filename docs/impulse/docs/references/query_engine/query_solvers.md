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
- [Solver column mappings and filters](../../config/configuration.md#solver-column-mappings-and-filters)
  — adapt the solver to a silver layer whose physical column names
  diverge from Impulse's internal names, scope reads by `project_id`, or
  apply per-table equality filters.

If `query_engine` is omitted from your config entirely, the engine runs
with `DefaultSolver` and `data_type = "RLE"`.

## API reference

Auto-generated symbol-level docs:

- [`DefaultSolver`](../api/impulse_query_engine/analyze/query/solvers/default_solver.md)
- [`QuerySolver`](../api/impulse_query_engine/analyze/query/solvers/query_solver.md)
  — abstract base class defining the six-stage solver pipeline.
- [`SolverConfig`](../api/impulse_query_engine/analyze/query/solvers/solver_config.md)
  — per-table column mappings, filters, and project scoping.
