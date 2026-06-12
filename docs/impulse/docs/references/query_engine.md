---
sidebar_position: 2
title: Query Engine
---

# Query Engine

The query engine resolves channel selections and evaluates events and
aggregations against silver-layer data. It does this through a *solver*:
the component that knows how your silver tables are physically laid out
and how to read them. Impulse ships a single solver — `DefaultSolver` —
that adapts to the silver tables you have.

## The solver

`DefaultSolver` adapts its behaviour to the shape of your silver layer,
table by table:

- **Channel selection** runs against a narrow EAV `channel_tags` table
  when one is configured (pivoting its `(key, value)` rows on the fly), and
  otherwise directly against columns of `channel_metrics` (so an attribute
  such as `channel_name` lives as a column on `channel_metrics`). The
  presence of `source.channel_tags_table` selects the mode per query.
- **Container filtering** uses a narrow EAV `container_tags` table when one
  is configured, and otherwise treats container attributes as columns on
  `container_metrics` (the wide-only model).
- **Channel aliasing** (logical names that map to physical channels) is
  available when a `channel_mapping` table is configured, with optional
  per-alias **unit conversion** when a `unit_conversion` table is also
  configured.

Tag tables (`container_tags`, `channel_tags`) are always read in the narrow
EAV layout `(key, value)` and pivoted on the fly.

## Table requirements

| Silver table        | Required? | Notes                                                        |
|---------------------|-----------|--------------------------------------------------------------|
| `container_metrics` | required  |                                                              |
| `channel_metrics`   | required  | also carries channel-selection columns in the wide model     |
| `channels`          | required  | the time-series data (RLE or raw)                            |
| `container_tags`    | optional  | narrow EAV; omit for the wide-only container model           |
| `channel_tags`      | optional  | narrow EAV; omit when selection attributes are on `channel_metrics` |
| `channel_mapping`   | optional  | channel aliases                                              |
| `unit_conversion`   | optional  | per-alias unit conversion                                    |

See the [Silver Layer Schema](../data_model/silver_layer_schema.md) for
the columns each table is expected to carry.

## Configuring the solver

Solver tuning lives under the `query_engine` section of your report config:

- [`query_engine.solver`](../config/configuration.md#query_engine-optional)
  — the solver to use. Defaults to `"DefaultSolver"`. The values
  `"DeltaSolver"` and `"KeyValueStoreSolver"` are **deprecated aliases**
  retained for backward compatibility; both resolve to `DefaultSolver`.
- [Solver column mappings and filters](../config/configuration.md#solver-column-mappings-and-filters)
  — adapt the solver to a silver layer whose physical column names
  diverge from Impulse's internal names, scope reads by `project_id`, or
  apply per-table equality filters.

If `query_engine` is omitted from your config entirely, the engine runs
with `DefaultSolver` and `data_type = "RLE"`.

## API reference

Auto-generated symbol-level docs:

- [`DefaultSolver`](api/impulse_query_engine/analyze/query/solvers/default_solver.md)
- [`QuerySolver`](api/impulse_query_engine/analyze/query/solvers/query_solver.md)
  — abstract base class defining the six-stage solver pipeline.
- [`SolverConfig`](api/impulse_query_engine/analyze/query/solvers/solver_config.md)
  — per-table column mappings, filters, and project scoping.
