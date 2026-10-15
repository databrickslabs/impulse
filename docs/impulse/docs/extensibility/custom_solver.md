---
sidebar_position: 2
title: Custom solver
---

# Custom solver

The **solver** turns a query into results. It runs a fixed pipeline over the
[silver model](../data_model/silver_layer_schema.md): filter containers by tags and metrics, find the
candidate channels for each selection, pick the best candidate, and evaluate the TSAL expressions
(`solve`). Impulse ships `DefaultSolver`, which adapts to narrow or wide silver layouts through
[`solver_config`](../config/configuration.md#solver-column-mappings-and-filters). When you need the
engine to *behave* differently, you can register your own solver and select it by name in the report
config.

With nothing configured, `DefaultSolver` is used and behaviour is unchanged.

## Custom solver or custom MeasurementDB?

Impulse has two extension points with different jobs. The **MeasurementDB** decides *where and how*
the silver tables are read; the **solver** decides *what the engine does* with them.

| Your problem | Use |
|---|---|
| Column names differ, or narrow-EAV vs. wide layout | `DefaultSolver` + `solver_config`, no code |
| Your tables have a different shape: other tables, unions, joins, grain changes, EAV unpivots, or row filters that must run before a reshape | A [custom MeasurementDB](custom_measurement_db.md), or producing the silver shape upstream (ETL job or views) |
| The engine should do something different with silver-shaped data: custom candidate selection, extra pruning, post-processing the solve output, a different time-series encoding or storage | **A custom solver** (this page) |

Rule of thumb: if the problem is **how the data looks**, change how it's read; if it's **what the
engine does with silver data**, change the solver.

Prefer not to reshape data inside a solver. The solver isn't the only reader of the silver tables:
incremental reporting detects new and updated containers by running only the solver's container
stages (`filter_container_tags`, `filter_container_metrics`), outside `solve()`, and anything that
queries the MeasurementDB directly doesn't go through the solver at all. A reshape in a solver has to
be repeated in every stage that reads a table, and the other readers still see the original tables.
Reshape in a [custom MeasurementDB](custom_measurement_db.md) instead: every reader goes through it,
so they all see the same data.

## Implement one

A custom solver is a subclass of `DefaultSolver`, plus an optional config class for its own
settings, registered under a name:

```python
import pyspark.sql.functions as F

from impulse_query_engine.analyze.query.solvers import DefaultSolver, SolverConfig, register_solver


class ExcludeContainersConfig(SolverConfig):
    excluded_container_ids: list[int]  # required: no default


@register_solver("ExcludeContainersSolver", ExcludeContainersConfig)
class ExcludeContainersSolver(DefaultSolver):
    def solve(self, query, channels_df, selections, dtypes):
        solved = super().solve(query, channels_df, selections, dtypes)
        return solved.filter(
            ~F.col(self.config.container_id_col).isin(self.config.excluded_container_ids)
        )
```

- **Override only the stages you want to change** and call `super()` for the rest. Subclass
  `DefaultSolver`. Subclassing `QuerySolver` directly is possible but means implementing all
  stages, including the alias stages (`filter_aliased_channel_metrics`,
  `resolve_channel_selections`), and overriding `from_config` if you need RAW data.
- **Put your settings on a `SolverConfig` subclass.** It keeps the per-table `column_name_mapping`,
  `filters` and `project_id`, and `self.config` is an instance of your class.
- **`register_solver(name, config_cls=SolverConfig, *, aliases=())`** registers the class under the
  name and each alias. Re-registering a name replaces the previous entry.
- **Impulse builds the solver with `from_config(ctx)`.** `DefaultSolver` subclasses inherit a version
  that passes the Spark session, `solver_config` and the RAW-data settings (`data_type`,
  `drop_implausible_data`, `raw_encoder`). Override `from_config` only if your constructor takes
  different arguments.

## Select it in the report config

```json
"query_engine": {
  "solver": "ExcludeContainersSolver",
  "solver_config": { "excluded_container_ids": [3] }
}
```

- `solver` is the registered name. The default is `"DefaultSolver"`; `"DeltaSolver"` and
  `"KeyValueStoreSolver"` are deprecated aliases for it.
- `solver_config` is validated with the config class the selected solver registered.
- In Python, the same keys go on `QueryEngine`:
  `QueryEngine(solver="ExcludeContainersSolver", solver_config={"excluded_container_ids": [3]})`.

For ad-hoc queries you don't need the registry: pass an instance,
`query.solve(spark, solver=ExcludeContainersSolver(spark, config=ExcludeContainersConfig(excluded_container_ids=[3])))`.

## Parse-time validation

The config is checked when it is loaded, so these fail as a validation error rather than at run time:

- an unregistered `solver` name (the message lists the registered names);
- a missing required setting of your config class, or a setting with the wrong type, including when
  `solver_config` is left out entirely;
- an unknown key in `solver_config`: a misspelled setting, or your settings without `"solver"`
  selecting your solver. `SolverConfig` forbids unknown keys and your config class inherits that, so
  neither is silently dropped.

## Rules

- **Import the package that registers the solver before the report config is parsed**, including in
  tools that only generate or edit configs. Selection is by name only: config data never triggers an
  import.
- **Keep each stage's output contract.** `filter_container_metrics` returns all container columns
  needed downstream (container dimensions, event bounds), and `solve` returns one row per container:
  `container_id` plus one column per selection.
- **Use the internal column names from `self.config`** (e.g. `self.config.container_id_col`), not
  string literals: column mapping has already been applied when a stage runs.
- **Calculated channels need `solve_calculated_channels`**, which `DefaultSolver` implements. The
  `QuerySolver` base raises, so subclass `DefaultSolver` if your reports use calculated channels.
