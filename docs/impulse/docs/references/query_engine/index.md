---
title: Query Engine
---

# Query Engine

The query engine resolves channel selections and evaluates time-series expressions against the
silver-layer data. It has two parts:

- **[TSAL](tsal/index.md)** — the expression language you write to select channels, derive virtual
  signals, and define events and aggregations.
- **[Query Solvers](query_solvers.md)** — the `DefaultSolver` that knows how your silver tables are
  laid out, reads them, and evaluates the expressions per container.

## Two ways to use it

- **Directly, for ad-hoc analysis.** Build a query against a `MeasurementDB` and solve it
  interactively — no reporting setup required:

  ```python
  eng_rpm = db.query.channel(channel_name='Engine RPM')
  result = db.query.select(eng_rpm.mean().alias('rpm_mean')).solve(spark, solver=DefaultSolver(spark))
  # or .toPandas(spark, solver=DefaultSolver(spark)) for a pandas DataFrame
  ```

- **As the foundation of the [Report](../report/index.md) component.** The reporting layer embeds TSAL
  expressions in events and aggregations, and the `Report` orchestrator batch-solves them through the
  query engine and persists the results to the gold layer.
