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

## Snapshot consistency

A single solve fans out into many lazy reads across the silver tables. By default each read sees the
_latest_ Delta version at the moment it materializes, so a table that changes mid-analysis can be
observed inconsistently across those reads.

To freeze a consistent snapshot, pin the silver tables to their current Delta versions before
querying — every subsequent read then uses `versionAsOf` that pinned version:

```python
db.pin_versions(spark)   # resolve each table's current version once
result = db.query.select(...).solve(spark, solver=DefaultSolver(spark))
```

:::note Opt-in for direct use, automatic in reporting
For direct query-engine use, pinning is **opt-in** — call `pin_versions` yourself when you want it.
The pin is stored on the config and **persists until cleared**, so a long-lived `MeasurementDB` keeps
reading the pinned snapshot until you re-pin (call it again) or unpin (`db.config.pinned_versions = {}`).
The [Report](../report/index.md) component pins automatically at the start of every run, so snapshot
consistency there needs no action.
:::
