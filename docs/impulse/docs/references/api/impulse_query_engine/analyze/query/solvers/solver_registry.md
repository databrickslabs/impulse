---
sidebar_label: solver_registry
title: impulse_query_engine.analyze.query.solvers.solver_registry
---

Name registry for query solvers, so a report config can select a solver by name.

A solver registers under a name with ``@register_solver(...)`` and is selected with
``query_engine.solver`` in the report config. Selection is by name only: the package that
registers a solver must be imported before the report config is parsed.


#### register\_solver

```python
def register_solver(name: str,
                    config_cls: type[SolverConfig] = SolverConfig,
                    *,
                    aliases: tuple[str, ...] = ())
```

Register a ``QuerySolver`` subclass under *name* (and *aliases*), configured by *config_cls*.

``config_cls`` validates ``query_engine.solver_config`` when this solver is selected.
Re-registering a name replaces the previous entry.


#### resolve\_solver

```python
def resolve_solver(name: str) -> Registration[QuerySolver, SolverConfig]
```

Return the registration for *name*; raises ``KeyError`` listing the registered names.


