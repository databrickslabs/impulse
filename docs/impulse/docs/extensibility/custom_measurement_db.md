---
sidebar_position: 1
title: Custom measurement DB
---

# Custom measurement DB

The **MeasurementDB** is the layer between your stored tables and everything that reads them — the
solve, incremental container detection, and ad-hoc analysis all read through it. Impulse ships a
built-in MeasurementDB that reads the [silver model](../data_model/silver_layer_schema.md) directly.
When your raw tables need more than column renames to match that model — unions, joins, a grain
change, an EAV unpivot, or a row filter that must run *before* a reshape — you can register your own
MeasurementDB and select it by name in the report config. The stock solver then runs unchanged, and
every read path sees the same silver-shaped data.

This is an additive, opt-in extension point: with nothing configured, the built-in MeasurementDB is
used and behaviour is unchanged.

## When to use it

| Your data differs from the silver model by… | Use |
|---|---|
| Column names, or narrow-EAV vs. wide layout | `DefaultSolver` + [`solver_config`](../config/configuration.md#solver-column-mappings-and-filters) — no code |
| Different tables, unions, joins, grain changes, EAV unpivots, or a pre-reshape row filter | **A custom MeasurementDB** (this page) |
| Query planning / execution the config cannot express | A change to the solver in Impulse (solvers are not customer-pluggable as of now) |

## Select it in the report config

Two optional keys under `query_engine` choose the implementation and pass it settings:

```json
"query_engine": {
  "measurement_db": "MyMeasurementDB",
  "measurement_db_config": { "session_table": "catalog.raw.sessions" }
}
```

- `measurement_db` — the **registered name** of the implementation (default `"MeasurementDB"`, the built-in reader).
- `measurement_db_config` — keyword arguments for that implementation's config class, merged with the
  `source` tables (e.g. extra source tables, prefilter keys).

In Python the same keys go on `QueryEngine`:
`QueryEngine(measurement_db="MyMeasurementDB", measurement_db_config={...})`.

## Implement one

A custom MeasurementDB is a small subclass of the built-in `MeasurementDB`, plus a config subclass of
`MeasurementDBConfig`, registered under a name:

```python
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from impulse_query_engine.measurement_db_registry import register_measurement_db


class MyDBConfig(MeasurementDBConfig):
    def __init__(self, *, session_table: str, **base_kwargs):  # required: no default
        super().__init__(**base_kwargs)   # keeps the source tables, table_locations, and pinning state
        self.session_table = session_table

    def configured_table_uris(self) -> list[str]:
        # Declare extra tables so they are pinned to one Delta snapshot per run (see below).
        return super().configured_table_uris() + [self.session_table]


@register_measurement_db("MyMeasurementDB", MyDBConfig)
class MyMeasurementDB(MeasurementDB):
    def container_metrics(self, spark):
        raw = super().container_metrics(spark)                         # raw read, pinned
        sessions = self._read_table(spark, self.config.session_table)  # extra read, pinned
        return ...  # reshape into the silver container_metrics shape
```

Override one method per silver table you need to reshape; the rest fall back to the base read. Each
override follows the same shape: call `super().<table>(spark)` for the raw, pinned read, then
join / union / reshape into the silver frame the solver expects.

## Rules

- **Import the registering package before the report config is parsed.** Selection is by name only
  (config data never triggers an import), so the package whose import runs `@register_measurement_db`
  must be imported first — including in any tool that only *generates* or *edits* a config.
- **Subclass `MeasurementDBConfig` and call `super().__init__(**base_kwargs)`.** The base initializer
  sets the source tables, `table_locations`, and the per-run pinning state the reads rely on.
- **Keep the config `__init__` free of side effects** — it is constructed once to validate the config
  at parse time and once more to build the DB.
- **Keep the DB constructor signature `__init__(self, config, ws)`** — Impulse builds it as `db_cls(config, ws)`.
- **Your DB owns correctness.** Impulse does not validate the frames a custom DB returns; it must
  provide the columns, types, row grain, and time unit the solver expects. The built-in `MeasurementDB`
  and the schemas in `impulse_query_engine/schema.py` are the reference.

## Parse-time validation

The config is checked when it is loaded (`ImpulseConfig.model_validate`), so these fail fast as a
validation error rather than at run time:

- an unregistered `measurement_db` name (the message lists the registered names);
- a missing required argument of the config class;
- an unknown or misspelled key;
- a key present in both `source` and `measurement_db_config`.

## Snapshot pinning (extra tables)

At the start of each run Impulse pins every configured table to one Delta version so all lazy reads
observe a consistent snapshot. The built-in pinning iterates `configured_table_uris()`, so any extra
table your DB reads must be:

1. **declared** — override `configured_table_uris()` to add its URI; and
2. **read through `self._read_table(spark, uri)`** (never `spark.read.table`), using the **same URI
   string** you declared.

Views and non-Delta tables cannot be time-travelled: they are skipped with a warning and read at the
latest version.

## Incremental reporting

Because every reader goes through the MeasurementDB, incremental container detection reads the same
reshaped `container_metrics` as the solve, so the silver↔gold join stays aligned by construction. If
your reshape renames the physical container-id column, the rename is applied in exactly one place and
both the solve and detection see it. See [incremental](../config/configuration.md#incremental-optional)
for how the mode is resolved.
