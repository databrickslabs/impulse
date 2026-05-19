---
sidebar_label: solver_config
title: impulse_query_engine.analyze.query.solvers.solver_config
---

Configuration for solver column mappings.

Provides Pydantic models that map silver-layer column names to the internal
column names used by the solver classes, making the solvers independent
of a specific data-layer naming convention.

Each input table has its own :class:`TableConfig` section with an optional
``column_name_mapping`` (physical column → internal name) and ``filters``
(internal column → equality value).

Solvers apply the ``column_name_mapping`` when reading a table to rename
physical columns to internal names.  All subsequent processing — including
filter application — uses the framework-internal column names exposed as
properties on :class:`SolverConfig`.


## TableConfig

```python
class TableConfig(BaseModel)
```

Per-table configuration for column renaming and equality filters.

**Arguments**:

- `column_name_mapping` (`dict[str, str]`): Mapping from physical column names on the table to internal
names used by the solver.  An empty dict means no renaming
(physical names already match internal names).
- `filters` (`dict[str, str]`): Equality filters applied to the table **after** column renaming.
Keys are internal column names; values are the literal values
to match.

## SolverConfig

```python
class SolverConfig(BaseModel)
```

Per-table configuration for solver column name mappings and filters.

The framework uses a fixed set of internal column names (e.g.
``container_id``, ``channel_id``, ``tstart``, ``tend``, ``value``).
When a silver-layer table uses different physical column names, the
per-table ``column_name_mapping`` renames them to the internal names
so that solver code can always reference the same constants.

**Arguments**:

- `project_id` (`str or None`): Optional project identifier applied as a filter on relevant tables
(container_tags, channel_mapping) by solvers that support it.
- `container_tags` (`TableConfig`): Column mappings and filters for the container tags (narrow/EAV) table.
- `container_metrics` (`TableConfig`): Column mappings and filters for the container metrics table.
- `channel_tags` (`TableConfig`): Column mappings and filters for the channel tags table.
- `channel_metrics` (`TableConfig`): Column mappings and filters for the channel metrics table.
- `channel_mapping` (`TableConfig`): Column mappings and filters for the channel mapping (alias) table.
- `channels` (`TableConfig`): Column mappings and filters for the channel data table.

#### from\_json

```python
def from_json(cls, json_path: str) -> "SolverConfig"
```

Load a SolverConfig from a JSON file.

**Arguments**:

- `json_path` (`str`): Path to the JSON configuration file.

**Returns**:

`SolverConfig`: A new SolverConfig instance populated from the file.

#### from\_dict

```python
def from_dict(cls, data: dict) -> "SolverConfig"
```

Create a SolverConfig from a dictionary.

This is a convenience alias for ``model_validate(data)``.

**Arguments**:

- `data` (`dict`): Dictionary with configuration keys.

**Returns**:

`SolverConfig`: A new SolverConfig instance populated from *data*.

#### container\_id\_col

```python
def container_id_col() -> str
```

Internal column name for the container identifier.


#### channel\_id\_col

```python
def channel_id_col() -> str
```

Internal column name for the channel identifier.


#### channel\_id\_cols

```python
def channel_id_cols() -> list[str]
```

Composite key ``[container_id, channel_id]``.


#### tstart\_col

```python
def tstart_col() -> str
```

Internal column name for the start timestamp.


#### tend\_col

```python
def tend_col() -> str
```

Internal column name for the end timestamp.


#### value\_col

```python
def value_col() -> str
```

Internal column name for the signal value on the channels table.


#### tag\_key\_col

```python
def tag_key_col() -> str
```

Internal column name for the attribute key on the container_tags (EAV) table.


#### tag\_value\_col

```python
def tag_value_col() -> str
```

Internal column name for the attribute value on the container_tags (EAV) table.


#### alias\_priority\_col

```python
def alias_priority_col() -> str
```

Internal column name for the alias priority on the channel_mapping table.


#### project\_id\_col

```python
def project_id_col() -> str
```

Internal column name for the project identifier.


#### parent\_id\_col

```python
def parent_id_col() -> str
```

Internal column name for the parent/scope identifier.


#### col\_map

```python
def col_map() -> dict[str, str]
```

Short-key → internal-column-name mapping for UDFs and caches.


