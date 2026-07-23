---
sidebar_label: calculated_channel
title: impulse_reporting.channels.calculated_channel
---

## CalculatedChannel

```python
class CalculatedChannel()
```

A reporting-layer calculated (derived) channel.

Orchestration counterpart to a query-engine ``CalculatedChannel``: it wraps a
:class:`TimeSeriesExpression` built from the operator DSL (e.g.
``q.channel(channel_name="raw_speed") * 3.6``) plus an ``identity`` dict, and
is driven by :class:`Report` to compute the channel across containers, persist
the narrow result to a gold fact table, and update it incrementally.

Structurally parallels :class:`BasicEvent` (holds an aliased expression,
name-derived id, SHA-256 definition hash) but — like ``ContainerEvent`` — it
drives its own solve via ``QueryBuilder.solve_calculated_channels`` rather than
riding the centralized wide ``solved_df``.  Accordingly :meth:`get_expression`
returns ``None`` so it is excluded from the batch solve.

**Arguments**:

- `name` (`str`): Name of the calculated channel (used as the entity id seed's fallback and
stored on the dimension row).
- `expr` (`TimeSeriesExpression`): The wrapped expression; must evaluate to a ``SampleSeries``.
- `identity` (`Mapping[str, str]`): Channel identity.  Any non-empty set of keys; seeds the deterministic
``channel_id`` and is stored once on ``calculated_channel_dimension`` as a
``MapType(string, string)`` column (joined to the fact via ``channel_id``,
not repeated on fact rows).
- `desc` (`str`): Human-readable description (stored on the dimension row, excluded from the
definition hash).
- `attributes` (`Mapping[str, str]`): Key-value metadata stored on the dimension row.

#### canonical\_identity

```python
def canonical_identity() -> str
```

Public, order-independent identity key.

Two channels with the same ``identity`` (regardless of key insertion
order) share this value and therefore the same ``channel_id``.  Used by


#### get\_name

```python
def get_name() -> str
```

Return the channel name.


#### set\_report\_id

```python
def set_report_id(report_id: int)
```

Set the owning report id.


#### get\_id

```python
def get_id() -> int
```

Return the deterministic entity id (also the fact/dimension ``channel_id``).


#### get\_expression

```python
def get_expression() -> TimeSeriesExpression | None
```

Return ``None`` — calculated channels drive their own narrow solve.

Returning ``None`` keeps this channel out of the centralized wide batch
solve (``collect_solvable_expressions``), mirroring ``ContainerEvent``.


#### get\_expression\_str

```python
def get_expression_str() -> str
```

String form of the wrapped expression (identity + expr, no name/desc).


#### get\_channel\_type\_str

```python
def get_channel_type_str() -> str
```

Channel type string, matching the ``ChannelType`` enum member name.


#### determine\_definition\_hash

```python
def determine_definition_hash() -> int
```

Hash of the computation-affecting definition (expression + identity).


#### as\_dict

```python
def as_dict() -> dict
```

Dictionary representation of the dimension metadata.

``identity`` is a plain dict, persisted on the dimension as a
``MapType(string, string)`` column (no fixed per-key columns).


#### as\_spark\_row

```python
def as_spark_row() -> Row
```

Spark Row representation of the dimension metadata.


#### determine\_calculated\_channels

```python
def determine_calculated_channels(
        cls,
        spark: SparkSession,
        channels: list[CalculatedChannel],
        *,
        query: QueryBuilder = None,
        solver: QuerySolver = None,
        pre_filtered_containers_df: DataFrame = None) -> DataFrame | None
```

Solve the given channels and shape the result into fact rows.

Drives ``QueryBuilder.solve_calculated_channels`` (the narrow, many-rows-
per-container endpoint) with the report's ``query`` + ``solver``, then
projects to :data:`CALCULATED_CHANNEL_FACT_SCHEMA`.  Because each channel's
``channel_id`` was fixed to its entity id at construction, no id-join is
needed.

**Arguments**:

- `spark` (`SparkSession`): Spark session, forwarded to ``QueryBuilder.solve_calculated_channels``.
- `channels` (`list of CalculatedChannel`): Channels to solve; identity keys may differ across channels.
- `query` (`QueryBuilder`): Query builder used to select and solve the channels.
- `solver` (`QuerySolver`): Solver implementing ``solve_calculated_channels`` (a ``DefaultSolver``).
- `pre_filtered_containers_df` (`DataFrame`): Incremental container subset; ``None`` processes all containers.

**Returns**:

`DataFrame or None`: Narrow fact DataFrame, or ``None`` when there are no channels.

#### determine\_metadata\_df

```python
def determine_metadata_df(cls, spark: SparkSession,
                          channels: list[CalculatedChannel])
```

Create the dimension DataFrame for the given channels.

``identity`` is a self-describing ``MapType(string, string)`` column,
which ``createDataFrame`` builds directly from the plain dict returned by


