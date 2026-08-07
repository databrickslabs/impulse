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
riding the centralized wide ``solved_df``.  It is dispatched separately from
the batch solve (never passed to ``collect_solvable_expressions``).

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
def get_expression() -> TimeSeriesExpression
```

Return the wrapped query-engine ``CalculatedChannel`` expression.


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


#### determine\_channel\_metrics

```python
def determine_channel_metrics(
        cls,
        spark: SparkSession,
        channels: list[CalculatedChannel],
        fact_df: DataFrame | None,
        *,
        attribute_columns: list[str] | None = None,
        kpis: list[str] | None = None) -> DataFrame | None
```

Derive a silver-shaped ``channel_metrics`` DataFrame from the fact rows.

The calculated-channel fact table already matches the silver ``channels``
table; this builds its companion ``channel_metrics`` so the pair can serve
as an Impulse silver source.  Metrics are aggregated **directly from the
narrow fact rows** (``container_id, channel_id, tstart, tend, value``),
grouped by ``(container_id, channel_id)``.

The output schema is **dynamic**: fixed columns ``container_id,
channel_id, type, data_type`` plus one column per configured KPI (see
``kpis``), one per identity key (the union across all ``channels``), and one
per configured attribute key.  Identity/attribute values are pulled from
each channel's in-memory ``identity`` / ``attributes`` dicts (null where a
channel omits a key).  On an identity/attribute key collision, identity wins
and the attribute is skipped.

**Arguments**:

- `spark` (`SparkSession`): Session used to build the per-channel metadata frame.
- `channels` (`list of CalculatedChannel`): The channels whose fact rows are in ``fact_df``; supply identity and
attributes.
- `fact_df` (`DataFrame or None`): Narrow fact DataFrame (output of :meth:`determine_calculated_channels`).
``None`` returns ``None``.
- `attribute_columns` (`list of str`): Attribute keys to surface as columns.  Default/empty → no attribute
columns.  A key no channel defines yields an all-null column.
- `kpis` (`list of str`): KPI names to compute (see ``calculated_channel_kpis.KPI_BUILDERS``); the
output carries one column per name, in order.  ``None`` → the default
KPIs (``duration, min, max, mean``).

**Returns**:

`DataFrame or None`: The dynamic-schema metrics DataFrame, or ``None`` when ``fact_df`` is
``None``.

