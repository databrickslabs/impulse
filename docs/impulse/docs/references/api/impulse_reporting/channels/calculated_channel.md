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
- `identity` (`Mapping[str, str]`): Output identity columns.  Must contain exactly the keys
``{"channel_name", "data_key"}``; the values are emitted as literals on
every fact row and seed the deterministic ``channel_id``.
- `desc` (`str`): Human-readable description (stored on the dimension row, excluded from the
definition hash).
- `attributes` (`Mapping[str, str]`): Key-value metadata stored on the dimension row.

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

Uses the wrapped-expression string, which encodes identity and the
expression but not name/description/report_id/attributes.  SHA-256, first
8 bytes as a signed int (fits ``LongType``) — same technique as events.


#### as\_dict

```python
def as_dict() -> dict
```

Dictionary representation matching ``CALCULATED_CHANNEL_DIMENSION_SCHEMA``.


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

- `spark` (`SparkSession`): Spark session (unused directly; kept for interface parity).
- `channels` (`list of CalculatedChannel`): Channels to solve; all share the same identity key set.
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


