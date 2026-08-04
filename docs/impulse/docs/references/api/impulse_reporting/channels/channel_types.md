---
sidebar_label: channel_types
title: impulse_reporting.channels.channel_types
---

## ChannelType

```python
class ChannelType(Enum)
```

Enumeration of available calculated-channel types.

Mirrors :class:`EventType` / :class:`AggregationType`: maps each enum member
to its class and resolves the associated gold fact/dimension table names and
schemas.

**Arguments**:

- `CALCULATED_CHANNEL` (`CalculatedChannel`): Derived-channel type computed via ``solve_calculated_channels``.

#### get\_fact\_table\_name

```python
def get_fact_table_name() -> str
```

Get the fact table name for the channel type.

**Raises**:

- `ValueError`: If the channel type is not supported.

**Returns**:

`str`: The name of the fact table associated with this channel type.

#### get\_fact\_schema

```python
def get_fact_schema() -> StructType
```

Get the fact schema for the channel type.

**Raises**:

- `ValueError`: If the channel type is not supported.

**Returns**:

`StructType`: The PySpark schema structure for this channel type.

#### get\_dimension\_table\_name

```python
def get_dimension_table_name() -> str
```

Get the dimension table name for the channel type.

**Raises**:

- `ValueError`: If the channel type is not supported.

**Returns**:

`str`: The name of the dimension table associated with this channel type.

#### get\_dimension\_schema

```python
def get_dimension_schema() -> StructType
```

Get the dimension schema for the channel type.

**Raises**:

- `ValueError`: If the channel type is not supported.

**Returns**:

`StructType`: The PySpark schema structure for this channel type.

#### get\_any\_for\_fact\_table

```python
def get_any_for_fact_table(cls, table_name: str) -> "ChannelType"
```

Return the first ChannelType whose fact table name matches.

**Arguments**:

- `table_name` (`str`): Fact table name to look up.

**Raises**:

- `ValueError`: If no ChannelType matches the given table name.

**Returns**:

`ChannelType`: 

#### get\_any\_for\_dimension\_table

```python
def get_any_for_dimension_table(cls, table_name: str) -> "ChannelType"
```

Return the first ChannelType whose dimension table name matches.

**Arguments**:

- `table_name` (`str`): Dimension table name to look up.

**Raises**:

- `ValueError`: If no ChannelType matches the given table name.

**Returns**:

`ChannelType`: 

