---
sidebar_label: tag_expression
title: mda_query_engine.analyze.metadata.tag_expression
---

## TagSelector

```python
class TagSelector(TagExpression)
```

#### \_\_init\_\_

```python
def __init__(key: str, cast_type: str | None = None)
```

Initialize a TagSelector.

**Arguments**:

- `key` (`str`): The name of the tag to select.
- `cast_type` (`str or None`): Spark type to cast the column to before comparison
(e.g. ``"int"``, ``"double"``, ``"string"``, ``"timestamp"``).
When *None* (default) no casting is applied.

#### get\_selector\_expr

```python
def get_selector_expr() -> Column
```

Return a Spark SQL column expression for the selected tag.

**Returns**:

`pyspark.sql.Column`: Spark SQL column corresponding to the tag key, cast to
``cast_type`` when one is configured.

#### build\_pandas

```python
def build_pandas(df: pd.DataFrame) -> pd.Series
```

Return a pandas Series for the selected tag from the DataFrame.

**Arguments**:

- `df` (`pandas.DataFrame`): DataFrame containing tag data.

**Returns**:

`pandas.Series`: Series corresponding to the tag key, cast when configured.

#### required\_tags

```python
def required_tags() -> set[str]
```

Return a set containing the tag key.

**Returns**:

`set of str`: Set containing the tag key.

#### \_\_hash\_\_

```python
def __hash__()
```

Return the hash of the TagSelector.

**Returns**:

`int`: Hash value.

#### \_\_repr\_\_

```python
def __repr__()
```

Return the string representation of the TagSelector.

**Returns**:

`str`: String representation.

#### \_\_str\_\_

```python
def __str__()
```

Return the string representation of the TagSelector.

**Returns**:

`str`: String representation.

#### as\_dict

```python
def as_dict() -> dict
```

Return a dictionary representation of the TagSelector.

**Returns**:

`dict`: Dictionary representation.

#### from\_dict

```python
def from_dict(obj: dict) -> TagSelector
```

Construct a TagSelector from a dictionary.

**Arguments**:

- `obj` (`dict`): Dictionary containing tag selector data.

**Returns**:

`TagSelector`: TagSelector instance.

## TagOp

```python
class TagOp(TagExpression)
```

#### \_\_init\_\_

```python
def __init__(operation, *args, **kwargs)
```

Initialize a TagOp.

**Arguments**:

- `operation` (`callable`): The operation to apply.
- `*args`: Arguments like (TagSelector<channel_name>, 'Engine RPM') for the operation.
- `**kwargs`: Keyword arguments for the operation.

#### get\_selector\_expr

```python
def get_selector_expr() -> Column
```

Build a Spark SQL expression for the tag operation.

**Returns**:

`pyspark.sql.Column`: Spark SQL column representing the tag operation.

#### build\_pandas

```python
def build_pandas(df: pd.DataFrame) -> pd.Series
```

Build a pandas Series for the tag operation from the given DataFrame.

**Arguments**:

- `df` (`pandas.DataFrame`): DataFrame containing tag data.

**Returns**:

`pandas.Series`: Series representing the tag operation.

#### required\_tags

```python
def required_tags() -> set[str]
```

Return a set of required tag keys for the operation.

**Returns**:

`set of str`: Set of required tag keys.

#### \_\_hash\_\_

```python
def __hash__()
```

Return the hash of the TagOp.

**Returns**:

`int`: Hash value.

#### \_\_repr\_\_

```python
def __repr__()
```

Return the string representation of the TagOp.

**Returns**:

`str`: String representation.

#### \_\_str\_\_

```python
def __str__()
```

Return the string representation of the TagOp.

**Returns**:

`str`: String representation.

#### as\_dict

```python
def as_dict() -> dict
```

Return a dictionary representation of the TagOp.

**Returns**:

`dict`: Dictionary representation.

#### from\_dict

```python
def from_dict(obj: dict) -> TagOp
```

Construct a TagOp from a dictionary.

**Arguments**:

- `obj` (`dict`): Dictionary containing tag operation data.

**Returns**:

`TagOp`: TagOp instance.

