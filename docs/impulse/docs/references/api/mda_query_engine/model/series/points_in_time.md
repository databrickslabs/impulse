---
sidebar_label: points_in_time
title: mda_query_engine.model.series.points_in_time
---

PointInTime class implementation


## PointsInTime

```python
class PointsInTime()
```

#### \_\_init\_\_

```python
def __init__(tstarts: npt.NDArray)
```

Initialize the PointsInTime object.

**Arguments**:

- `tstarts` (`numpy.ndarray or array-like`): Array of time points.

#### dtype

```python
def dtype()
```

Returns the Spark data type for points in time.

**Returns**:

`pyspark.sql.types.ArrayType`: Spark ArrayType for points in time: [tstart_1, ..., tstart_N].

#### get\_data

```python
def get_data() -> list
```

Returns a list of time points.

**Returns**:

`list`: List of time points.

#### \_\_len\_\_

```python
def __len__()
```

Returns the number of time points.

**Returns**:

`int`: Number of time points.

#### \_\_and\_\_

```python
def __and__(other: PointsInTime) -> PointsInTime
```

Returns the intersection with another PointsInTime object.

**Arguments**:

- `other` (`PointsInTime`): PointsInTime object to intersect with.

**Returns**:

`PointsInTime`: Intersection result.

#### \_\_or\_\_

```python
def __or__(other: PointsInTime) -> PointsInTime
```

Returns the union with another PointsInTime object.

**Arguments**:

- `other` (`PointsInTime`): PointsInTime object to union with.

**Returns**:

`PointsInTime`: Union result.

#### expand\_right

```python
def expand_right(width: float)
```

Expands each point in time to an interval to the right by the given width.

**Arguments**:

- `width` (`float`): Amount to expand to the right (in seconds).

**Returns**:

`Intervals`: Intervals object with expanded right bounds.

#### expand\_left

```python
def expand_left(width: float)
```

Expands each point in time to an interval to the left by the given width.

**Arguments**:

- `width` (`float`): Amount to expand to the left (in seconds).

**Returns**:

`Intervals`: Intervals object with expanded left bounds.

#### expand

```python
def expand(width: float)
```

Expands each point in time to an interval on both sides by the given width.

**Arguments**:

- `width` (`float`): Amount to expand both sides (in seconds).

**Returns**:

`Intervals`: Intervals object with expanded bounds.

#### empty

```python
def empty()
```

Returns an empty PointsInTime object.

**Returns**:

`PointsInTime`: Empty PointsInTime object.

