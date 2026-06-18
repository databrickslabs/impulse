---
sidebar_label: points_in_time_series
title: impulse_query_engine.model.series.points_in_time_series
---

PointsInTimeSeries class implementation


## PointsInTimeSeries

```python
class PointsInTimeSeries()
```

#### \_\_init\_\_

```python
def __init__(tstarts: Sized, values: Sized)
```

Initialize the PointsInTimeSeries object.

A PointsInTimeSeries associates a value to each timestamp. Unlike a SampleSeries,
a value is only defined *at* its timestamp and is not considered valid in between
consecutive timestamps.

**Arguments**:

- `tstarts` (`Sized`): Array-like of time points.
- `values` (`Sized`): Array-like of values, one per time point.

#### dtype

```python
def dtype()
```

Returns the Spark data type for PointsInTimeSeries.

**Returns**:

`pyspark.sql.types.ArrayType`: Spark ArrayType for points in time series: [[tstart_1, value_1], ...].

#### get\_data

```python
def get_data() -> list
```

Returns the series as a list of [tstart, value] lists.

**Returns**:

`list`: List of [tstart, value] pairs.

#### \_\_len\_\_

```python
def __len__() -> int
```

Returns the number of points in the series.

**Returns**:

`int`: Number of points.

#### start\_time

```python
def start_time() -> FloatOrNaN
```

Returns the time of the first point.

**Returns**:

`float`: Start time or NaN if empty.

#### end\_time

```python
def end_time() -> FloatOrNaN
```

Returns the time of the last point.

**Returns**:

`float`: End time or NaN if empty.

#### to\_points\_in\_time

```python
def to_points_in_time() -> PointsInTime
```

Returns the timestamps of this series as a PointsInTime object (dropping the values).

**Returns**:

`PointsInTime`: Points in time with this series' timestamps.

#### plane\_sweep

```python
def plane_sweep(
    points, other: SampleSeries | Intervals | PointsInTime | PointsInTimeSeries
) -> list[tuple[int, int]]
```

Find overlaps between a point-like series and another series/interval object.

**Arguments**:

- `points` (`PointsInTimeSeries or PointsInTime`): Point-like object exposing a ``tstarts`` attribute.
- `other` (`SampleSeries, Intervals, PointsInTime, or PointsInTimeSeries`): Object to find overlaps with. Interval-like objects (SampleSeries, Intervals) match
points contained in their half-open intervals; point-like objects (PointsInTime,
PointsInTimeSeries) match points with equal timestamps.

**Raises**:

- `NotImplementedError`: If ``other`` is not one of the supported types.

**Returns**:

`list of tuple`: ``(points_idx, other_idx)`` index pairs indicating overlaps.

#### synchronized

```python
def synchronized(
    other: SampleSeries | PointsInTimeSeries
) -> tuple[PointsInTimeSeries, PointsInTimeSeries]
```

Synchronize this point series with a value-carrying series.

The result grid is the subset of this series' timestamps that overlap ``other``: points
contained in one of ``other``'s intervals (when ``other`` is a SampleSeries) or points with
a matching timestamp (when ``other`` is a PointsInTimeSeries). Out-of-overlap points are
dropped. Both returned series share the same timestamps.

**Arguments**:

- `other` (`SampleSeries or PointsInTimeSeries`): Value-carrying series to synchronize with.

**Raises**:

- `NotImplementedError`: If ``other`` does not carry values (Intervals, PointsInTime).

**Returns**:

`tuple of PointsInTimeSeries`: Synchronized (this series, other series).

#### synchronized\_all

```python
def synchronized_all(
    others: list[SampleSeries | PointsInTimeSeries]
) -> tuple[PointsInTimeSeries, ...]
```

Synchronize this point series with multiple value-carrying series.

The common grid is narrowed against each operand in turn; all previously synchronized
series are re-aligned to the narrowed grid.

**Arguments**:

- `others` (`list of SampleSeries or PointsInTimeSeries`): Series to synchronize with.

**Raises**:

- `NotImplementedError`: If any operand does not carry values (Intervals, PointsInTime).

**Returns**:

`tuple of PointsInTimeSeries`: Synchronized series, one per input series (this series first).

#### \_\_add\_\_

```python
def __add__(
        other: float | SampleSeries | PointsInTimeSeries
) -> PointsInTimeSeries
```

Add another series or scalar to this series.


#### \_\_radd\_\_

```python
def __radd__(
        other: float | SampleSeries | PointsInTimeSeries
) -> PointsInTimeSeries
```

Add this series to another series or scalar (reversed operands).


#### \_\_sub\_\_

```python
def __sub__(
        other: float | SampleSeries | PointsInTimeSeries
) -> PointsInTimeSeries
```

Subtract another series or scalar from this series.


#### \_\_rsub\_\_

```python
def __rsub__(
        other: float | SampleSeries | PointsInTimeSeries
) -> PointsInTimeSeries
```

Subtract this series from another series or scalar (reversed operands).


#### \_\_mul\_\_

```python
def __mul__(
        other: float | SampleSeries | PointsInTimeSeries
) -> PointsInTimeSeries
```

Multiply this series by another series or scalar.


#### \_\_rmul\_\_

```python
def __rmul__(
        other: float | SampleSeries | PointsInTimeSeries
) -> PointsInTimeSeries
```

Multiply another series or scalar by this series (reversed operands).


#### \_\_truediv\_\_

```python
def __truediv__(
        other: float | SampleSeries | PointsInTimeSeries
) -> PointsInTimeSeries
```

Divide this series by another series or scalar.


#### \_\_rtruediv\_\_

```python
def __rtruediv__(
        other: float | SampleSeries | PointsInTimeSeries
) -> PointsInTimeSeries
```

Divide another series or scalar by this series (reversed operands).


#### \_\_gt\_\_

```python
def __gt__(other: float | SampleSeries | PointsInTimeSeries) -> PointsInTime
```

Return points where this series is greater than another.


#### \_\_ge\_\_

```python
def __ge__(other: float | SampleSeries | PointsInTimeSeries) -> PointsInTime
```

Return points where this series is greater than or equal to another.


#### \_\_lt\_\_

```python
def __lt__(other: float | SampleSeries | PointsInTimeSeries) -> PointsInTime
```

Return points where this series is less than another.


#### \_\_le\_\_

```python
def __le__(other: float | SampleSeries | PointsInTimeSeries) -> PointsInTime
```

Return points where this series is less than or equal to another.


#### \_\_eq\_\_

```python
def __eq__(other: float | SampleSeries | PointsInTimeSeries) -> PointsInTime
```

Return points where this series is equal to another.


#### \_\_ne\_\_

```python
def __ne__(other: float | SampleSeries | PointsInTimeSeries) -> PointsInTime
```

Return points where this series is not equal to another.


#### count

```python
def count() -> int
```

Returns the number of points in the series.

**Returns**:

`int`: Number of points.

#### sum

```python
def sum() -> FloatOrNaN
```

Returns the sum of the values.

**Returns**:

`float`: Sum of values, or NaN if empty.

#### mean

```python
def mean() -> FloatOrNaN
```

Returns the mean of the values.

**Returns**:

`float`: Mean of values, or NaN if empty.

#### min

```python
def min() -> FloatOrNaN
```

Returns the minimum value.

**Returns**:

`float`: Minimum value, or NaN if empty.

#### max

```python
def max() -> FloatOrNaN
```

Returns the maximum value.

**Returns**:

`float`: Maximum value, or NaN if empty.

#### \_\_str\_\_

```python
def __str__() -> str
```

Returns a string representation of the PointsInTimeSeries.

**Returns**:

`str`: String representation.

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Returns a string representation for debugging.

**Returns**:

`str`: String representation.

#### empty

```python
def empty() -> PointsInTimeSeries
```

Returns an empty PointsInTimeSeries.

**Returns**:

`PointsInTimeSeries`: Empty PointsInTimeSeries object.

