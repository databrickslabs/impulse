---
sidebar_label: intervals
title: impulse_query_engine.model.series.intervals
---

Intervals class implementation


## Intervals

```python
class Intervals()
```

#### \_\_init\_\_

```python
def __init__(tstarts: npt.NDArray,
             tends: npt.NDArray,
             merge_overlaps=False,
             del_last_empty=False)
```

Initialize the Intervals object.

**Arguments**:

- `tstarts` (`numpy.ndarray or array-like`): Array of interval start times.
- `tends` (`numpy.ndarray or array-like`): Array of interval end times.
- `merge_overlaps` (`bool`): If True, merge overlapping and consecutive intervals (default is False).
- `del_last_empty` (`bool`): If True, remove empty intervals at the end (default is False).

#### dtype

```python
def dtype()
```

Returns the Spark data type for intervals.

**Returns**:

`pyspark.sql.types.ArrayType`: Spark ArrayType for intervals: [[tstart_1, tend_1], ..., [tstart_N, tend_N]].

#### get\_data

```python
def get_data() -> list
```

Returns a list of [tstart, tend] pairs.

**Returns**:

`list`: List of interval start and end pairs.

#### merge\_overlaps

```python
def merge_overlaps(inplace=False) -> Intervals
```

Merge overlapping and consecutive intervals together.

**Arguments**:

- `inplace` (`bool`): If True, modifies the current object in place (default is False).

**Returns**:

`Intervals`: Intervals object with merged intervals.

#### merge\_intervals

```python
def merge_intervals(d: float) -> Intervals
```

Merge intervals whose gap is strictly less than d time units.

**Arguments**:

- `d` (`float`): Maximum gap (in time units) between consecutive intervals to merge.

**Raises**:

- `ValueError`: If d is negative.

**Returns**:

`Intervals`: New Intervals object with close intervals merged.

#### debounce

```python
def debounce(d: float) -> Intervals
```

Keep intervals only after the signal state has sustained for at least d

time units (debounce / sustaining semantics).

Short intervals (duration < d) that follow a confirmed event within
debounce tolerance are absorbed into that event.  Short intervals that
are isolated (no confirmed event yet, or gap from the last confirmed end
is >= d) are discarded.  Long intervals (duration >= d) always start or
extend a confirmed event.

The difference from ``merge_intervals`` and ``filter`` is best shown
with an example.  Consider ``d = 3`` (signal must sustain 3 units)::

    original signal:    ________--__--__---------------____-____----__----__--__---
    merge_intervals(3): ________-----------------------____-____------------------- (3 events)
    filter(3):          ________________---------------_________----__----______--- (4 events)
    debounce(3):        ________________---------------_________------------------- (2 events)

**Arguments**:

- `d` (`float`): Debounce threshold in time units.  The signal must sustain for at
least this long to be recognised as a valid event.

**Raises**:

- `ValueError`: If d is negative.

**Returns**:

`Intervals`: New Intervals object with debounced intervals.

#### filter

```python
def filter(d: float) -> Intervals
```

Remove intervals whose duration is strictly less than d time units.

**Arguments**:

- `d` (`float`): Minimum duration (in time units) for an interval to be kept.

**Raises**:

- `ValueError`: If d is negative.

**Returns**:

`Intervals`: New Intervals object with short intervals removed.

#### starts

```python
def starts() -> np.ndarray[np.float64]
```

Returns an array of all start times.

**Returns**:

`numpy.ndarray`: Array of interval start times.

#### ends

```python
def ends() -> np.ndarray[np.float64]
```

Returns an array of all end times.

**Returns**:

`numpy.ndarray`: Array of interval end times.

#### start\_time

```python
def start_time() -> np.float64
```

Returns the start time of the first interval.

**Returns**:

`float`: Start time of the first interval, or NaN if empty.

#### end\_time

```python
def end_time() -> np.int64
```

Returns the end time of the last interval.

**Returns**:

`float`: End time of the last interval, or NaN if empty.

#### duration\_ms

```python
def duration_ms() -> np.float64
```

Returns the total duration in milliseconds.

**Returns**:

`float`: Total duration (end_time - start_time) in milliseconds.

#### durations

```python
def durations() -> npt.NDArray
```

Returns an array containing the durations of all intervals.

**Returns**:

`numpy.ndarray`: Array of durations for all intervals.

#### expand\_left

```python
def expand_left(width: float) -> Intervals
```

Expands the left bound of all intervals by width seconds.

**Arguments**:

- `width` (`float`): Amount to expand the left bound (in seconds).

**Returns**:

`Intervals`: New Intervals object with expanded left bounds.

#### expand\_right

```python
def expand_right(width: float) -> Intervals
```

Expands the right bound of all intervals by width seconds.

**Arguments**:

- `width` (`float`): Amount to expand the right bound (in seconds).

**Returns**:

`Intervals`: New Intervals object with expanded right bounds.

#### expand

```python
def expand(width: float) -> Intervals
```

Expands both bounds of all intervals by width seconds.

**Arguments**:

- `width` (`float`): Amount to expand both bounds (in seconds).

**Returns**:

`Intervals`: New Intervals object with expanded bounds.

#### shrink\_left

```python
def shrink_left(width: float) -> Intervals
```

Shrinks the left bound of all intervals by width seconds.

**Arguments**:

- `width` (`float`): Amount to shrink the left bound (in seconds).

**Returns**:

`Intervals`: New Intervals object with shrunk left bounds.

#### shrink\_right

```python
def shrink_right(width: float) -> Intervals
```

Shrinks the right bound of all intervals by width seconds.

**Arguments**:

- `width` (`float`): Amount to shrink the right bound (in seconds).

**Returns**:

`Intervals`: New Intervals object with shrunk right bounds.

#### shrink

```python
def shrink(width: float) -> Intervals
```

Shrinks both bounds of all intervals by width seconds.

**Arguments**:

- `width` (`float`): Amount to shrink both bounds (in seconds).

**Returns**:

`Intervals`: New Intervals object with shrunk bounds.

#### \_\_and\_\_

```python
def __and__(other: Intervals | PointsInTime) -> Intervals | PointsInTime
```

Returns the intersection with another Intervals or PointsInTime object.

**Arguments**:

- `other` (`Intervals or PointsInTime`): Object to intersect with.

**Returns**:

`Intervals or PointsInTime`: Intersection result.

#### \_\_or\_\_

```python
def __or__(other: Intervals) -> Intervals
```

Returns the union with another Intervals object.

**Arguments**:

- `other` (`Intervals`): Intervals object to union with.

**Returns**:

`Intervals`: Union of intervals.

#### \_\_len\_\_

```python
def __len__() -> int
```

Returns the number of intervals.

**Returns**:

`int`: Number of intervals.

#### plane\_sweep

```python
def plane_sweep(obj1: Intervals | PointsInTime,
                obj2: Intervals | PointsInTime) -> list[tuple[int, int]]
```

Find intersections between intervals or points in time.

**Arguments**:

- `obj1` (`Intervals or PointsInTime`): First object to check for intersection.
- `obj2` (`Intervals or PointsInTime`): Second object to check for intersection.

**Raises**:

- `NotImplementedError`: If argument types are not Intervals or PointsInTime.

**Returns**:

`list of tuple`: List of index pairs indicating intersections.

#### empty

```python
def empty()
```

Returns an Intervals object with no intervals.

**Returns**:

`Intervals`: Empty Intervals object.

