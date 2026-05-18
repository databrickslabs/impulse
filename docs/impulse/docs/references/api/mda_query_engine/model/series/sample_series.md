---
sidebar_label: sample_series
title: mda_query_engine.model.series.sample_series
---

SampleSeries class implementation


## SampleSeries

```python
class SampleSeries()
```

#### \_\_init\_\_

```python
def __init__(tstarts: Sized, tends: Sized, values: Sized)
```

Initialize the SampleSeries object.

**Arguments**:

- `tstarts` (`Sized`): Array-like of interval start times.
- `tends` (`Sized`): Array-like of interval end times.
- `values` (`Sized`): Array-like of sample values.

#### dtype

```python
def dtype()
```

Returns the Spark data type for SampleSeries.

**Returns**:

`pyspark.sql.types.BinaryType`: Spark BinaryType for serialized SampleSeries.

#### get\_data

```python
def get_data() -> list
```

Returns the series as a list of [tstart, tend, value] lists.

**Returns**:

`list`: List of [tstart, tend, value] triples.

#### sparse

```python
def sparse() -> SampleSeries
```

Returns a sparse version of this SampleSeries, merging consecutive samples with the same value.

**Returns**:

`SampleSeries`: New sparse SampleSeries.

#### \_\_len\_\_

```python
def __len__() -> int
```

Returns the number of samples in the series.

**Returns**:

`int`: Number of samples.

#### \_\_add\_\_

```python
def __add__(other: SampleSeries | float) -> SampleSeries
```

Add another SampleSeries or scalar to this series.

**Arguments**:

- `other` (`SampleSeries or float`): Operand to add.

**Returns**:

`SampleSeries`: Resulting SampleSeries.

#### \_\_radd\_\_

```python
def __radd__(other: SampleSeries | float) -> SampleSeries
```

Add this series to another SampleSeries or scalar (reversed operands).

**Arguments**:

- `other` (`SampleSeries or float`): Operand to add.

**Returns**:

`SampleSeries`: Resulting SampleSeries.

#### \_\_sub\_\_

```python
def __sub__(other: SampleSeries | float) -> SampleSeries
```

Subtract another SampleSeries or scalar from this series.

**Arguments**:

- `other` (`SampleSeries or float`): Operand to subtract.

**Returns**:

`SampleSeries`: Resulting SampleSeries.

#### \_\_rsub\_\_

```python
def __rsub__(other: SampleSeries | float) -> SampleSeries
```

Subtract this series from another SampleSeries or scalar (reversed operands).

**Arguments**:

- `other` (`SampleSeries or float`): Operand to subtract.

**Returns**:

`SampleSeries`: Resulting SampleSeries.

#### \_\_mul\_\_

```python
def __mul__(other: SampleSeries | float) -> SampleSeries
```

Multiply this series by another SampleSeries or scalar.

**Arguments**:

- `other` (`SampleSeries or float`): Operand to multiply.

**Returns**:

`SampleSeries`: Resulting SampleSeries.

#### \_\_rmul\_\_

```python
def __rmul__(other: SampleSeries | float) -> SampleSeries
```

Multiply another SampleSeries or scalar by this series (reversed operands).

**Arguments**:

- `other` (`SampleSeries or float`): Operand to multiply.

**Returns**:

`SampleSeries`: Resulting SampleSeries.

#### \_\_truediv\_\_

```python
def __truediv__(other: SampleSeries | float) -> SampleSeries
```

Divide this series by another SampleSeries or scalar.

**Arguments**:

- `other` (`SampleSeries or float`): Operand to divide.

**Returns**:

`SampleSeries`: Resulting SampleSeries.

#### \_\_rtruediv\_\_

```python
def __rtruediv__(other: SampleSeries | float) -> SampleSeries
```

Divide another SampleSeries or scalar by this series (reversed operands).

**Arguments**:

- `other` (`SampleSeries or float`): Operand to divide.

**Returns**:

`SampleSeries`: Resulting SampleSeries.

#### \_\_mod\_\_

```python
def __mod__(other: int | float | SampleSeries) -> Intervals
```

Return the modulus of this series and another SampleSeries or scalar.

**Arguments**:

- `other` (`int, float, or SampleSeries`): Operand for modulus.

**Returns**:

`SampleSeries`: Resulting SampleSeries.

#### \_\_rmod\_\_

```python
def __rmod__(other: int | float | SampleSeries) -> Intervals
```

Return the modulus of another SampleSeries or scalar and this series.

**Arguments**:

- `other` (`int, float, or SampleSeries`): Operand for modulus (reversed operands).

**Returns**:

`SampleSeries`: Resulting SampleSeries.

#### \_\_gt\_\_

```python
def __gt__(other: int | float | SampleSeries) -> Intervals
```

Return intervals where this series is greater than another.

**Arguments**:

- `other` (`int, float, or SampleSeries`): Operand for comparison.

**Returns**:

`Intervals`: Intervals where condition holds.

#### \_\_ge\_\_

```python
def __ge__(other: int | float | SampleSeries) -> Intervals
```

Return intervals where this series is greater than or equal to another.

**Arguments**:

- `other` (`int, float, or SampleSeries`): Operand for comparison.

**Returns**:

`Intervals`: Intervals where condition holds.

#### \_\_lt\_\_

```python
def __lt__(other: int | float | SampleSeries) -> Intervals
```

Return intervals where this series is less than another.

**Arguments**:

- `other` (`int, float, or SampleSeries`): Operand for comparison.

**Returns**:

`Intervals`: Intervals where condition holds.

#### \_\_le\_\_

```python
def __le__(other: int | float | SampleSeries) -> Intervals
```

Return intervals where this series is less than or equal to another.

**Arguments**:

- `other` (`int, float, or SampleSeries`): Operand for comparison.

**Returns**:

`Intervals`: Intervals where condition holds.

#### \_\_eq\_\_

```python
def __eq__(other: int | float | SampleSeries) -> Intervals
```

Return intervals where this series is equal to another.

**Arguments**:

- `other` (`int, float, or SampleSeries`): Operand for comparison.

**Returns**:

`Intervals`: Intervals where condition holds.

#### \_\_ne\_\_

```python
def __ne__(other: int | float | SampleSeries) -> Intervals
```

Return intervals where this series is not equal to another.

**Arguments**:

- `other` (`int, float, or SampleSeries`): Operand for comparison.

**Returns**:

`Intervals`: Intervals where condition holds.

#### sample\_count

```python
def sample_count() -> int
```

Returns the number of samples in this SampleSeries.

**Returns**:

`int`: Number of samples.

#### unique\_times

```python
def unique_times() -> npt.NDArray
```

Returns a sorted array of all unique start and end times.

**Returns**:

`numpy.ndarray`: Array of unique times.

#### start\_time

```python
def start_time() -> FloatOrNaN
```

Returns the start time of the first sample.

**Returns**:

`float`: Start time or NaN if empty.

#### end\_time

```python
def end_time() -> FloatOrNaN
```

Returns the end time of the last sample.

**Returns**:

`float`: End time or NaN if empty.

#### duration\_ms

```python
def duration_ms() -> FloatOrNaN
```

Returns the total duration in milliseconds.

**Returns**:

`float`: Duration in milliseconds.

#### nan\_ratio

```python
def nan_ratio() -> FloatOrNaN
```

Returns the ratio of NaN samples to all samples, weighted by duration.

**Returns**:

`float`: Ratio of NaN durations to total duration.

#### durations

```python
def durations() -> npt.NDArray
```

Returns an array of durations for all samples.

**Returns**:

`numpy.ndarray`: Array of durations (in seconds).

#### sample\_rate

```python
def sample_rate() -> FloatOrNaN
```

Returns the average sample rate (mean duration).

**Returns**:

`float`: Average sample rate or NaN if empty.

#### sum

```python
def sum() -> FloatOrNaN
```

Returns the sum of all values, weighted by duration.

**Returns**:

`float`: Weighted sum of values.

#### min

```python
def min() -> FloatOrNaN
```

Returns the minimum value in the series.

**Returns**:

`float`: Minimum value or NaN if empty.

#### max

```python
def max() -> FloatOrNaN
```

Returns the maximum value in the series.

**Returns**:

`float`: Maximum value or NaN if empty.

#### mean

```python
def mean() -> FloatOrNaN
```

Returns the mean value, weighted by durations.

**Returns**:

`float`: Weighted mean value or NaN if empty.

#### rising\_edges

```python
def rising_edges() -> PointsInTime
```

Returns points in time where the value rises compared to the previous sample.

**Returns**:

`PointsInTime`: Points where rising edges occur.

#### falling\_edges

```python
def falling_edges() -> PointsInTime
```

Returns points in time where the value falls compared to the previous sample.

**Returns**:

`PointsInTime`: Points where falling edges occur.

#### rising\_edge

```python
def rising_edge() -> PointsInTime
```

Alias for rising_edges().

**Returns**:

`PointsInTime`: Points where rising edges occur.

#### falling\_edge

```python
def falling_edge() -> PointsInTime
```

Alias for falling_edges().

**Returns**:

`PointsInTime`: Points where falling edges occur.

#### intervals\_between\_falling\_edges

```python
def intervals_between_falling_edges() -> Intervals
```

Build intervals [tstart, tend] from falling edges of the series.

Processes each continuous interval of the series separately.
Within each continuous block, each interval starts at a falling
edge and ends at the timestamp before the next falling edge.
The first interval in a block starts at the block's first timestamp;
the last interval in a block ends at the block's last timestamp.
Blocks with no falling edges contribute no intervals.

**Returns**:

`Intervals`: Intervals between consecutive falling edges.

#### diff

```python
def diff() -> "SampleSeries"
```

Calculate the difference between consecutive values.

**Returns**:

`SampleSeries`: New SampleSeries with difference values, preserving original timestamps.
The first value of each continuous segment is 0 (no previous value to diff from).

#### histogram

```python
def histogram(bins: npt.ArrayLike = None,
              weights: SampleSeries = None,
              weight_type: str = None) -> tuple[npt.NDArray, npt.NDArray]
```

Compute a histogram of the sample values using the specified bins.

**Arguments**:

- `bins` (`array_like`): Bin edges for the histogram. If None, uses [-np.inf, np.inf].
- `weights` (`SampleSeries`): Custom weights series. If None, uses sample durations as weights.
- `weight_type` (`str`): Type of weighting to use. Options:
- None (default): Use weights values directly (or durations if weights is None)
- 'time': Multiply weights values by their durations

**Returns**:

`ndarray`: The values of the histogram.

#### histogram2d

```python
def histogram2d(
        y_series: SampleSeries,
        x_bins: npt.ArrayLike,
        y_bins: npt.ArrayLike,
        weights: SampleSeries = None,
        weight_type: str = None
) -> tuple[npt.NDArray, npt.NDArray, npt.NDArray]
```

Compute a bi-dimensional histogram of the sample values using the specified x and y bins.

**Arguments**:

- `y_series` (`SampleSeries`): The second sample series for the y-axis.
- `x_bins` (`array_like`): Bin edges for the x-axis.
- `y_bins` (`array_like`): Bin edges for the y-axis.
- `weights` (`SampleSeries`): Custom weights series. If None, uses sample durations as weights.
- `weight_type` (`str`): Type of weighting to use. Options:
- None (default): Use weights values directly (or durations if weights is None)
- 'time': Multiply weights values by their durations

**Returns**:

`ndarray`: The 2D histogram array.

#### synchronized

```python
def synchronized(other: SampleSeries)
```

Synchronize this series with another, aligning intervals.

**Arguments**:

- `other` (`SampleSeries`): Series to synchronize with.

**Returns**:

`tuple of SampleSeries`: Synchronized SampleSeries objects.

#### synchronized\_all

```python
def synchronized_all(others: list[SampleSeries])
```

Synchronize this series with multiple other SampleSeries.

**Arguments**:

- `others` (`list of SampleSeries`): List of series to synchronize.

**Returns**:

`tuple of SampleSeries`: Synchronized SampleSeries objects.

#### where

```python
def where(other: Intervals) -> SampleSeries
```

Returns a SampleSeries where the given intervals are defined.

**Arguments**:

- `other` (`Intervals`): Intervals to filter by.

**Returns**:

`SampleSeries`: Filtered SampleSeries.

#### resample

```python
def resample(sample_rate=1.0)
```

Resample the series at a given sample rate.

**Arguments**:

- `sample_rate` (`float`): Desired sample rate (default 1.0).

**Returns**:

`SampleSeries`: Resampled SampleSeries.

#### rolling\_average

```python
def rolling_average(window_size=1.0, evenly_spaced=False) -> SampleSeries
```

Compute rolling average over a window.

**Arguments**:

- `window_size` (`float`): Size of the rolling window (default 1.0).
- `evenly_spaced` (`bool`): If True, use evenly spaced weights (default False).

**Returns**:

`SampleSeries`: Rolling average SampleSeries.

#### rolling\_stats

```python
def rolling_stats(
        window_size=1.0,
        evenly_spaced=False
) -> tuple[SampleSeries, SampleSeries, SampleSeries]
```

Compute rolling min, max, and average over a window.

**Arguments**:

- `window_size` (`float`): Size of the rolling window (default 1.0).
- `evenly_spaced` (`bool`): If True, use evenly spaced weights (default False).

**Returns**:

`SampleSeries`: Rolling minimum values.

#### trapz

```python
def trapz() -> float
```

Perform discrete integration using the composite trapezoidal rule.

**Returns**:

`float`: Integrated value.

#### cumtrapz

```python
def cumtrapz() -> SampleSeries
```

Perform cumulative discrete integration using the trapezoidal rule.

**Returns**:

`SampleSeries`: Cumulative integrated SampleSeries.

#### \_\_str\_\_

```python
def __str__() -> str
```

Returns a string representation of the SampleSeries.

**Returns**:

`str`: String representation.

#### \_\_repr\_\_

```python
def __repr__() -> str
```

Returns a string representation for debugging.

**Returns**:

`str`: String representation.

#### serialize

```python
def serialize()
```

Serialize and compress the SampleSeries.

**Returns**:

`bytes`: Compressed serialized data.

#### deserialize

```python
def deserialize(d)
```

Deserialize a compressed SampleSeries.

**Arguments**:

- `d` (`bytes`): Compressed serialized data.

**Returns**:

`SampleSeries`: Deserialized SampleSeries object.

#### to\_pickle

```python
def to_pickle(uri: str)
```

Write the SampleSeries to disk at the given URI.

**Arguments**:

- `uri` (`str`): File path to write to.

#### from\_pickle

```python
def from_pickle(uri: str)
```

Read a pickled SampleSeries from disk.

**Arguments**:

- `uri` (`str`): File path to read from.

**Returns**:

`SampleSeries`: Loaded SampleSeries object.

#### from\_timestamps

```python
def from_timestamps(times, values)
```

Create a SampleSeries from timestamps and values.

**Arguments**:

- `times` (`array-like`): Array of timestamps.
- `values` (`array-like`): Array of values.

**Returns**:

`SampleSeries`: Constructed SampleSeries.

#### empty

```python
def empty() -> SampleSeries
```

Returns an empty SampleSeries.

**Returns**:

`SampleSeries`: Empty SampleSeries object.

