---
sidebar_label: histogram
title: impulse_reporting.aggregations.histogram
---

## HistogramDuration

```python
class HistogramDuration(Histogram)
```

Class representing a histogram duration aggregation in a report.


#### \_\_init\_\_

```python
def __init__(name: str,
             base_expr: TimeSeriesExpression,
             bins: list[float],
             event: Event | None = None,
             desc: str = None,
             agg_type: str = "histogram_duration",
             channel_name: str = None,
             values_unit: str = None,
             bins_unit: str = None)
```

Initialize a HistogramDuration object.

**Arguments**:

- `name` (`str`): Name of the histogram duration aggregation.
- `base_expr` (`TimeSeriesExpression`): Base time series expression to compute the histogram duration from.
- `bins` (`list of float`): List of bin edges for the histogram duration.
- `event` (`Event`): Optional event to filter the base expression.
- `desc` (`str`): Description of the histogram duration.
- `agg_type` (`str`): Type of aggregation, defaults to "histogram_duration".
- `channel_name` (`str`): Name of the signal associated with the histogram duration.
- `values_unit` (`str`): Unit of the histogram duration values.
- `bins_unit` (`str`): Unit of the histogram duration bins.

## HistogramCustomWeights

```python
class HistogramCustomWeights(Histogram)
```

Class representing a histogram with a custom weight in a report.


#### \_\_init\_\_

```python
def __init__(name: str,
             base_expr: TimeSeriesExpression,
             weights_expr: TimeSeriesExpression,
             bins: list[float],
             event: Event | None = None,
             desc: str = None,
             agg_type: str = "histogram_custom_weights",
             channel_name: str = None,
             weights_channel_name: str = None,
             values_unit: str = None,
             bins_unit: str = None,
             channel_interp_kind: str = "previous",
             weights_interp_kind: str = "previous",
             math_fct_for_weights: str = None,
             math_fct_kwargs: dict = None,
             weight_type: str = None)
```

Initialize a HistogramCustomWeights object.

**Arguments**:

- `name` (`str`): Name of the custom histogram aggregation.
- `base_expr` (`TimeSeriesExpression`): Base time series expression to compute the histogram from.
- `weights_expr` (`TimeSeriesExpression`): Time series expression to use as custom weights for the histogram.
- `bins` (`list of float`): List of bin edges for the histogram.
- `event` (`Event`): Optional event to filter the base expression.
- `desc` (`str`): Description of the histogram.
- `agg_type` (`str`): Type of aggregation, defaults to "histogram_custom_weights".
- `channel_name` (`str`): Name of the signal associated with the histogram.
- `weights_channel_name` (`str`): Name of the weights signal associated with the histogram.
- `values_unit` (`str`): Unit of the histogram values.
- `bins_unit` (`str`): Unit of the histogram bins.
- `channel_interp_kind` (`str`): Interpolation method for the channel values (default is 'previous').
- `weights_interp_kind` (`str`): Interpolation method for the weights (default is 'previous').
- `math_fct_for_weights` (`callable`): Optional function to apply to the weights before aggregation.
- `math_fct_kwargs` (`dict`): Additional keyword arguments to pass to the math function for weights (default is {}).
- `weight_type` (`str`): If the custom weighted signal is required to be weighted with time, it must be provided as 'time'.
By default it is set to None, this option is provided to prevent errors from RLE compression method of the channels.

#### as\_dict

```python
def as_dict() -> dict
```

Get a dictionary representation of the histogram aggregation.

**Returns**:

`dict`: Dictionary containing histogram aggregation metadata.

