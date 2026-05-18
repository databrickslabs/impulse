---
sidebar_label: histogram2d
title: mda_reporting.aggregations.histogram2d
---

## Histogram2DDuration

```python
class Histogram2DDuration(Histogram2D)
```

Class representing a 2D histogram aggregation in a report.

This class uses duration series as the weight of the histogram.


#### \_\_init\_\_

```python
def __init__(name: str,
             x_expr: TimeSeriesExpression,
             y_expr: TimeSeriesExpression,
             x_bins: list[float],
             y_bins: list[float],
             event: Event | None = None,
             desc: str = None,
             agg_type: str = "histogram_duration",
             x_channel_name: str = None,
             y_channel_name: str = None,
             values_unit: str = None,
             x_bins_unit: str = None,
             y_bins_unit: str = None)
```

Initialize a Histogram2D object.

**Arguments**:

- `name` (`str`): Name of the histogram aggregation.
- `x_expr` (`TimeSeriesExpression`): Time series expression for the x-axis.
- `y_expr` (`TimeSeriesExpression`): Time series expression for the y-axis.
- `x_bins` (`list of float`): List of bin edges for the x-axis.
- `y_bins` (`list of float`): List of bin edges for the y-axis.
- `event` (`Event`): Optional event to filter the expressions.
- `desc` (`str`): Description of the histogram.
- `agg_type` (`str`): Type of aggregation.
- `x_channel_name` (`str`): Name of the signal associated with the x-axis.
- `y_channel_name` (`str`): Name of the signal associated with the y-axis.
- `values_unit` (`str`): Unit of the histogram values.
- `x_bins_unit` (`str`): Unit of the x-axis bins.
- `y_bins_unit` (`str`): Unit of the y-axis bins.

## Histogram2DCustomWeights

```python
class Histogram2DCustomWeights(Histogram2D)
```

Class representing a 2D histogram aggregation with custom weights in a report.


#### \_\_init\_\_

```python
def __init__(name: str,
             x_expr: TimeSeriesExpression,
             y_expr: TimeSeriesExpression,
             weights_expr: TimeSeriesExpression,
             x_bins: list[float],
             y_bins: list[float],
             event: Event | None = None,
             desc: str = None,
             agg_type: str = "histogram2d_custom_weights",
             x_channel_name: str = None,
             y_channel_name: str = None,
             weights_channel_name: str = None,
             values_unit: str = None,
             x_bins_unit: str = None,
             y_bins_unit: str = None,
             channel_interp_kind: str = "previous",
             weights_interp_kind: str = "previous",
             math_fct_for_weights: str = None,
             math_fct_kwargs: dict = None,
             weight_type: str = None)
```

Initialize a Histogram2DCustomWeights object.

**Arguments**:

- `name` (`str`): Name of the histogram aggregation.
- `x_expr` (`TimeSeriesExpression`): Time series expression for the x-axis.
- `y_expr` (`TimeSeriesExpression`): Time series expression for the y-axis.
- `weights_expr` (`TimeSeriesExpression`): Time series expression for the weights.
- `x_bins` (`list of float`): List of bin edges for the x-axis.
- `y_bins` (`list of float`): List of bin edges for the y-axis.
- `event` (`Event`): Optional event to filter the expressions.
- `desc` (`str`): Description of the histogram.
- `agg_type` (`str`): Type of aggregation, defaults to 'histogram2d_custom_weights'.
- `x_channel_name` (`str`): Name of the signal associated with the x-axis.
- `y_channel_name` (`str`): Name of the signal associated with the y-axis.
- `weights_channel_name` (`str`): Name of the signal associated with the weights.
- `values_unit` (`str`): Unit of the histogram values.
- `x_bins_unit` (`str`): Unit of the x-axis bins.
- `y_bins_unit` (`str`): Unit of the y-axis bins.
- `channel_interp_kind` (`str`): Interpolation method for the channel values, defaults to 'previous'.
- `weights_interp_kind` (`str`): Interpolation method for the weights, defaults to 'previous'.
- `math_fct_for_weights` (`str`): Optional function name to apply to the weights before aggregation.
Example: 'diff' to compute the difference of consecutive weight values.
- `math_fct_kwargs` (`dict`): Additional keyword arguments to pass to the math function for weights,
defaults to an empty dictionary.
- `weight_type` (`str`): If the custom weighted signal is required to be weighted with time, it must be provided as 'time'.
By default it is set to None, this option is provided to prevent errors from RLE compression method of the channels.

#### as\_dict

```python
def as_dict() -> dict
```

Get a dictionary representation of the histogram aggregation.

**Returns**:

`dict`: Dictionary containing histogram aggregation metadata.

## Histogram2DDistance

```python
class Histogram2DDistance(Histogram2DCustomWeights)
```

Class representing a 2D histogram aggregation weighted by distance.

This class extends Histogram2DCustomWeights to compute a 2D histogram
where the weights are derived from the difference of consecutive weight
values (using the 'diff' math function), typically representing distance.


#### \_\_init\_\_

```python
def __init__(name: str,
             x_expr: TimeSeriesExpression,
             y_expr: TimeSeriesExpression,
             weights_expr: TimeSeriesExpression,
             x_bins: list[float],
             y_bins: list[float],
             event: Event | None = None,
             desc: str = None,
             x_channel_name: str = None,
             y_channel_name: str = None,
             values_unit: str = None,
             x_bins_unit: str = None,
             y_bins_unit: str = None,
             channel_interp_kind: str = "previous",
             weights_interp_kind: str = "previous",
             math_fct_kwargs: dict = None)
```

Initialize a Histogram2DDistance object.

**Arguments**:

- `name` (`str`): Name of the histogram aggregation.
- `x_expr` (`TimeSeriesExpression`): Time series expression for the x-axis.
- `y_expr` (`TimeSeriesExpression`): Time series expression for the y-axis.
- `weights_expr` (`TimeSeriesExpression`): Time series expression for the weights (e.g., cumulative distance).
- `x_bins` (`list of float`): List of bin edges for the x-axis.
- `y_bins` (`list of float`): List of bin edges for the y-axis.
- `event` (`Event`): Optional event to filter the expressions.
- `desc` (`str`): Description of the histogram.
- `x_channel_name` (`str`): Name of the signal associated with the x-axis.
- `y_channel_name` (`str`): Name of the signal associated with the y-axis.
- `values_unit` (`str`): Unit of the histogram values.
- `x_bins_unit` (`str`): Unit of the x-axis bins.
- `y_bins_unit` (`str`): Unit of the y-axis bins.
- `channel_interp_kind` (`str`): Interpolation method for the channel values, defaults to 'previous'.
- `weights_interp_kind` (`str`): Interpolation method for the weights, defaults to 'previous'.
- `math_fct_kwargs` (`dict`): Additional keyword arguments to pass to the 'diff' math function,
defaults to an empty dictionary.

