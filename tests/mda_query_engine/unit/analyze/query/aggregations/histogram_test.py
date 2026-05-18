from pyspark.sql.types import StructType

from mda_query_engine.analyze.metadata.tag_expression import TagSelector
from mda_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesSelector,
)
from mda_query_engine.analyze.query.aggregations.histogram import (
    HistogramCustomWeights,
    HistogramDuration,
)


def test_histogram_str():
    """Test the string representation of the Histogram class."""

    selector = TimeSeriesSelector(TagSelector("name") == "test")
    hist = HistogramDuration(
        selection=selector, bins=[0.0, 1.0, 2.0], aggregation_level="container"
    )
    expected_str = (
        "<Histogram selection=TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, "
        "bins=[0.0, 1.0, 2.0], aggregation_level=container>"
    )
    assert str(hist) == expected_str

    # Attribute assertions for HistogramDuration
    assert hist.selection == selector
    assert hist.bins == [0.0, 1.0, 2.0]
    assert hist.aggregation_level == "container"

    # Method existence and return type assertions for HistogramDuration
    assert hasattr(hist, "dtype")
    assert callable(hist.dtype)
    dtype_result = hist.dtype()
    assert isinstance(dtype_result, StructType)

    assert hasattr(hist, "build")
    assert callable(hist.build)

    assert hasattr(hist, "required_tags")
    assert callable(hist.required_tags)
    required_tags_result = hist.required_tags()
    assert isinstance(required_tags_result, set)

    assert hasattr(hist, "get_selector_expr")
    assert callable(hist.get_selector_expr)
    selector_expr_result = hist.get_selector_expr()
    assert selector_expr_result is not None


def test_histogram_custom_weight_str():
    """Test the string representation of the Histogram class."""

    selection = TimeSeriesSelector(TagSelector("name") == "test")
    weight = TimeSeriesSelector(TagSelector("name") == "test")
    hist = HistogramCustomWeights(selection=selection, weights=weight, bins=[0.0, 1.0, 2.0])
    expected_str = "<Histogram channel=TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, weights=TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, bins=[0.0, 1.0, 2.0], channel_interp_kind=previous, weights_interp_kind=previous, math_fct_for_weights=None, math_fct_kwargs=None>"
    assert str(hist) == expected_str

    # Attribute assertions for HistogramCustomWeights
    assert hist.selection == selection
    assert hist.weights == weight
    assert hist.bins == [0.0, 1.0, 2.0]
    assert hist.channel_interp_kind == "previous"
    assert hist.weights_interp_kind == "previous"
    assert hist.math_fct_for_weights is None
    assert hist.math_fct_kwargs is None

    # Method existence and return type assertions for HistogramCustomWeights
    assert hasattr(hist, "dtype")
    assert callable(hist.dtype)
    dtype_result = hist.dtype()
    assert isinstance(dtype_result, StructType)

    assert hasattr(hist, "build")
    assert callable(hist.build)

    assert hasattr(hist, "required_tags")
    assert callable(hist.required_tags)
    required_tags_result = hist.required_tags()
    assert isinstance(required_tags_result, set)

    assert hasattr(hist, "get_selector_expr")
    assert callable(hist.get_selector_expr)
    selector_expr_result = hist.get_selector_expr()
    assert selector_expr_result is not None


def test_histogram_custom_weights_get_selectors():
    sel = TimeSeriesSelector(TagSelector("name") == "speed")
    wgt = TimeSeriesSelector(TagSelector("name") == "torque")
    hist = HistogramCustomWeights(selection=sel, weights=wgt, bins=[0.0, 1.0])
    result = hist.get_selectors()
    assert len(result) == 2
    assert sel in result
    assert wgt in result


def test_histogram_duration_get_selectors():
    sel = TimeSeriesSelector(TagSelector("name") == "speed")
    hist = HistogramDuration(selection=sel, bins=[0.0, 50.0, 100.0])
    result = hist.get_selectors()
    assert result == [sel]


def test_histogram_duration_get_selectors_nested():
    sel_a = TimeSeriesSelector(TagSelector("name") == "a")
    sel_b = TimeSeriesSelector(TagSelector("name") == "b")
    hist = HistogramDuration(selection=sel_a + sel_b, bins=[0.0, 1.0])
    result = hist.get_selectors()
    assert len(result) == 2
    assert sel_a in result
    assert sel_b in result
