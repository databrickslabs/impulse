from pyspark.sql.types import StructType

from mda_query_engine.analyze.metadata.tag_expression import TagSelector
from mda_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from mda_query_engine.analyze.query.aggregations.histogram2d import (
    Histogram2DCustomWeights,
    Histogram2DDuration,
)


def test_histogram2d_str():
    """Test the string representation of the Histogram class."""

    selector = TimeSeriesSelector(TagSelector("name") == "test")
    hist = Histogram2DDuration(
        x_selection=selector,
        y_selection=selector,
        x_bins=[0.0, 1.0, 2.0],
        y_bins=[0.0, 1.0, 2.0],
    )
    expected_str = (
        "<Histogram2D x_selection=TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, "
        "y_selection=TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, "
        "x_bins=[0.0, 1.0, 2.0], y_bins=[0.0, 1.0, 2.0]>"
    )
    assert str(hist) == expected_str

    # Attribute assertions for Histogram2DDuration
    assert hist.x_selection == selector
    assert hist.y_selection == selector
    assert hist.x_bins == [0.0, 1.0, 2.0]
    assert hist.y_bins == [0.0, 1.0, 2.0]

    # Method existence and return type assertions for Histogram2DDuration
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


def test_histogram2d_custom_weights_str():

    selector = TimeSeriesSelector(TagSelector("name") == "test")
    weights_selector = TimeSeriesSelector(TagSelector("name") == "weight")
    hist = Histogram2DCustomWeights(
        x_selection=selector,
        y_selection=selector,
        weights_expr=weights_selector,
        x_bins=[0.0, 1.0, 2.0],
        y_bins=[0.0, 1.0, 2.0],
    )
    expected_str = (
        "<Histogram2D x_selection=TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, "
        "y_selection=TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, weights_expr=TimeSeriesSelector<TagOp<eq(TagSelector<name>,weight)>>, "
        "x_bins=[0.0, 1.0, 2.0], y_bins=[0.0, 1.0, 2.0]>"
    )
    assert str(hist) == expected_str

    # Attribute assertions for Histogram2DCustomWeights
    assert hist.x_selection == selector
    assert hist.y_selection == selector
    assert hist.weights_expr == weights_selector
    assert hist.x_bins == [0.0, 1.0, 2.0]
    assert hist.y_bins == [0.0, 1.0, 2.0]

    # Method existence and return type assertions for Histogram2DCustomWeights
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
