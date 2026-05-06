import pyspark.sql.types as T
import pytest
from pyspark.errors import AnalysisException

from mda_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from mda_query_engine.analyze.query.solvers.basic_narrow_solver import BasicNarrowSolver
from mda_reporting.aggregations.histogram import Histogram
from mda_reporting.aggregations.histogram2d import (
    Histogram2DCustomWeights,
    Histogram2DDistance,
    Histogram2DDuration,
)
from mda_reporting.events.basic_event import BasicEvent
from tests.conftest import basic_narrow_db, spark


def test_as_spark_row():
    ts_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    hist = Histogram2DDuration(
        name="my_hist_1", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins
    )
    row = hist.as_spark_row()
    assert len(row) == 18  # hash is added so number is increased to 18


def test_histogram2d_init():
    """Test Histogram2D initialization with required parameters"""
    base_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0, 3.0]
    hist = Histogram2DDuration(
        name="test_hist", x_expr=base_expr, y_expr=base_expr, x_bins=bins, y_bins=bins
    )

    assert hist.name == "test_hist"
    assert hist.page_number == -1  # Default value
    assert hist.x_expr == base_expr
    assert hist.y_expr == base_expr
    assert hist.x_bins == bins
    assert hist.y_bins == bins
    assert hist.event is None
    assert hist.desc is None
    assert hist.agg_type == "histogram_duration"
    assert hist.values_unit is None
    assert hist.x_bins_unit is None
    assert hist.y_bins_unit is None


def test_histogram_init_with_optional_params():
    """Test Histogram2D initialization with all optional parameters"""
    base_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)
    bins = [0.0, 5.0, 10.0]
    hist = Histogram2DDuration(
        name="test_hist",
        x_expr=base_expr,
        y_expr=base_expr,
        x_bins=bins,
        y_bins=bins,
        event=basic_event,
        desc="Test histogram",
        agg_type="frequency",
        values_unit="Hz",
        x_bins_unit="rpm",
        y_bins_unit="rpm",
    )

    assert hist.name == "test_hist"
    assert hist.x_expr == base_expr
    assert hist.y_expr == base_expr
    assert hist.x_bins == bins
    assert hist.y_bins == bins
    assert hist.event == basic_event
    assert hist.desc == "Test histogram"
    assert hist.agg_type == "frequency"
    assert hist.values_unit == "Hz"
    assert hist.x_bins_unit == "rpm"
    assert hist.y_bins_unit == "rpm"


def test_get_name():
    """Test get_name method"""
    ts_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    hist = Histogram2DDuration(
        name="my_histogram", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins
    )
    assert hist.get_name() == "my_histogram"


def test_get_expression():
    """Test get_expression method returns TimeSeriesExpression"""
    ts_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    hist = Histogram2DDuration(
        name="my_histogram", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins
    )
    expression = hist.get_expression()
    assert expression is not None
    # Expression should be set during initialization
    assert hasattr(expression, "__str__")


def test_get_expression_with_event_expr():
    """Test expression creation when event is provided"""
    ts_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    hist = Histogram2DDuration(
        name="my_histogram",
        x_expr=ts_expr,
        y_expr=ts_expr,
        x_bins=bins,
        y_bins=bins,
        event=basic_event,
    )
    expression = hist.get_expression()
    assert expression is not None


def test_get_expression_str():
    """Test get_expression_str method"""
    ts_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    hist = Histogram2DDuration(
        name="my_histogram", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins
    )
    expr_str = hist.get_expression_str()
    assert isinstance(expr_str, str)
    assert expr_str != "NA"


def test_as_dict():
    """Test as_dict method"""
    ts_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    hist = Histogram2DDuration(
        name="my_histogram", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins
    )

    hist_dict = hist.as_dict()
    assert isinstance(hist_dict, dict)
    assert hist_dict["name"] == "my_histogram"
    assert hist_dict["page_number"] == -1  # Default value
    assert hist_dict["x_bins"] == [0.0, 1.0, 2.0]
    assert hist_dict["y_bins"] == [0.0, 1.0, 2.0]
    assert hist_dict["x_signal_expression"] == ts_expr.get_expression_str()
    assert hist_dict["y_signal_expression"] == ts_expr.get_expression_str()
    assert hist_dict["description"] is None
    assert hist_dict["agg_type"] == "histogram_duration"
    assert hist_dict["values_unit"] is None
    assert hist_dict["values_unit"] is None
    assert hist_dict["x_bins_unit"] is None
    assert hist_dict["y_bins_unit"] is None


def test_as_spark_row_complete():
    """Test as_spark_row with all fields populated"""
    base_expr = TimeSeriesSelector(None)
    bins = [0.0, 10.0, 20.0, 30.0]

    hist = Histogram2DDuration(
        name="complete_hist",
        x_expr=base_expr,
        y_expr=base_expr,
        x_bins=bins,
        y_bins=bins,
        desc="Complete histogram test",
        agg_type="distribution",
        values_unit="count",
        x_bins_unit="value",
        y_bins_unit="value",
    )

    row = hist.as_spark_row()
    assert row.name == "complete_hist"
    assert row.description == "Complete histogram test"
    assert row.agg_type == "distribution"
    assert row.x_bins == bins
    assert row.y_bins == bins
    assert row.values_unit == "count"
    assert row.x_bins_unit == "value"
    assert row.y_bins_unit == "value"
    assert isinstance(row.x_signal_expression, str)
    assert isinstance(row.y_signal_expression, str)


def test_as_spark_row_minimal():
    """Test as_spark_row with minimal required fields"""
    base_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0]
    hist = Histogram2DDuration(
        name="minimal_hist",
        x_expr=base_expr,
        y_expr=base_expr,
        x_bins=bins,
        y_bins=bins,
    )

    row = hist.as_spark_row()
    assert row.name == "minimal_hist"
    assert row.description is None
    assert row.agg_type == "histogram_duration"
    assert row.x_bins == bins
    assert row.y_bins == bins
    assert row.values_unit is None
    assert row.x_bins_unit is None
    assert row.y_bins_unit is None
    assert isinstance(row.x_signal_expression, str)
    assert isinstance(row.y_signal_expression, str)


def test_bins_validation():
    """Test that bins parameter accepts different numeric types"""
    base_expr = TimeSeriesSelector(None)
    bins = [0, 1, 2, 3]
    # Test with integers
    hist1 = Histogram2DDuration(
        name="test1", x_expr=base_expr, y_expr=base_expr, x_bins=bins, y_bins=bins
    )
    assert hist1.x_bins == [0, 1, 2, 3]
    assert hist1.y_bins == [0, 1, 2, 3]

    # Test with floats
    bins = [0.0, 1.5, 3.0]
    hist2 = Histogram2DDuration(
        name="test2", x_expr=base_expr, y_expr=base_expr, x_bins=bins, y_bins=bins
    )
    assert hist2.x_bins == [0.0, 1.5, 3.0]
    assert hist2.y_bins == [0.0, 1.5, 3.0]

    # Test with mixed types
    bins = [0, 1.5, 3]
    hist3 = Histogram2DDuration(
        name="test3", x_expr=base_expr, y_expr=base_expr, x_bins=bins, y_bins=bins
    )
    assert hist3.x_bins == bins
    assert hist3.y_bins == bins


def test_determine_aggregations(spark, basic_narrow_db):
    """Test that determine_histogram_visuals returns a DataFrame with expected columns"""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    veh_speed = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")
    event = BasicEvent(name="test_event_1", expr=eng_rpm > 1000)
    bins = [0.0, 10000.0]

    hist = Histogram2DDuration(
        name="test_hist",
        x_expr=eng_rpm,
        y_expr=veh_speed,
        event=event,
        x_bins=bins,
        y_bins=bins,
    )
    solver = BasicNarrowSolver(spark)
    solved_df = basic_narrow_db.query.select(hist.get_expression()).solve(spark, solver)
    df = Histogram2DDuration.determine_aggregations(
        spark=spark, aggregations=[hist], solved_df=solved_df
    )
    assert df.count() > 0  # Ensure that some data is returned
    assert "container_id" in df.columns
    assert "visual_id" in df.columns
    assert "event_id" in df.columns
    assert "x_bin_id" in df.columns
    assert "y_bin_id" in df.columns
    assert "hist_value" in df.columns
    assert "x_lower_bound" in df.columns
    assert "y_lower_bound" in df.columns
    assert "x_upper_bound" in df.columns
    assert "y_upper_bound" in df.columns
    assert "x_bin_name" in df.columns
    assert "y_bin_name" in df.columns


def test_determine_metadata_df(spark, basic_narrow_db):
    """Test that determine_metadata_df returns a DataFrame with expected columns"""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    veh_spd = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")

    x_bins = [0.0, 10000.0]
    y_bins = [0.0, 300.0]

    hist1 = Histogram2DDuration(
        name="test_hist",
        desc="engine rpm histogram",
        x_expr=eng_rpm,
        y_expr=veh_spd,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    hist2 = Histogram2DDuration(
        name="test_hist2", x_expr=eng_rpm, y_expr=veh_spd, x_bins=x_bins, y_bins=y_bins
    )

    df = Histogram2DDuration.determine_metadata_df(spark=spark, histograms=[hist1, hist2])
    assert df.count() == 2  # Ensure that metadata for two histograms is returned
    assert "page_number" in df.columns
    assert "name" in df.columns
    assert "description" in df.columns
    assert "agg_type" in df.columns
    assert "x_bins" in df.columns
    assert "y_bins" in df.columns
    assert "values_unit" in df.columns
    assert "x_bins_unit" in df.columns
    assert "y_bins_unit" in df.columns
    assert "x_signal_expression" in df.columns
    assert "y_signal_expression" in df.columns


def test_get_visual_id_column(spark, basic_narrow_db):
    """Test get_visual_id_column static method with list of aggregations."""

    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    veh_spd = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")

    x_bins = [0, 500, 1000, 1500]
    y_bins = [0, 1000, 2000]

    hist1 = Histogram2DDuration(
        name="hist_1", x_expr=eng_rpm, y_expr=veh_spd, x_bins=x_bins, y_bins=y_bins
    )
    hist2 = Histogram2DDuration(
        name="hist_2", x_expr=eng_rpm, y_expr=veh_spd, x_bins=x_bins, y_bins=y_bins
    )

    visuals = [hist1, hist2]

    col_expr = Histogram.get_visual_id_column(visuals, "hist_name")

    test_data = [("hist_1",), ("hist_2",), ("unknown_hist",)]
    df = spark.createDataFrame(test_data, ["hist_name"])

    result_df = df.withColumn("visual_id", col_expr)
    results = result_df.collect()

    assert results[0]["visual_id"] == hist1.get_id()
    assert results[1]["visual_id"] == hist2.get_id()
    assert results[2]["visual_id"] is None


def test_get_visual_id_column_empty_visuals(spark):
    """Test get_visual_id_column static method with empty visuals list."""
    visuals = []

    col_expr = Histogram2DDuration.get_visual_id_column(visuals, "hist_name")

    test_data = [("any_hist",), ("another_hist",)]
    df = spark.createDataFrame(test_data, ["hist_name"])

    result_df = df.withColumn("visual_id", col_expr)
    results = result_df.collect()

    # All should map to None when visuals list is empty
    assert results[0]["visual_id"] is None
    assert results[1]["visual_id"] is None


def test_unpivot_measurement_info(spark):
    """Test _unpivot_measurement_info method."""
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField(
                "test_histogram",
                T.StructType(
                    [
                        T.StructField("H", T.ArrayType(T.ArrayType(T.DoubleType()))),
                        T.StructField("xedges", T.ArrayType(T.DoubleType())),
                        T.StructField("yedges", T.ArrayType(T.DoubleType())),
                    ]
                ),
            ),
        ]
    )
    data = [
        T.Row(
            container_id=1,
            test_histogram=T.Row(
                H=[[1.0, 2.0], [3.0, 4.0]],
                xedges=[0.0, 1.0, 2.0],
                yedges=[10.0, 20.0, 30.0],
            ),
        )
    ]

    df = spark.createDataFrame(data, schema=schema)

    histogram_names = ["test_histogram"]
    result = Histogram2DDuration._unpivot_measurement_info(histogram_names)(df)
    expected_result = [
        T.Row(
            container_id=1,
            hist_name="test_histogram",
            value=T.Row(
                H=[[1.0, 2.0], [3.0, 4.0]],
                xedges=[0.0, 1.0, 2.0],
                yedges=[10.0, 20.0, 30.0],
            ),
        )
    ]

    assert result.collect() == expected_result

    with pytest.raises(AnalysisException):
        Histogram2DDuration._unpivot_measurement_info(["unknown"])(df)


def test_extract_histogram2d_info(spark):
    """Test _extract_histogram2d_info method."""
    schema = T.StructType(
        [
            T.StructField(
                "value",
                T.StructType(
                    [
                        T.StructField("H", T.ArrayType(T.ArrayType(T.DoubleType()))),
                        T.StructField("xedges", T.ArrayType(T.DoubleType())),
                        T.StructField("yedges", T.ArrayType(T.DoubleType())),
                    ]
                ),
            )
        ]
    )

    data = [
        T.Row(
            value=T.Row(
                H=[[1.0, 2.0], [3.0, 4.0]],
                xedges=[0.0, 1.0, 2.0],
                yedges=[10.0, 20.0, 30.0],
            )
        )
    ]
    df = spark.createDataFrame(data, schema)
    result = Histogram2DDuration._extract_histogram2d_info(df)
    expected_result = [
        T.Row(
            hist_values=[[1.0, 2.0], [3.0, 4.0]],
            x_hist_bins=[0.0, 1.0, 2.0],
            y_hist_bins=[10.0, 20.0, 30.0],
        )
    ]

    assert result.drop("value").collect() == expected_result


def test_add_event_id_column(spark):
    """Test _add_event_id_column method."""
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField("hist_name", T.StringType()),
        ]
    )

    data = [
        T.Row(container_id=1, hist_name="rpm_vs_speed_hist"),
        T.Row(container_id=2, hist_name="not_present"),
    ]

    ts_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    my_event = BasicEvent(name="veh_spd_event", expr=ts_expr, desc="Vehicle speed > 1 km/h")
    hist = Histogram2DDuration(
        name="rpm_vs_speed_hist",
        x_expr=ts_expr,
        y_expr=ts_expr,
        x_bins=bins,
        y_bins=bins,
        event=my_event,
    )

    df = spark.createDataFrame(data, schema=schema)

    result = Histogram2DDuration._add_event_id_column([hist])(df)
    expected_result = [
        T.Row(container_id=1, hist_name="rpm_vs_speed_hist", event_id=491722133),
        T.Row(container_id=2, hist_name="not_present", event_id=None),
    ]

    assert result.collect() == expected_result

    result = Histogram2DDuration._add_event_id_column([])(df)
    expected_result = [
        T.Row(container_id=1, hist_name="rpm_vs_speed_hist", event_id=None),
        T.Row(container_id=2, hist_name="not_present", event_id=None),
    ]

    assert result.orderBy("container_id").collect() == expected_result


def test_explode_histogram2d_values(spark):
    """Test _explode_histogram2d_values method."""
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField("hist_name", T.StringType()),
            T.StructField("event_id", T.IntegerType()),
            T.StructField("x_hist_bins", T.ArrayType(T.DoubleType())),
            T.StructField("y_hist_bins", T.ArrayType(T.DoubleType())),
            T.StructField("hist_values", T.ArrayType(T.ArrayType(T.DoubleType()))),
        ]
    )

    data = [
        T.Row(
            container_id=1,
            hist_name="rpm_vs_speed_hist",
            event_id=460732922,
            x_hist_bins=[0.0, 1.0, 2.0],
            y_hist_bins=[10.0, 20.0, 30.0],
            hist_values=[[1.0, 2.0], [3.0, 4.0]],
        )
    ]

    df = spark.createDataFrame(data, schema=schema)
    result = Histogram2DDuration._explode_histogram2d_values(df)
    expected_result = [
        T.Row(
            container_id=1,
            x_hist_bins=[0.0, 1.0, 2.0],
            y_hist_bins=[10.0, 20.0, 30.0],
            x_bin_id=0,
            y_bin_id=0,
            hist_value=1.0,
        ),
        T.Row(
            container_id=1,
            x_hist_bins=[0.0, 1.0, 2.0],
            y_hist_bins=[10.0, 20.0, 30.0],
            x_bin_id=0,
            y_bin_id=1,
            hist_value=2.0,
        ),
        T.Row(
            container_id=1,
            x_hist_bins=[0.0, 1.0, 2.0],
            y_hist_bins=[10.0, 20.0, 30.0],
            x_bin_id=1,
            y_bin_id=0,
            hist_value=3.0,
        ),
        T.Row(
            container_id=1,
            x_hist_bins=[0.0, 1.0, 2.0],
            y_hist_bins=[10.0, 20.0, 30.0],
            x_bin_id=1,
            y_bin_id=1,
            hist_value=4.0,
        ),
    ]

    assert (
        result.drop("hist_name", "event_id").orderBy("x_bin_id", "y_bin_id").collect()
        == expected_result
    )


def test_extract_histogram2d_bin_info(spark):
    """Test _extract_histogram2d_bin_info method."""
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField("hist_name", T.StringType()),
            T.StructField("event_id", T.IntegerType()),
            T.StructField("x_hist_bins", T.ArrayType(T.DoubleType())),
            T.StructField("y_hist_bins", T.ArrayType(T.DoubleType())),
            T.StructField("x_bin_id", T.IntegerType()),
            T.StructField("y_bin_id", T.IntegerType()),
            T.StructField("hist_value", T.DoubleType()),
        ]
    )

    data = [
        T.Row(
            container_id=1,
            hist_name="rpm_vs_speed_hist",
            event_id=460732922,
            x_hist_bins=[0.0, 10.0],
            y_hist_bins=[0.0, 10.0],
            x_bin_id=0,
            y_bin_id=0,
            hist_value=0.0,
        )
    ]
    df = spark.createDataFrame(data, schema=schema)

    result = Histogram2DDuration._extract_histogram2d_bin_info(df)
    expected_result = [
        T.Row(
            x_lower_bound=0.0,
            y_lower_bound=0.0,
            x_upper_bound=10.0,
            y_upper_bound=10.0,
            x_bin_name="0.0-10.0",
            y_bin_name="0.0-10.0",
            hist_value=0.0,
        )
    ]

    assert (
        result.select(
            "x_lower_bound",
            "y_lower_bound",
            "x_upper_bound",
            "y_upper_bound",
            "x_bin_name",
            "y_bin_name",
            "hist_value",
        ).collect()
    ) == expected_result


def test_add_visual_id_column(spark):
    """Test _add_visual_id_column method."""
    schema = T.StructType(
        [
            T.StructField("container_id", T.LongType()),
            T.StructField("hist_name", T.StringType()),
        ]
    )

    data = [
        T.Row(container_id=1, hist_name="rpm_vs_speed_hist"),
        T.Row(container_id=2, hist_name="not_present"),
    ]

    ts_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    hist = Histogram2DDuration(
        name="rpm_vs_speed_hist",
        x_expr=ts_expr,
        y_expr=ts_expr,
        x_bins=bins,
        y_bins=bins,
    )

    df = spark.createDataFrame(data, schema=schema)

    result = Histogram2DDuration._add_visual_id_column([hist])(df)
    expected_result = [
        T.Row(container_id=1, hist_name="rpm_vs_speed_hist", visual_id=616372477),
        T.Row(container_id=2, hist_name="not_present", visual_id=None),
    ]

    assert result.orderBy("container_id").collect() == expected_result


# Tests for Histogram2DCustomWeights class


def test_histogram2d_custom_weights_init():
    """Test Histogram2DCustomWeights initialization with required parameters"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    x_bins = [0.0, 1.0, 2.0, 3.0]
    y_bins = [0.0, 5.0, 10.0]

    hist = Histogram2DCustomWeights(
        name="test_custom_weights_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=x_bins,
        y_bins=y_bins,
    )

    assert hist.name == "test_custom_weights_hist"
    assert hist.page_number == -1
    assert hist.x_expr == x_expr
    assert hist.y_expr == y_expr
    assert hist.weights_expr == weights_expr
    assert hist.x_bins == x_bins
    assert hist.y_bins == y_bins
    assert hist.event is None
    assert hist.desc is None
    assert hist.agg_type == "histogram2d_custom_weights"
    assert hist.values_unit is None
    assert hist.x_bins_unit is None
    assert hist.y_bins_unit is None
    assert hist.channel_interp_kind == "previous"
    assert hist.weights_interp_kind == "previous"
    assert hist.math_fct_for_weights is None
    assert hist.math_fct_kwargs is None


def test_histogram2d_custom_weights_init_with_optional_params():
    """Test Histogram2DCustomWeights initialization with all optional parameters"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)
    x_bins = [0.0, 5.0, 10.0]
    y_bins = [0.0, 100.0, 200.0]

    hist = Histogram2DCustomWeights(
        name="test_custom_weights_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=x_bins,
        y_bins=y_bins,
        event=basic_event,
        desc="Test custom weights histogram",
        agg_type="custom_type",
        x_channel_name="x_signal",
        y_channel_name="y_signal",
        weights_channel_name="weights_signal",
        values_unit="kg",
        x_bins_unit="rpm",
        y_bins_unit="km/h",
        channel_interp_kind="linear",
        weights_interp_kind="linear",
        math_fct_for_weights="diff",
        math_fct_kwargs={"param1": 1},
    )

    assert hist.name == "test_custom_weights_hist"
    assert hist.x_expr == x_expr
    assert hist.y_expr == y_expr
    assert hist.weights_expr == weights_expr
    assert hist.x_bins == x_bins
    assert hist.y_bins == y_bins
    assert hist.event == basic_event
    assert hist.desc == "Test custom weights histogram"
    assert hist.agg_type == "custom_type"
    assert hist.x_channel_name == "x_signal"
    assert hist.y_channel_name == "y_signal"
    assert hist.weights_channel_name == "weights_signal"
    assert hist.values_unit == "kg"
    assert hist.x_bins_unit == "rpm"
    assert hist.y_bins_unit == "km/h"
    assert hist.channel_interp_kind == "linear"
    assert hist.weights_interp_kind == "linear"
    assert hist.math_fct_for_weights == "diff"
    assert hist.math_fct_kwargs == {"param1": 1}


def test_histogram2d_custom_weights_get_name():
    """Test get_name method for Histogram2DCustomWeights"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    hist = Histogram2DCustomWeights(
        name="my_custom_histogram",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    assert hist.get_name() == "my_custom_histogram"


def test_histogram2d_custom_weights_get_expression():
    """Test get_expression method for Histogram2DCustomWeights"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    hist = Histogram2DCustomWeights(
        name="my_custom_histogram",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    expression = hist.get_expression()
    assert expression is not None
    assert hasattr(expression, "__str__")


def test_histogram2d_custom_weights_get_expression_with_event():
    """Test expression creation for Histogram2DCustomWeights when event is provided"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    hist = Histogram2DCustomWeights(
        name="my_custom_histogram",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
        event=basic_event,
    )
    expression = hist.get_expression()
    assert expression is not None


def test_histogram2d_custom_weights_get_expression_str():
    """Test get_expression_str method for Histogram2DCustomWeights"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    hist = Histogram2DCustomWeights(
        name="my_custom_histogram",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    expr_str = hist.get_expression_str()
    assert isinstance(expr_str, str)
    assert expr_str != "NA"


def test_histogram2d_custom_weights_as_dict():
    """Test as_dict method for Histogram2DCustomWeights"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    x_bins = [0.0, 1.0, 2.0]
    y_bins = [0.0, 5.0, 10.0]

    hist = Histogram2DCustomWeights(
        name="my_custom_histogram",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=x_bins,
        y_bins=y_bins,
        weights_channel_name="distance",
    )

    hist_dict = hist.as_dict()
    assert isinstance(hist_dict, dict)
    assert hist_dict["name"] == "my_custom_histogram"
    assert hist_dict["page_number"] == -1
    assert hist_dict["x_bins"] == x_bins
    assert hist_dict["y_bins"] == y_bins
    assert hist_dict["x_signal_expression"] == x_expr.get_expression_str()
    assert hist_dict["y_signal_expression"] == y_expr.get_expression_str()
    assert hist_dict["weights_channel_name"] == "distance"
    assert hist_dict["weights_expression"] == weights_expr.get_expression_str()
    assert hist_dict["description"] is None
    assert hist_dict["agg_type"] == "histogram2d_custom_weights"
    assert hist_dict["values_unit"] is None
    assert hist_dict["x_bins_unit"] is None
    assert hist_dict["y_bins_unit"] is None


def test_histogram2d_custom_weights_as_spark_row():
    """Test as_spark_row method for Histogram2DCustomWeights"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    x_bins = [0.0, 10.0, 20.0, 30.0]
    y_bins = [0.0, 50.0, 100.0]

    hist = Histogram2DCustomWeights(
        name="complete_custom_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=x_bins,
        y_bins=y_bins,
        desc="Complete custom weights histogram test",
        values_unit="count",
        x_bins_unit="rpm",
        y_bins_unit="km/h",
        weights_channel_name="odometer",
    )

    row = hist.as_spark_row()
    assert row.name == "complete_custom_hist"
    assert row.description == "Complete custom weights histogram test"
    assert row.agg_type == "histogram2d_custom_weights"
    assert row.x_bins == x_bins
    assert row.y_bins == y_bins
    assert row.values_unit == "count"
    assert row.x_bins_unit == "rpm"
    assert row.y_bins_unit == "km/h"
    assert row.weights_channel_name == "odometer"
    assert isinstance(row.x_signal_expression, str)
    assert isinstance(row.y_signal_expression, str)
    assert isinstance(row.weights_expression, str)


def test_histogram2d_custom_weights_as_spark_row_len():
    """Test as_spark_row returns expected number of fields for Histogram2DCustomWeights"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    hist = Histogram2DCustomWeights(
        name="my_custom_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    row = hist.as_spark_row()
    assert len(row) == 18


# Tests for Histogram2DDistance class


def test_histogram2d_distance_init():
    """Test Histogram2DDistance initialization with required parameters"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    x_bins = [0.0, 1.0, 2.0, 3.0]
    y_bins = [0.0, 5.0, 10.0]

    hist = Histogram2DDistance(
        name="test_distance_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=x_bins,
        y_bins=y_bins,
    )

    assert hist.name == "test_distance_hist"
    assert hist.page_number == -1
    assert hist.x_expr == x_expr
    assert hist.y_expr == y_expr
    assert hist.weights_expr == weights_expr
    assert hist.x_bins == x_bins
    assert hist.y_bins == y_bins
    assert hist.event is None
    assert hist.desc is None
    assert hist.agg_type == "histogram2d_distance"
    assert hist.values_unit is None
    assert hist.x_bins_unit is None
    assert hist.y_bins_unit is None
    assert hist.channel_interp_kind == "previous"
    assert hist.weights_interp_kind == "previous"
    assert hist.math_fct_for_weights == "diff"
    assert hist.math_fct_kwargs is None


def test_histogram2d_distance_init_with_optional_params():
    """Test Histogram2DDistance initialization with all optional parameters"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)
    x_bins = [0.0, 5.0, 10.0]
    y_bins = [0.0, 100.0, 200.0]

    hist = Histogram2DDistance(
        name="test_distance_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=x_bins,
        y_bins=y_bins,
        event=basic_event,
        desc="Test distance histogram",
        x_channel_name="x_signal",
        y_channel_name="y_signal",
        values_unit="km",
        x_bins_unit="rpm",
        y_bins_unit="km/h",
        channel_interp_kind="linear",
        weights_interp_kind="linear",
        math_fct_kwargs={"clip_negative": True},
    )

    assert hist.name == "test_distance_hist"
    assert hist.x_expr == x_expr
    assert hist.y_expr == y_expr
    assert hist.weights_expr == weights_expr
    assert hist.x_bins == x_bins
    assert hist.y_bins == y_bins
    assert hist.event == basic_event
    assert hist.desc == "Test distance histogram"
    assert hist.agg_type == "histogram2d_distance"
    assert hist.x_channel_name == "x_signal"
    assert hist.y_channel_name == "y_signal"
    assert hist.values_unit == "km"
    assert hist.x_bins_unit == "rpm"
    assert hist.y_bins_unit == "km/h"
    assert hist.channel_interp_kind == "linear"
    assert hist.weights_interp_kind == "linear"
    assert hist.math_fct_for_weights == "diff"
    assert hist.math_fct_kwargs == {"clip_negative": True}


def test_histogram2d_distance_get_name():
    """Test get_name method for Histogram2DDistance"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    hist = Histogram2DDistance(
        name="my_distance_histogram",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    assert hist.get_name() == "my_distance_histogram"


def test_histogram2d_distance_get_expression():
    """Test get_expression method for Histogram2DDistance"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    hist = Histogram2DDistance(
        name="my_distance_histogram",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    expression = hist.get_expression()
    assert expression is not None
    assert hasattr(expression, "__str__")


def test_histogram2d_distance_get_expression_with_event():
    """Test expression creation for Histogram2DDistance when event is provided"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    hist = Histogram2DDistance(
        name="my_distance_histogram",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
        event=basic_event,
    )
    expression = hist.get_expression()
    assert expression is not None


def test_histogram2d_distance_get_expression_str():
    """Test get_expression_str method for Histogram2DDistance"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    hist = Histogram2DDistance(
        name="my_distance_histogram",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    expr_str = hist.get_expression_str()
    assert isinstance(expr_str, str)
    assert expr_str != "NA"


def test_histogram2d_distance_as_dict():
    """Test as_dict method for Histogram2DDistance"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    x_bins = [0.0, 1.0, 2.0]
    y_bins = [0.0, 5.0, 10.0]

    hist = Histogram2DDistance(
        name="my_distance_histogram",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=x_bins,
        y_bins=y_bins,
    )

    hist_dict = hist.as_dict()
    assert isinstance(hist_dict, dict)
    assert hist_dict["name"] == "my_distance_histogram"
    assert hist_dict["page_number"] == -1
    assert hist_dict["x_bins"] == x_bins
    assert hist_dict["y_bins"] == y_bins
    assert hist_dict["x_signal_expression"] == x_expr.get_expression_str()
    assert hist_dict["y_signal_expression"] == y_expr.get_expression_str()
    assert hist_dict["weights_expression"] == weights_expr.get_expression_str()
    assert hist_dict["description"] is None
    assert hist_dict["agg_type"] == "histogram2d_distance"
    assert hist_dict["values_unit"] is None
    assert hist_dict["x_bins_unit"] is None
    assert hist_dict["y_bins_unit"] is None


def test_histogram2d_distance_as_spark_row():
    """Test as_spark_row method for Histogram2DDistance"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    x_bins = [0.0, 10.0, 20.0, 30.0]
    y_bins = [0.0, 50.0, 100.0]

    hist = Histogram2DDistance(
        name="complete_distance_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=x_bins,
        y_bins=y_bins,
        desc="Complete distance histogram test",
        values_unit="km",
        x_bins_unit="rpm",
        y_bins_unit="km/h",
    )

    row = hist.as_spark_row()
    assert row.name == "complete_distance_hist"
    assert row.description == "Complete distance histogram test"
    assert row.agg_type == "histogram2d_distance"
    assert row.x_bins == x_bins
    assert row.y_bins == y_bins
    assert row.values_unit == "km"
    assert row.x_bins_unit == "rpm"
    assert row.y_bins_unit == "km/h"
    assert isinstance(row.x_signal_expression, str)
    assert isinstance(row.y_signal_expression, str)
    assert isinstance(row.weights_expression, str)


def test_histogram2d_distance_as_spark_row_len():
    """Test as_spark_row returns expected number of fields for Histogram2DDistance"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    hist = Histogram2DDistance(
        name="my_distance_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    row = hist.as_spark_row()
    assert len(row) == 18


def test_histogram2d_distance_agg_type_is_fixed():
    """Test that Histogram2DDistance always uses 'histogram2d_distance' as agg_type"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    hist = Histogram2DDistance(
        name="test_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    # agg_type should always be 'histogram2d_distance' regardless of input
    assert hist.agg_type == "histogram2d_distance"


def test_histogram2d_distance_math_fct_is_diff():
    """Test that Histogram2DDistance always uses 'diff' as math_fct_for_weights"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    hist = Histogram2DDistance(
        name="test_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    # math_fct_for_weights should always be 'diff'
    assert hist.math_fct_for_weights == "diff"


def test_histogram2d_custom_weights_bins_validation():
    """Test that bins parameter accepts different numeric types for Histogram2DCustomWeights"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)

    # Test with integers
    x_bins = [0, 1, 2, 3]
    y_bins = [0, 10, 20]
    hist1 = Histogram2DCustomWeights(
        name="test1",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    assert hist1.x_bins == [0, 1, 2, 3]
    assert hist1.y_bins == [0, 10, 20]

    # Test with floats
    x_bins = [0.0, 1.5, 3.0]
    y_bins = [0.0, 5.5, 11.0]
    hist2 = Histogram2DCustomWeights(
        name="test2",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    assert hist2.x_bins == [0.0, 1.5, 3.0]
    assert hist2.y_bins == [0.0, 5.5, 11.0]

    # Test with mixed types
    x_bins = [0, 1.5, 3]
    y_bins = [0, 5.5, 11]
    hist3 = Histogram2DCustomWeights(
        name="test3",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    assert hist3.x_bins == x_bins
    assert hist3.y_bins == y_bins


def test_histogram2d_distance_bins_validation():
    """Test that bins parameter accepts different numeric types for Histogram2DDistance"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)

    # Test with integers
    x_bins = [0, 1, 2, 3]
    y_bins = [0, 10, 20]
    hist1 = Histogram2DDistance(
        name="test1",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    assert hist1.x_bins == [0, 1, 2, 3]
    assert hist1.y_bins == [0, 10, 20]

    # Test with floats
    x_bins = [0.0, 1.5, 3.0]
    y_bins = [0.0, 5.5, 11.0]
    hist2 = Histogram2DDistance(
        name="test2",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=x_bins,
        y_bins=y_bins,
    )
    assert hist2.x_bins == [0.0, 1.5, 3.0]
    assert hist2.y_bins == [0.0, 5.5, 11.0]


def test_histogram2d_custom_weights_get_event():
    """Test get_event method for Histogram2DCustomWeights"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)
    bins = [0.0, 1.0, 2.0]

    hist_with_event = Histogram2DCustomWeights(
        name="test_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
        event=basic_event,
    )
    assert hist_with_event.get_event() == basic_event

    hist_without_event = Histogram2DCustomWeights(
        name="test_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    assert hist_without_event.get_event() is None


def test_histogram2d_distance_get_event():
    """Test get_event method for Histogram2DDistance"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)
    bins = [0.0, 1.0, 2.0]

    hist_with_event = Histogram2DDistance(
        name="test_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
        event=basic_event,
    )
    assert hist_with_event.get_event() == basic_event

    hist_without_event = Histogram2DDistance(
        name="test_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    assert hist_without_event.get_event() is None


def test_histogram2d_custom_weights_get_id():
    """Test get_id method for Histogram2DCustomWeights returns consistent unique ID"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    hist1 = Histogram2DCustomWeights(
        name="test_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    hist2 = Histogram2DCustomWeights(
        name="test_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    hist3 = Histogram2DCustomWeights(
        name="different_name",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )

    # Same name and expression should return same ID
    assert hist1.get_id() == hist2.get_id()
    # Different name should return different ID
    assert hist1.get_id() != hist3.get_id()
    # ID should be a positive integer
    assert isinstance(hist1.get_id(), int)
    assert hist1.get_id() > 0


def test_histogram2d_distance_get_id():
    """Test get_id method for Histogram2DDistance returns consistent unique ID"""
    x_expr = TimeSeriesSelector(None)
    y_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]

    hist1 = Histogram2DDistance(
        name="test_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    hist2 = Histogram2DDistance(
        name="test_hist",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    hist3 = Histogram2DDistance(
        name="different_name",
        x_expr=x_expr,
        y_expr=y_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )

    # Same name and expression should return same ID
    assert hist1.get_id() == hist2.get_id()
    # Different name should return different ID
    assert hist1.get_id() != hist3.get_id()
    # ID should be a positive integer
    assert isinstance(hist1.get_id(), int)
    assert hist1.get_id() > 0


# ---------------------------------------------------------------------------
# Definition hash tests
# ---------------------------------------------------------------------------
def test_definition_hash_exists_in_as_dict():
    """Verify definition_hash key is present and non-null in as_dict output."""
    ts_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    hist = Histogram2DDuration(
        name="hash_hist", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins
    )
    d = hist.as_dict()
    assert "definition_hash" in d
    assert d["definition_hash"] is not None
    assert isinstance(d["definition_hash"], int)


def test_definition_hash_exists_in_spark_row():
    """Verify definition_hash field is present in as_spark_row output."""
    ts_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    hist = Histogram2DDuration(
        name="hash_hist", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins
    )
    row = hist.as_spark_row()
    assert hasattr(row, "definition_hash")
    assert row.definition_hash is not None
    assert isinstance(row.definition_hash, int)


def test_definition_hash_is_deterministic():
    """Same inputs must always produce the same hash."""
    ts_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    hist1 = Histogram2DDuration(name="h", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins)
    hist2 = Histogram2DDuration(name="h", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins)
    assert hist1.determine_definition_hash() == hist2.determine_definition_hash()


def test_definition_hash_changes_with_x_bins():
    """Different x_bins must produce a different hash."""
    ts_expr = TimeSeriesSelector(None)
    y_bins = [0.0, 1.0, 2.0]
    hist1 = Histogram2DDuration(
        name="h", x_expr=ts_expr, y_expr=ts_expr, x_bins=[0.0, 1.0], y_bins=y_bins
    )
    hist2 = Histogram2DDuration(
        name="h", x_expr=ts_expr, y_expr=ts_expr, x_bins=[0.0, 5.0, 10.0], y_bins=y_bins
    )
    assert hist1.determine_definition_hash() != hist2.determine_definition_hash()


def test_definition_hash_changes_with_y_bins():
    """Different y_bins must produce a different hash."""
    ts_expr = TimeSeriesSelector(None)
    x_bins = [0.0, 1.0, 2.0]
    hist1 = Histogram2DDuration(
        name="h", x_expr=ts_expr, y_expr=ts_expr, x_bins=x_bins, y_bins=[0.0, 1.0]
    )
    hist2 = Histogram2DDuration(
        name="h", x_expr=ts_expr, y_expr=ts_expr, x_bins=x_bins, y_bins=[0.0, 5.0, 10.0]
    )
    assert hist1.determine_definition_hash() != hist2.determine_definition_hash()


def test_definition_hash_ignores_name():
    """Hash must not change when only the name differs."""
    ts_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    hist1 = Histogram2DDuration(
        name="name_a", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins
    )
    hist2 = Histogram2DDuration(
        name="name_b", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins
    )
    assert hist1.determine_definition_hash() == hist2.determine_definition_hash()


def test_definition_hash_ignores_description():
    """Hash must not change when only desc differs."""
    ts_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    hist1 = Histogram2DDuration(
        name="h", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins, desc="A"
    )
    hist2 = Histogram2DDuration(
        name="h", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins, desc="B"
    )
    assert hist1.determine_definition_hash() == hist2.determine_definition_hash()


def test_definition_hash_changes_with_event():
    """Adding an event must change the hash."""
    ts_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    event = BasicEvent(name="ev", expr=TimeSeriesSelector(None))
    hist_no_event = Histogram2DDuration(
        name="h", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins
    )
    hist_with_event = Histogram2DDuration(
        name="h", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins, event=event
    )
    assert hist_no_event.determine_definition_hash() != hist_with_event.determine_definition_hash()


def test_definition_hash_custom_weights_includes_weights():
    """Histogram2DCustomWeights hash must include the weights expression."""
    ts_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    hist_dur = Histogram2DDuration(
        name="h", x_expr=ts_expr, y_expr=ts_expr, x_bins=bins, y_bins=bins
    )
    hist_cw = Histogram2DCustomWeights(
        name="h",
        x_expr=ts_expr,
        y_expr=ts_expr,
        weights_expr=weights_expr,
        x_bins=bins,
        y_bins=bins,
    )
    # Duration and CustomWeights should differ because weights_expr is included
    assert hist_dur.determine_definition_hash() != hist_cw.determine_definition_hash()


def test_determine_aggregations_requires_solved_df(spark):
    expr = TimeSeriesSelector(None)
    hist = Histogram2DDuration(
        name="test_hist",
        x_expr=expr,
        y_expr=expr,
        x_bins=[0.0, 10000.0],
        y_bins=[0.0, 10000.0],
    )
    with pytest.raises(ValueError, match="requires solved_df"):
        Histogram2DDuration.determine_aggregations(spark=spark, aggregations=[hist])
