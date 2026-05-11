import pytest

from mda_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from mda_query_engine.analyze.query.solvers.key_value_store_solver import KeyValueStoreSolver
from mda_reporting.aggregations.histogram import (
    HistogramCustomWeights,
    HistogramDistance,
    HistogramDuration,
)
from mda_reporting.events.basic_event import BasicEvent
from tests.conftest import basic_narrow_db, spark


def test_as_spark_row():
    hist = HistogramDuration(
        name="my_hist_1", base_expr=TimeSeriesSelector(None), bins=[0.0, 1.0, 2.0]
    )
    row = hist.as_spark_row()
    assert len(row) == 14


def test_histogram_init():
    """Test Histogram initialization with required parameters"""
    base_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0, 3.0]
    hist = HistogramDuration(name="test_hist", base_expr=base_expr, bins=bins)

    assert hist.name == "test_hist"
    assert hist.page_number == -1  # Default value
    assert hist.base_expr == base_expr
    assert hist.bins == bins
    assert hist.event is None
    assert hist.desc is None
    assert hist.agg_type == "histogram_duration"
    assert hist.values_unit is None
    assert hist.bins_unit is None


def test_histogram_init_with_optional_params():
    """Test Histogram initialization with all optional parameters"""
    base_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)
    bins = [0.0, 5.0, 10.0]
    hist = HistogramDuration(
        name="test_hist",
        base_expr=base_expr,
        bins=bins,
        event=basic_event,
        desc="Test histogram",
        agg_type="frequency",
        values_unit="Hz",
        bins_unit="rpm",
    )

    assert hist.name == "test_hist"
    assert hist.base_expr == base_expr
    assert hist.bins == bins
    assert hist.event == basic_event
    assert hist.desc == "Test histogram"
    assert hist.agg_type == "frequency"
    assert hist.values_unit == "Hz"
    assert hist.bins_unit == "rpm"


def test_get_name():
    """Test get_name method"""
    hist = HistogramDuration(
        name="my_histogram", base_expr=TimeSeriesSelector(None), bins=[0.0, 1.0]
    )
    assert hist.get_name() == "my_histogram"


def test_get_expression():
    """Test get_expression method returns TimeSeriesExpression"""
    base_expr = TimeSeriesSelector(None)
    hist = HistogramDuration(name="test", base_expr=base_expr, bins=[0.0, 1.0, 2.0])
    expression = hist.get_expression()
    assert expression is not None
    # Expression should be set during initialization
    assert hasattr(expression, "__str__")


def test_get_expression_with_event_expr():
    """Test expression creation when event is provided"""
    base_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)
    hist = HistogramDuration(name="test", base_expr=base_expr, bins=[0.0, 1.0], event=basic_event)
    expression = hist.get_expression()
    assert expression is not None


def test_get_expression_str():
    """Test get_expression_str method"""
    base_expr = TimeSeriesSelector(None)
    hist = HistogramDuration(name="test", base_expr=base_expr, bins=[0.0, 1.0, 2.0])
    expr_str = hist.get_expression_str()
    assert isinstance(expr_str, str)
    assert expr_str != "NA"


def test_as_dict():
    """Test as_dict method"""
    base_expr = TimeSeriesSelector(None)
    hist = HistogramDuration(name="test_hist", base_expr=base_expr, bins=[0.0, 1.0, 2.0])

    hist_dict = hist.as_dict()
    assert isinstance(hist_dict, dict)
    assert hist_dict["name"] == "test_hist"
    assert hist_dict["page_number"] == -1  # Default value
    assert hist_dict["bins"] == [0.0, 1.0, 2.0]
    assert hist_dict["signal_expression"] == base_expr.get_expression_str()
    assert hist_dict["description"] is None
    assert hist_dict["agg_type"] == "histogram_duration"
    assert hist_dict["values_unit"] is None
    assert hist_dict["bins_unit"] is None


def test_as_spark_row_complete():
    """Test as_spark_row with all fields populated"""
    base_expr = TimeSeriesSelector(None)
    bins = [0.0, 10.0, 20.0, 30.0]

    hist = HistogramDuration(
        name="complete_hist",
        base_expr=base_expr,
        bins=bins,
        desc="Complete histogram test",
        agg_type="distribution",
        values_unit="count",
        bins_unit="value",
    )

    row = hist.as_spark_row()
    assert row.name == "complete_hist"
    assert row.description == "Complete histogram test"
    assert row.agg_type == "distribution"
    assert row.bins == bins
    assert row.values_unit == "count"
    assert row.bins_unit == "value"
    assert isinstance(row.signal_expression, str)


def test_as_spark_row_minimal():
    """Test as_spark_row with minimal required fields"""
    base_expr = TimeSeriesSelector(None)
    hist = HistogramDuration(name="minimal_hist", base_expr=base_expr, bins=[0.0, 1.0])

    row = hist.as_spark_row()
    assert row.name == "minimal_hist"
    assert row.description is None
    assert row.agg_type == "histogram_duration"
    assert row.bins == [0.0, 1.0]
    assert row.values_unit is None
    assert row.bins_unit is None
    assert isinstance(row.signal_expression, str)


def test_bins_validation():
    """Test that bins parameter accepts different numeric types"""
    base_expr = TimeSeriesSelector(None)

    # Test with integers
    hist1 = HistogramDuration(name="test1", base_expr=base_expr, bins=[0, 1, 2, 3])
    assert hist1.bins == [0, 1, 2, 3]

    # Test with floats
    hist2 = HistogramDuration(name="test2", base_expr=base_expr, bins=[0.0, 1.5, 3.0])
    assert hist2.bins == [0.0, 1.5, 3.0]

    # Test with mixed types
    hist3 = HistogramDuration(name="test3", base_expr=base_expr, bins=[0, 1.5, 3])
    assert hist3.bins == [0, 1.5, 3]


def test_determine_aggregations(spark, basic_narrow_db):
    """Test that determine_histogram_visuals returns a DataFrame with expected columns"""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    event = BasicEvent(name="test_event_1", expr=eng_rpm > 1000)
    hist = HistogramDuration(name="test_hist", base_expr=eng_rpm, event=event, bins=[0.0, 10000.0])
    solver = KeyValueStoreSolver(spark)
    solved_df = basic_narrow_db.query.select(hist.get_expression()).solve(spark, solver)
    df = HistogramDuration.determine_aggregations(
        spark=spark, aggregations=[hist], solved_df=solved_df
    )
    assert df.count() > 0  # Ensure that some data is returned
    assert "container_id" in df.columns
    assert "visual_id" in df.columns
    assert "event_id" in df.columns
    assert "bin_id" in df.columns
    assert "hist_value" in df.columns
    assert "lower_bound" in df.columns
    assert "upper_bound" in df.columns
    assert "bin_name" in df.columns


def test_determine_metadata_df(spark, basic_narrow_db):
    """Test that determine_metadata_df returns a DataFrame with expected columns"""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    veh_spd = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")
    hist1 = HistogramDuration(
        name="test_hist", desc="engine rpm histogram", base_expr=eng_rpm, bins=[0.0, 10000.0]
    )
    hist2 = HistogramDuration(name="test_hist2", base_expr=veh_spd, bins=[0.0, 300.0])

    df = HistogramDuration.determine_metadata_df(spark=spark, histograms=[hist1, hist2])
    assert df.count() == 2  # Ensure that metadata for two histograms is returned
    assert "page_number" in df.columns
    assert "name" in df.columns
    assert "description" in df.columns
    assert "agg_type" in df.columns
    assert "bins" in df.columns
    assert "values_unit" in df.columns
    assert "bins_unit" in df.columns
    assert "signal_expression" in df.columns


def test_get_visual_id_column(spark, basic_narrow_db):
    """Test get_visual_id_column static method with list of aggregations."""

    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")

    hist1 = HistogramDuration(name="hist_1", base_expr=eng_rpm, bins=[0, 500, 1000, 1500])
    hist2 = HistogramDuration(name="hist_2", base_expr=eng_rpm, bins=[0, 1000, 2000])

    visuals = [hist1, hist2]

    col_expr = HistogramDuration.get_visual_id_column(visuals, "hist_name")

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

    col_expr = HistogramDuration.get_visual_id_column(visuals, "hist_name")

    test_data = [("any_hist",), ("another_hist",)]
    df = spark.createDataFrame(test_data, ["hist_name"])

    result_df = df.withColumn("visual_id", col_expr)
    results = result_df.collect()

    # All should map to None when visuals list is empty
    assert results[0]["visual_id"] is None
    assert results[1]["visual_id"] is None


def test_histogram_custom_weights():
    """Test as_spark_row with minimal required fields"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    hist = HistogramCustomWeights(
        name="custom_weights_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
    )

    row = hist.as_spark_row()
    assert row.name == "custom_weights_hist"
    assert row.description is None
    assert row.agg_type == "histogram_custom_weights"
    assert row.bins == [0.0, 1.0]
    assert row.values_unit is None
    assert row.bins_unit is None
    assert isinstance(row.signal_expression, str)


def test_histogram_custom_weights_init():
    """Test HistogramCustomWeights initialization with required parameters"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0, 3.0]
    hist = HistogramCustomWeights(
        name="test_custom_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=bins,
    )

    assert hist.name == "test_custom_hist"
    assert hist.page_number == -1  # Default value
    assert hist.base_expr == base_expr
    assert hist.weights_expr == weights_expr
    assert hist.bins == bins
    assert hist.event is None
    assert hist.desc is None
    assert hist.agg_type == "histogram_custom_weights"
    assert hist.values_unit is None
    assert hist.bins_unit is None
    assert hist.channel_interp_kind == "previous"
    assert hist.weights_interp_kind == "previous"
    assert hist.math_fct_for_weights is None
    assert hist.math_fct_kwargs is None


def test_histogram_custom_weights_init_with_optional_params():
    """Test HistogramCustomWeights initialization with all optional parameters"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)
    bins = [0.0, 5.0, 10.0]
    hist = HistogramCustomWeights(
        name="test_custom_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=bins,
        event=basic_event,
        desc="Test custom weights histogram",
        agg_type="custom_frequency",
        channel_name="engine_rpm",
        weights_channel_name="vehicle_speed",
        values_unit="Hz",
        bins_unit="rpm",
        channel_interp_kind="linear",
        weights_interp_kind="linear",
        math_fct_for_weights="diff",
        math_fct_kwargs={"param1": 1},
    )

    assert hist.name == "test_custom_hist"
    assert hist.base_expr == base_expr
    assert hist.weights_expr == weights_expr
    assert hist.bins == bins
    assert hist.event == basic_event
    assert hist.desc == "Test custom weights histogram"
    assert hist.agg_type == "custom_frequency"
    assert hist.channel_name == "engine_rpm"
    assert hist.weights_channel_name == "vehicle_speed"
    assert hist.values_unit == "Hz"
    assert hist.bins_unit == "rpm"
    assert hist.channel_interp_kind == "linear"
    assert hist.weights_interp_kind == "linear"
    assert hist.math_fct_for_weights == "diff"
    assert hist.math_fct_kwargs == {"param1": 1}


def test_histogram_custom_weights_get_name():
    """Test get_name method for HistogramCustomWeights"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    hist = HistogramCustomWeights(
        name="my_custom_histogram",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
    )
    assert hist.get_name() == "my_custom_histogram"


def test_histogram_custom_weights_get_expression():
    """Test get_expression method returns TimeSeriesExpression for HistogramCustomWeights"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    hist = HistogramCustomWeights(
        name="test",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0, 2.0],
    )
    expression = hist.get_expression()
    assert expression is not None
    assert hasattr(expression, "__str__")


def test_histogram_custom_weights_get_expression_with_event():
    """Test expression creation when event is provided for HistogramCustomWeights"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)
    hist = HistogramCustomWeights(
        name="test",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
        event=basic_event,
    )
    expression = hist.get_expression()
    assert expression is not None


def test_histogram_custom_weights_get_expression_str():
    """Test get_expression_str method for HistogramCustomWeights"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    hist = HistogramCustomWeights(
        name="test",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0, 2.0],
    )
    expr_str = hist.get_expression_str()
    assert isinstance(expr_str, str)
    assert expr_str != "NA"


def test_histogram_custom_weights_as_dict():
    """Test as_dict method for HistogramCustomWeights"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    hist = HistogramCustomWeights(
        name="test_custom_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0, 2.0],
        weights_channel_name="weight_signal",
    )

    hist_dict = hist.as_dict()
    assert isinstance(hist_dict, dict)
    assert hist_dict["name"] == "test_custom_hist"
    assert hist_dict["page_number"] == -1  # Default value
    assert hist_dict["bins"] == [0.0, 1.0, 2.0]
    assert hist_dict["signal_expression"] == base_expr.get_expression_str()
    assert hist_dict["weights_expression"] == weights_expr.get_expression_str()
    assert hist_dict["weights_channel_name"] == "weight_signal"
    assert hist_dict["description"] is None
    assert hist_dict["agg_type"] == "histogram_custom_weights"
    assert hist_dict["values_unit"] is None
    assert hist_dict["bins_unit"] is None


def test_histogram_custom_weights_as_spark_row_complete():
    """Test as_spark_row with all fields populated for HistogramCustomWeights"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 10.0, 20.0, 30.0]

    hist = HistogramCustomWeights(
        name="complete_custom_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=bins,
        desc="Complete custom histogram test",
        agg_type="distribution",
        channel_name="base_signal",
        weights_channel_name="weight_signal",
        values_unit="count",
        bins_unit="value",
    )

    row = hist.as_spark_row()
    assert row.name == "complete_custom_hist"
    assert row.description == "Complete custom histogram test"
    assert row.agg_type == "distribution"
    assert row.bins == bins
    assert row.channel_name == "base_signal"
    assert row.weights_channel_name == "weight_signal"
    assert row.values_unit == "count"
    assert row.bins_unit == "value"
    assert isinstance(row.signal_expression, str)
    assert isinstance(row.weights_expression, str)


def test_histogram_custom_weights_as_spark_row_minimal():
    """Test as_spark_row with minimal required fields for HistogramCustomWeights"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    hist = HistogramCustomWeights(
        name="minimal_custom_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
    )

    row = hist.as_spark_row()
    assert row.name == "minimal_custom_hist"
    assert row.description is None
    assert row.agg_type == "histogram_custom_weights"
    assert row.bins == [0.0, 1.0]
    assert row.values_unit is None
    assert row.bins_unit is None
    assert row.weights_channel_name is None
    assert isinstance(row.signal_expression, str)
    assert isinstance(row.weights_expression, str)


def test_histogram_custom_weights_bins_validation():
    """Test that bins parameter accepts different numeric types for HistogramCustomWeights"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)

    # Test with integers
    hist1 = HistogramCustomWeights(
        name="test1", base_expr=base_expr, weights_expr=weights_expr, bins=[0, 1, 2, 3]
    )
    assert hist1.bins == [0, 1, 2, 3]

    # Test with floats
    hist2 = HistogramCustomWeights(
        name="test2",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.5, 3.0],
    )
    assert hist2.bins == [0.0, 1.5, 3.0]

    # Test with mixed types
    hist3 = HistogramCustomWeights(
        name="test3", base_expr=base_expr, weights_expr=weights_expr, bins=[0, 1.5, 3]
    )
    assert hist3.bins == [0, 1.5, 3]


def test_histogram_custom_weights_determine_aggregations(spark, basic_narrow_db):
    """Test that determine_aggregations returns a DataFrame with expected columns for HistogramCustomWeights"""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    veh_spd = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")
    event = BasicEvent(name="test_event_1", expr=eng_rpm > 1000)
    hist = HistogramCustomWeights(
        name="test_custom_hist",
        base_expr=eng_rpm,
        weights_expr=veh_spd,
        event=event,
        bins=[0.0, 10000.0],
    )
    solver = KeyValueStoreSolver(spark)
    solved_df = basic_narrow_db.query.select(hist.get_expression()).solve(spark, solver)
    df = HistogramCustomWeights.determine_aggregations(
        spark=spark, aggregations=[hist], solved_df=solved_df
    )
    assert df.count() > 0  # Ensure that some data is returned
    assert "container_id" in df.columns
    assert "visual_id" in df.columns
    assert "event_id" in df.columns
    assert "bin_id" in df.columns
    assert "hist_value" in df.columns
    assert "lower_bound" in df.columns
    assert "upper_bound" in df.columns
    assert "bin_name" in df.columns


def test_histogram_custom_weights_determine_metadata_df(spark, basic_narrow_db):
    """Test that determine_metadata_df returns a DataFrame with expected columns for HistogramCustomWeights"""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    veh_spd = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")
    hist1 = HistogramCustomWeights(
        name="test_custom_hist",
        desc="engine rpm with speed weights histogram",
        base_expr=eng_rpm,
        weights_expr=veh_spd,
        bins=[0.0, 10000.0],
    )
    hist2 = HistogramCustomWeights(
        name="test_custom_hist2",
        base_expr=veh_spd,
        weights_expr=eng_rpm,
        bins=[0.0, 300.0],
    )

    df = HistogramCustomWeights.determine_metadata_df(spark=spark, histograms=[hist1, hist2])
    assert df.count() == 2  # Ensure that metadata for two histograms is returned
    assert "page_number" in df.columns
    assert "name" in df.columns
    assert "description" in df.columns
    assert "agg_type" in df.columns
    assert "bins" in df.columns
    assert "values_unit" in df.columns
    assert "bins_unit" in df.columns
    assert "signal_expression" in df.columns
    assert "weights_expression" in df.columns
    assert "weights_channel_name" in df.columns


def test_histogram_custom_weights_get_visual_id_column(spark, basic_narrow_db):
    """Test get_visual_id_column static method with list of HistogramCustomWeights aggregations."""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    veh_spd = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")

    hist1 = HistogramCustomWeights(
        name="custom_hist_1",
        base_expr=eng_rpm,
        weights_expr=veh_spd,
        bins=[0, 500, 1000, 1500],
    )
    hist2 = HistogramCustomWeights(
        name="custom_hist_2",
        base_expr=eng_rpm,
        weights_expr=veh_spd,
        bins=[0, 1000, 2000],
    )

    visuals = [hist1, hist2]

    col_expr = HistogramCustomWeights.get_visual_id_column(visuals, "hist_name")

    test_data = [("custom_hist_1",), ("custom_hist_2",), ("unknown_hist",)]
    df = spark.createDataFrame(test_data, ["hist_name"])

    result_df = df.withColumn("visual_id", col_expr)
    results = result_df.collect()

    assert results[0]["visual_id"] == hist1.get_id()
    assert results[1]["visual_id"] == hist2.get_id()
    assert results[2]["visual_id"] is None


def test_histogram_custom_weights_get_event():
    """Test get_event method for HistogramCustomWeights"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    # Test with event
    hist_with_event = HistogramCustomWeights(
        name="test",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
        event=basic_event,
    )
    assert hist_with_event.get_event() == basic_event

    # Test without event
    hist_without_event = HistogramCustomWeights(
        name="test", base_expr=base_expr, weights_expr=weights_expr, bins=[0.0, 1.0]
    )
    assert hist_without_event.get_event() is None


def test_histogram_custom_weights_get_id():
    """Test get_id method returns consistent unique identifier for HistogramCustomWeights"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)

    hist1 = HistogramCustomWeights(
        name="test_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
    )
    hist2 = HistogramCustomWeights(
        name="test_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
    )
    hist3 = HistogramCustomWeights(
        name="different_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
    )

    # Same configuration should produce same ID
    assert hist1.get_id() == hist2.get_id()
    # Different name should produce different ID
    assert hist1.get_id() != hist3.get_id()
    # ID should be a positive integer
    assert isinstance(hist1.get_id(), int)
    assert hist1.get_id() > 0


# HistogramDistance Tests


def test_histogram_distance_init():
    """Test HistogramDistance initialization with required parameters"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0, 3.0]
    hist = HistogramDistance(
        name="test_distance_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=bins,
    )

    assert hist.name == "test_distance_hist"
    assert hist.page_number == -1  # Default value
    assert hist.base_expr == base_expr
    assert hist.weights_expr == weights_expr
    assert hist.bins == bins
    assert hist.event is None
    assert hist.desc is None
    assert hist.agg_type == "histogram_distance"
    assert hist.values_unit is None
    assert hist.bins_unit is None
    assert hist.channel_interp_kind == "previous"
    assert hist.weights_interp_kind == "previous"
    assert hist.math_fct_for_weights == "diff"
    assert hist.math_fct_kwargs is None


def test_histogram_distance_init_with_optional_params():
    """Test HistogramDistance initialization with all optional parameters"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)
    bins = [0.0, 5.0, 10.0]
    hist = HistogramDistance(
        name="test_distance_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=bins,
        event=basic_event,
        desc="Test distance histogram",
        channel_name="engine_rpm",
        weights_channel_name="odometer",
        values_unit="km",
        bins_unit="rpm",
    )

    assert hist.name == "test_distance_hist"
    assert hist.base_expr == base_expr
    assert hist.weights_expr == weights_expr
    assert hist.bins == bins
    assert hist.event == basic_event
    assert hist.desc == "Test distance histogram"
    assert hist.agg_type == "histogram_distance"
    assert hist.channel_name == "engine_rpm"
    assert hist.weights_channel_name == "odometer"
    assert hist.values_unit == "km"
    assert hist.bins_unit == "rpm"
    # These should be set by HistogramDistance defaults
    assert hist.channel_interp_kind == "previous"
    assert hist.weights_interp_kind == "previous"
    assert hist.math_fct_for_weights == "diff"


def test_histogram_distance_get_name():
    """Test get_name method for HistogramDistance"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    hist = HistogramDistance(
        name="my_distance_histogram",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
    )
    assert hist.get_name() == "my_distance_histogram"


def test_histogram_distance_get_expression():
    """Test get_expression method returns TimeSeriesExpression for HistogramDistance"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    hist = HistogramDistance(
        name="test",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0, 2.0],
    )
    expression = hist.get_expression()
    assert expression is not None
    assert hasattr(expression, "__str__")


def test_histogram_distance_get_expression_with_event():
    """Test expression creation when event is provided for HistogramDistance"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)
    hist = HistogramDistance(
        name="test",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
        event=basic_event,
    )
    expression = hist.get_expression()
    assert expression is not None


def test_histogram_distance_get_expression_str():
    """Test get_expression_str method for HistogramDistance"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    hist = HistogramDistance(
        name="test",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0, 2.0],
    )
    expr_str = hist.get_expression_str()
    assert isinstance(expr_str, str)
    assert expr_str != "NA"


def test_histogram_distance_as_dict():
    """Test as_dict method for HistogramDistance"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    hist = HistogramDistance(
        name="test_distance_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0, 2.0],
        weights_channel_name="odometer",
    )

    hist_dict = hist.as_dict()
    assert isinstance(hist_dict, dict)
    assert hist_dict["name"] == "test_distance_hist"
    assert hist_dict["page_number"] == -1  # Default value
    assert hist_dict["bins"] == [0.0, 1.0, 2.0]
    assert hist_dict["signal_expression"] == base_expr.get_expression_str()
    assert hist_dict["weights_expression"] == weights_expr.get_expression_str()
    assert hist_dict["weights_channel_name"] == "odometer"
    assert hist_dict["description"] is None
    assert hist_dict["agg_type"] == "histogram_distance"
    assert hist_dict["values_unit"] is None
    assert hist_dict["bins_unit"] is None


def test_histogram_distance_as_spark_row():
    """Test as_spark_row with minimal required fields for HistogramDistance"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    hist = HistogramDistance(
        name="distance_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
    )

    row = hist.as_spark_row()
    assert row.name == "distance_hist"
    assert row.description is None
    assert row.agg_type == "histogram_distance"
    assert row.bins == [0.0, 1.0]
    assert row.values_unit is None
    assert row.bins_unit is None
    assert row.weights_channel_name is None
    assert isinstance(row.signal_expression, str)
    assert isinstance(row.weights_expression, str)


def test_histogram_distance_as_spark_row_complete():
    """Test as_spark_row with all fields populated for HistogramDistance"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 10.0, 20.0, 30.0]

    hist = HistogramDistance(
        name="complete_distance_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=bins,
        desc="Complete distance histogram test",
        channel_name="base_signal",
        weights_channel_name="odometer",
        values_unit="km",
        bins_unit="rpm",
    )

    row = hist.as_spark_row()
    assert row.name == "complete_distance_hist"
    assert row.description == "Complete distance histogram test"
    assert row.agg_type == "histogram_distance"
    assert row.bins == bins
    assert row.channel_name == "base_signal"
    assert row.weights_channel_name == "odometer"
    assert row.values_unit == "km"
    assert row.bins_unit == "rpm"
    assert isinstance(row.signal_expression, str)
    assert isinstance(row.weights_expression, str)


def test_histogram_distance_bins_validation():
    """Test that bins parameter accepts different numeric types for HistogramDistance"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)

    # Test with integers
    hist1 = HistogramDistance(
        name="test1", base_expr=base_expr, weights_expr=weights_expr, bins=[0, 1, 2, 3]
    )
    assert hist1.bins == [0, 1, 2, 3]

    # Test with floats
    hist2 = HistogramDistance(
        name="test2",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.5, 3.0],
    )
    assert hist2.bins == [0.0, 1.5, 3.0]

    # Test with mixed types
    hist3 = HistogramDistance(
        name="test3", base_expr=base_expr, weights_expr=weights_expr, bins=[0, 1.5, 3]
    )
    assert hist3.bins == [0, 1.5, 3]


def test_histogram_distance_determine_aggregations(spark, basic_narrow_db):
    """Test that determine_aggregations returns a DataFrame with expected columns for HistogramDistance"""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    veh_spd = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")
    event = BasicEvent(name="test_event_1", expr=eng_rpm > 1000)
    hist = HistogramDistance(
        name="test_distance_hist",
        base_expr=eng_rpm,
        weights_expr=veh_spd,
        event=event,
        bins=[0.0, 10000.0],
    )
    solver = KeyValueStoreSolver(spark)
    solved_df = basic_narrow_db.query.select(hist.get_expression()).solve(spark, solver)
    df = HistogramDistance.determine_aggregations(
        spark=spark, aggregations=[hist], solved_df=solved_df
    )
    assert df.count() > 0  # Ensure that some data is returned
    assert "container_id" in df.columns
    assert "visual_id" in df.columns
    assert "event_id" in df.columns
    assert "bin_id" in df.columns
    assert "hist_value" in df.columns
    assert "lower_bound" in df.columns
    assert "upper_bound" in df.columns
    assert "bin_name" in df.columns


def test_histogram_distance_determine_metadata_df(spark, basic_narrow_db):
    """Test that determine_metadata_df returns a DataFrame with expected columns for HistogramDistance"""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    veh_spd = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")
    hist1 = HistogramDistance(
        name="test_distance_hist",
        desc="engine rpm with speed distance histogram",
        base_expr=eng_rpm,
        weights_expr=veh_spd,
        bins=[0.0, 10000.0],
    )
    hist2 = HistogramDistance(
        name="test_distance_hist2",
        base_expr=veh_spd,
        weights_expr=eng_rpm,
        bins=[0.0, 300.0],
    )

    df = HistogramDistance.determine_metadata_df(spark=spark, histograms=[hist1, hist2])
    assert df.count() == 2  # Ensure that metadata for two histograms is returned
    assert "page_number" in df.columns
    assert "name" in df.columns
    assert "description" in df.columns
    assert "agg_type" in df.columns
    assert "bins" in df.columns
    assert "values_unit" in df.columns
    assert "bins_unit" in df.columns
    assert "signal_expression" in df.columns
    assert "weights_expression" in df.columns
    assert "weights_channel_name" in df.columns


def test_histogram_distance_get_visual_id_column(spark, basic_narrow_db):
    """Test get_visual_id_column static method with list of HistogramDistance aggregations."""
    eng_rpm = basic_narrow_db.query.channel(channel_name="Engine RPM")
    veh_spd = basic_narrow_db.query.channel(channel_name="Vehicle Speed Sensor")

    hist1 = HistogramDistance(
        name="distance_hist_1",
        base_expr=eng_rpm,
        weights_expr=veh_spd,
        bins=[0, 500, 1000, 1500],
    )
    hist2 = HistogramDistance(
        name="distance_hist_2",
        base_expr=eng_rpm,
        weights_expr=veh_spd,
        bins=[0, 1000, 2000],
    )

    visuals = [hist1, hist2]

    col_expr = HistogramDistance.get_visual_id_column(visuals, "hist_name")

    test_data = [("distance_hist_1",), ("distance_hist_2",), ("unknown_hist",)]
    df = spark.createDataFrame(test_data, ["hist_name"])

    result_df = df.withColumn("visual_id", col_expr)
    results = result_df.collect()

    assert results[0]["visual_id"] == hist1.get_id()
    assert results[1]["visual_id"] == hist2.get_id()
    assert results[2]["visual_id"] is None


def test_histogram_distance_get_event():
    """Test get_event method for HistogramDistance"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    event_expr = TimeSeriesSelector(None)
    basic_event = BasicEvent(name="test_event", expr=event_expr)

    # Test with event
    hist_with_event = HistogramDistance(
        name="test",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
        event=basic_event,
    )
    assert hist_with_event.get_event() == basic_event

    # Test without event
    hist_without_event = HistogramDistance(
        name="test", base_expr=base_expr, weights_expr=weights_expr, bins=[0.0, 1.0]
    )
    assert hist_without_event.get_event() is None


def test_histogram_distance_get_id():
    """Test get_id method returns consistent unique identifier for HistogramDistance"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)

    hist1 = HistogramDistance(
        name="test_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
    )
    hist2 = HistogramDistance(
        name="test_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
    )
    hist3 = HistogramDistance(
        name="different_hist",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
    )

    # Same configuration should produce same ID
    assert hist1.get_id() == hist2.get_id()
    # Different name should produce different ID
    assert hist1.get_id() != hist3.get_id()
    # ID should be a positive integer
    assert isinstance(hist1.get_id(), int)
    assert hist1.get_id() > 0


def test_histogram_distance_inherits_from_custom_weights():
    """Test that HistogramDistance properly inherits from HistogramCustomWeights"""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    hist = HistogramDistance(
        name="test",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=[0.0, 1.0],
    )

    # Verify it's an instance of HistogramCustomWeights
    assert isinstance(hist, HistogramCustomWeights)
    # Verify the math function is set to diff
    assert hist.math_fct_for_weights == "diff"
    # Verify agg_type is set correctly
    assert hist.agg_type == "histogram_distance"


# ---------------------------------------------------------------------------
# Definition hash tests
# ---------------------------------------------------------------------------
def test_definition_hash_exists_in_as_dict():
    """Verify definition_hash key is present and non-null in as_dict output."""
    hist = HistogramDuration(
        name="hash_hist", base_expr=TimeSeriesSelector(None), bins=[0.0, 1.0, 2.0]
    )
    d = hist.as_dict()
    assert "definition_hash" in d
    assert d["definition_hash"] is not None
    assert isinstance(d["definition_hash"], int)


def test_definition_hash_exists_in_spark_row():
    """Verify definition_hash field is present in as_spark_row output."""
    hist = HistogramDuration(
        name="hash_hist", base_expr=TimeSeriesSelector(None), bins=[0.0, 1.0, 2.0]
    )
    row = hist.as_spark_row()
    assert hasattr(row, "definition_hash")
    assert row.definition_hash is not None
    assert isinstance(row.definition_hash, int)


def test_definition_hash_is_deterministic():
    """Same inputs must always produce the same hash."""
    base_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    hist1 = HistogramDuration(name="h", base_expr=base_expr, bins=bins)
    hist2 = HistogramDuration(name="h", base_expr=base_expr, bins=bins)
    assert hist1.determine_definition_hash() == hist2.determine_definition_hash()


def test_definition_hash_changes_with_bins():
    """Different bins must produce a different hash."""
    base_expr = TimeSeriesSelector(None)
    hist1 = HistogramDuration(name="h", base_expr=base_expr, bins=[0.0, 1.0, 2.0])
    hist2 = HistogramDuration(name="h", base_expr=base_expr, bins=[0.0, 5.0, 10.0])
    assert hist1.determine_definition_hash() != hist2.determine_definition_hash()


def test_definition_hash_ignores_name():
    """Hash must not change when only the name differs."""
    base_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    hist1 = HistogramDuration(name="name_a", base_expr=base_expr, bins=bins)
    hist2 = HistogramDuration(name="name_b", base_expr=base_expr, bins=bins)
    assert hist1.determine_definition_hash() == hist2.determine_definition_hash()


def test_definition_hash_ignores_description():
    """Hash must not change when only desc differs."""
    base_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    hist1 = HistogramDuration(name="h", base_expr=base_expr, bins=bins, desc="A")
    hist2 = HistogramDuration(name="h", base_expr=base_expr, bins=bins, desc="B")
    assert hist1.determine_definition_hash() == hist2.determine_definition_hash()


def test_definition_hash_changes_with_event():
    """Adding an event must change the hash."""
    base_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    event = BasicEvent(name="ev", expr=TimeSeriesSelector(None))
    hist_no_event = HistogramDuration(name="h", base_expr=base_expr, bins=bins)
    hist_with_event = HistogramDuration(name="h", base_expr=base_expr, bins=bins, event=event)
    assert hist_no_event.determine_definition_hash() != hist_with_event.determine_definition_hash()


def test_definition_hash_custom_weights_includes_weights():
    """HistogramCustomWeights hash must include the weights expression."""
    base_expr = TimeSeriesSelector(None)
    weights_expr = TimeSeriesSelector(None)
    bins = [0.0, 1.0, 2.0]
    hist_dur = HistogramDuration(name="h", base_expr=base_expr, bins=bins)
    hist_cw = HistogramCustomWeights(
        name="h",
        base_expr=base_expr,
        weights_expr=weights_expr,
        bins=bins,
    )
    # Duration and CustomWeights should differ because weights_expr is included
    assert hist_dur.determine_definition_hash() != hist_cw.determine_definition_hash()


def test_determine_aggregations_requires_solved_df(spark):
    hist = HistogramDuration(
        name="test_hist",
        base_expr=TimeSeriesSelector(None),
        bins=[0.0, 10000.0],
    )
    with pytest.raises(ValueError, match="requires solved_df"):
        HistogramDuration.determine_aggregations(spark=spark, aggregations=[hist])
