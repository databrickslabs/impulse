# pylint: disable=missing-docstring
import numpy as np
from pyspark.sql import SparkSession

from mda_query_engine.analyze.query.solvers.delta_solver import DeltaSolver
from mda_query_engine.measurement_db import MeasurementDB
from tests.conftest import narrow_db, spark


# pylint: disable-next=redefined-outer-name
def test_histogram(spark: SparkSession, narrow_db: MeasurementDB):
    q = narrow_db.query
    series1 = q.channel(seed=0)
    q1 = q.select(series1.histogram([-np.inf, -1.0, 0.0, 1.0, np.inf]).alias("hist1"))
    result = q1.toPandas(spark, solver=DeltaSolver(spark))
    assert 10.0 == sum(result["hist1"].iloc[0][0])


def test_histogram2d(spark: SparkSession, narrow_db: MeasurementDB):
    q = narrow_db.query
    series1 = q.channel(seed=0)
    series2 = q.channel(seed=0)

    x_bins = [-np.inf, -1.0, 0.0, 1.0, np.inf]
    y_bins = [-np.inf, -1.0, 0.0, 1.0, np.inf]

    q1 = q.select(series1.histogram2d(series2, x_bins=x_bins, y_bins=y_bins).alias("hist1"))
    result = q1.toPandas(spark, solver=DeltaSolver(spark))
    assert 10.0 == sum(result["hist1"].iloc[0][0][3])


def test_histogram_custom_weights(spark: SparkSession, narrow_db: MeasurementDB):
    q = narrow_db.query
    series1 = q.channel(seed=0)
    series2 = q.channel(seed=0)

    bins = [-np.inf, -1.0, 0.0, 1.0, np.inf]

    q1 = q.select(
        series1.histogram_custom_weights(
            bins=bins,
            weights=series2,
            channel_interp_kind="previous",
            weights_interp_kind="previous",
            math_fct_for_weights=None,
            math_fct_kwargs={},
        ).alias("hist_custom")
    )
    result = q1.toPandas(spark, solver=DeltaSolver(spark))

    # Verify result structure
    assert "hist_custom" in result.columns
    assert len(result) > 0
    # Verify histogram values exist
    hist_values = result["hist_custom"].iloc[0][0]
    assert len(hist_values) == len(bins) - 1  # Number of bins minus 1


def test_histogram_custom_weights_with_math_function(
    spark: SparkSession, narrow_db: MeasurementDB
):
    q = narrow_db.query
    series1 = q.channel(seed=0)
    series2 = q.channel(seed=0)

    bins = [-np.inf, -1.0, 0.0, 1.0, np.inf]

    q1 = q.select(
        series1.histogram_custom_weights(
            bins=bins,
            weights=series2,
            channel_interp_kind="previous",
            weights_interp_kind="previous",
            math_fct_for_weights="diff",
            math_fct_kwargs={},
        ).alias("hist_custom_diff")
    )
    result = q1.toPandas(spark, solver=DeltaSolver(spark))

    # Verify result structure
    assert "hist_custom_diff" in result.columns
    assert len(result) > 0
    # Verify histogram values exist
    hist_values = result["hist_custom_diff"].iloc[0][0]
    assert len(hist_values) == len(bins) - 1  # Number of bins minus 1


def test_histogram2d_custom_weights(spark: SparkSession, narrow_db: MeasurementDB):
    q = narrow_db.query
    series1 = q.channel(seed=0)
    series2 = q.channel(seed=0)
    weights = q.channel(seed=0)

    x_bins = [-np.inf, -1.0, 0.0, 1.0, np.inf]
    y_bins = [-np.inf, -1.0, 0.0, 1.0, np.inf]

    q1 = q.select(
        series1.histogram2d_custom_weights(
            series2,
            weights_selection=weights,
            x_bins=x_bins,
            y_bins=y_bins,
            channel_interp_kind="previous",
            weights_interp_kind="previous",
            math_fct_for_weights=None,
            math_fct_kwargs={},
        ).alias("hist2d_custom")
    )
    result = q1.toPandas(spark, solver=DeltaSolver(spark))

    # Verify result structure
    assert "hist2d_custom" in result.columns
    assert len(result) > 0
    # Verify histogram values exist
    hist_values = result["hist2d_custom"].iloc[0][0]
    assert len(hist_values) == len(x_bins) - 1  # Number of x bins minus 1


def test_histogram2d_custom_weights_with_math_function(
    spark: SparkSession, narrow_db: MeasurementDB
):
    q = narrow_db.query
    series1 = q.channel(seed=0)
    series2 = q.channel(seed=0)
    weights = q.channel(seed=0)

    x_bins = [-np.inf, -1.0, 0.0, 1.0, np.inf]
    y_bins = [-np.inf, -1.0, 0.0, 1.0, np.inf]

    q1 = q.select(
        series1.histogram2d_custom_weights(
            series2,
            weights_selection=weights,
            x_bins=x_bins,
            y_bins=y_bins,
            channel_interp_kind="previous",
            weights_interp_kind="previous",
            math_fct_for_weights="diff",
            math_fct_kwargs={},
        ).alias("hist2d_custom_diff")
    )
    result = q1.toPandas(spark, solver=DeltaSolver(spark))

    # Verify result structure
    assert "hist2d_custom_diff" in result.columns
    assert len(result) > 0
    # Verify histogram values exist
    hist_values = result["hist2d_custom_diff"].iloc[0][0]
    assert len(hist_values) == len(x_bins) - 1  # Number of x bins minus 1
