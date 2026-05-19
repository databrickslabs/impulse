# pylint: disable=missing-function-docstring
from datetime import datetime

from pyspark.sql import SparkSession

from impulse_query_engine.analyze.metadata.metric_expression import MetricSelector
from impulse_query_engine.analyze.query.solvers.delta_solver import DeltaSolver
from impulse_query_engine.measurement_db import MeasurementDB
from tests.conftest import narrow_db, spark


def test_metric_expression1(spark: SparkSession, narrow_db: MeasurementDB):
    q = narrow_db.query
    date_filter = q.metric("start_dt") > datetime.fromisoformat("2023-08-15T00:00:000Z")
    q.where(date_filter)
    series1 = q.channel(seed=0)
    res = q.select(series1.alias("test")).toPandas(spark, solver=DeltaSolver(spark))
    assert len(res) == 1


def test_metric_expression2(spark: SparkSession, narrow_db: MeasurementDB):
    q = narrow_db.query
    date_filter = q.metric("start_dt") > datetime.fromisoformat("2023-08-16T00:00:000Z")
    q.where(date_filter)
    series1 = q.channel(seed=0)
    res = q.select(series1.alias("test")).toPandas(spark, solver=DeltaSolver(spark))
    assert len(res) == 0


def test_metric_expression3(spark: SparkSession, narrow_db: MeasurementDB):
    q = narrow_db.query
    date_filter = (q.metric("start_dt") > datetime.fromisoformat("2023-08-15T00:00:000Z")) & (
        q.metric("stop_dt") < datetime.fromisoformat("2023-08-15T14:00:000Z")
    )
    q.where(date_filter)
    series1 = q.channel(seed=0)
    res = q.select(series1.alias("test")).toPandas(spark, solver=DeltaSolver(spark))
    assert len(res) == 1


def test_metric_expression4(spark: SparkSession, narrow_db: MeasurementDB):
    q = narrow_db.query
    min_duration_filter = q.metric("duration_ms") > 100000
    q.where(min_duration_filter)
    series1 = q.channel(seed=0)
    res = q.select(series1.alias("test")).toPandas(spark, solver=DeltaSolver(spark))
    assert len(res) == 0


def test_metric_expression5(spark: SparkSession, narrow_db: MeasurementDB):
    q = narrow_db.query
    date_filter = (q.metric("start_dt") >= datetime.fromisoformat("2023-08-15T00:00:000Z")) & (
        q.metric("stop_dt") <= datetime.fromisoformat("2023-08-15T14:00:000Z")
    )
    min_duration_filter = q.metric("duration_ms") != 100000
    # filters are concatenated via a logical 'or' statements.
    # The date filter will be evaluated to True and we expect the sample series as a result.
    q.where([date_filter, min_duration_filter])
    print(min_duration_filter.get_selector_expr())
    series1 = q.channel(seed=0)
    res = q.select(series1.alias("test")).toPandas(spark, solver=DeltaSolver(spark))
    assert len(res) == 1


def test_metric_expression6(spark: SparkSession, narrow_db: MeasurementDB):
    q = narrow_db.query
    my_filter = (
        (q.metric("start_dt") >= datetime.fromisoformat("2023-08-15T00:00:000Z"))
        & (q.metric("stop_dt") <= datetime.fromisoformat("2023-08-15T14:00:000Z"))
        & (q.metric("duration_ms") != 100000)
    )
    q.where(my_filter)
    series1 = q.channel(seed=0)
    res = q.select(series1.alias("test")).toPandas(spark, solver=DeltaSolver(spark))
    assert len(res) == 0


def test_metric_selector_required_metrics():
    expr = MetricSelector("vehicle_key")
    assert expr.required_metrics() == {"vehicle_key"}


def test_metric_op_required_metrics_single():
    expr = MetricSelector("brand") == "Seat"
    assert expr.required_metrics() == {"brand"}


def test_metric_op_required_metrics_multiple():
    expr = (MetricSelector("brand") == "Seat") & (MetricSelector("model") == "Leon")
    assert expr.required_metrics() == {"brand", "model"}


def test_metric_op_required_metrics_or():
    expr = (MetricSelector("brand") == "Seat") | (MetricSelector("brand") == "VW")
    assert expr.required_metrics() == {"brand"}


def test_metric_op_required_metrics_nested():
    expr = ((MetricSelector("brand") == "Seat") & (MetricSelector("model") == "Leon")) | (
        MetricSelector("environment") == "test"
    )
    assert expr.required_metrics() == {"brand", "model", "environment"}
