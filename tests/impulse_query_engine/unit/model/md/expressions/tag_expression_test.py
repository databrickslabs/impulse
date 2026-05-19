# pylint: disable=missing-function-docstring
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.query.solvers.delta_solver import DeltaSolver
from impulse_query_engine.measurement_db import MeasurementDB
from tests.conftest import narrow_db, spark


def test_select_ts(spark: SparkSession, narrow_db: MeasurementDB):
    q = narrow_db.query
    series1 = q.channel(seed=0)
    res = q.select(series1.alias("test")).toPandas(spark, solver=DeltaSolver(spark))
    assert len(res) == 1
    ts = res.test.iloc[0]
    assert len(ts) == 10


def test_hash():
    expr1 = TagSelector("a")
    expr2 = TagSelector("b")
    assert hash(expr1) != hash(expr2)
    expr3 = expr1 == "test"
    expr4 = expr2 == "test"
    assert hash(expr3) != hash(expr4)
    expr5 = expr1 == "test1"
    expr6 = expr1 == "test2"
    assert hash(expr5) != hash(expr6)
    expr7 = (expr1 == "test1") and (expr2 == "test2")
    expr8 = (expr1 == "test1") or (expr2 == "test2")
    assert hash(expr7) != hash(expr8)
