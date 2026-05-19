# pylint: disable=missing-docstring
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.query.solvers.delta_solver import DeltaSolver
from impulse_query_engine.measurement_db import MeasurementDB
from tests.conftest import narrow_db, spark


# pylint: disable-next=redefined-outer-name
def test_query1(spark: SparkSession, narrow_db: MeasurementDB):
    query = narrow_db.query
    c1 = query.channel(seed="0")
    res = query.select(c1.max().alias("max"), c1.min().alias("min")).solve(
        spark, solver=DeltaSolver(spark)
    )
    assert 1 == len(res.collect())
