from mda_query_engine.analyze.query.solvers.basic_narrow_solver import BasicNarrowSolver
from mda_query_engine.analyze.query.solvers.delta_solver import DeltaSolver


def test_full(spark, narrow_db):
    query = narrow_db.query

    c1 = query.channel(seed="0")
    c2 = query.channel(seed="0")

    expr1 = c1.sum().alias("test")
    expr2 = c2.max().alias("test2")

    df = query.select(expr1, expr2).toPandas(spark, solver=DeltaSolver(spark))
    assert len(df) == 1


def test_basic_narrow_full(spark, basic_narrow_db):
    query = basic_narrow_db.query

    c1 = query.channel(channel_name="Engine RPM")
    c2 = query.channel(channel_name="Vehicle Speed Sensor")

    expr1 = c1.max().alias("eng_rpm_max")
    expr2 = c2.max().alias("veh_spd_max")

    df = query.select(expr1, expr2).solve(spark, solver=BasicNarrowSolver(spark))
    assert df.count() == 3
    assert df.select("eng_rpm_max").collect()[0]["eng_rpm_max"] >= 0
