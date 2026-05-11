from pyspark.sql.types import BinaryType

from mda_query_engine.analyze.query.solvers.delta_solver import DeltaSolver
from mda_query_engine.analyze.query.solvers.key_value_store_solver import KeyValueStoreSolver
from mda_query_engine.model.series.sample_series import SampleSeries


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

    df = query.select(expr1, expr2).solve(spark, solver=KeyValueStoreSolver(spark))
    assert df.count() == 3
    assert df.select("eng_rpm_max").collect()[0]["eng_rpm_max"] >= 0


def test_sample_series_returned_as_sample_series(spark, basic_narrow_db):
    query = basic_narrow_db.query

    rpm = query.channel(channel_name="Engine RPM")
    expr = rpm.alias("rpm_series")

    pdf = query.select(expr).toPandas(spark, solver=KeyValueStoreSolver(spark))
    assert len(pdf) == 3

    for _, row in pdf.iterrows():
        series_data = row["rpm_series"]
        assert isinstance(series_data, SampleSeries)
        assert len(series_data) > 0


def test_sample_series_solve_returns_binary(spark, basic_narrow_db):
    query = basic_narrow_db.query

    rpm = query.channel(channel_name="Engine RPM")
    expr = rpm.alias("rpm_series")

    df = query.select(expr).solve(spark, solver=KeyValueStoreSolver(spark))
    assert df.count() == 3

    schema_field = df.schema["rpm_series"]
    assert isinstance(schema_field.dataType, BinaryType)


def test_sample_series_mixed_with_scalar(spark, basic_narrow_db):
    query = basic_narrow_db.query

    rpm = query.channel(channel_name="Engine RPM")
    expr_series = rpm.alias("rpm_series")
    expr_max = rpm.max().alias("rpm_max")

    pdf = query.select(expr_series, expr_max).toPandas(spark, solver=KeyValueStoreSolver(spark))
    assert len(pdf) == 3

    for _, row in pdf.iterrows():
        assert isinstance(row["rpm_series"], SampleSeries)
        assert len(row["rpm_series"]) > 0
        assert isinstance(row["rpm_max"], float)
        assert row["rpm_max"] >= 0
