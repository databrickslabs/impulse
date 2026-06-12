# pylint: disable=missing-function-docstring
"""``container_id`` may be any Spark type, not just ``LongType``.

The solvers derive the ``container_id`` type of their grouped-map UDF output
schema (and of the empty channel-match frame) from the source tables instead
of hardcoding ``LongType``.  These tests reuse the existing solver fixtures and
simply recast ``container_id`` to ``StringType`` / ``IntegerType`` before
solving, then assert the type is preserved end-to-end.

See ``QuerySolver._build_solve_output_schema`` and
``QuerySolver._empty_channel_match_df``.
"""

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest

from impulse_query_engine.analyze.query.solvers.delta_solver import DeltaSolver
from impulse_query_engine.analyze.query.solvers.key_value_store_solver import (
    KeyValueStoreSolver,
)
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import basic_narrow_db, narrow_db, spark  # noqa: F401  (pytest fixtures)

# StringType / IntegerType are the new cases; LongType locks in backward compatibility.
_CID_TYPES = [T.StringType(), T.IntegerType(), T.LongType()]


def _recast_container_id(db: MeasurementDB, cid_type: T.DataType) -> MeasurementDB:
    """Clone a ``for_debug`` db, casting ``container_id`` to *cid_type* on every table."""
    tables = {
        name: (
            df.withColumn("container_id", F.col("container_id").cast(cid_type))
            if "container_id" in df.columns
            else df
        )
        for name, df in db.config.debug_tables.items()
    }
    return MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=db.ws)


@pytest.mark.parametrize("cid_type", _CID_TYPES, ids=lambda t: t.simpleString())
def test_delta_solve_preserves_container_id_type(spark, narrow_db, cid_type):
    db = _recast_container_id(narrow_db, cid_type)
    query = db.query
    result = query.select(query.channel(seed="0").mean().alias("m")).solve(
        spark, solver=DeltaSolver(spark)
    )
    assert result.schema["container_id"].dataType == cid_type
    means = [row.m for row in result.collect()]
    assert any(m is not None and m != 0 for m in means), means


@pytest.mark.parametrize("cid_type", _CID_TYPES, ids=lambda t: t.simpleString())
def test_kvs_solve_preserves_container_id_type(spark, basic_narrow_db, cid_type):
    db = _recast_container_id(basic_narrow_db, cid_type)
    query = db.query
    result = query.select(
        query.channel(channel_name="Vehicle Speed Sensor").mean().alias("m")
    ).solve(spark, solver=KeyValueStoreSolver(spark))
    assert result.schema["container_id"].dataType == cid_type
    means = [row.m for row in result.collect()]
    assert any(m is not None and m != 0 for m in means), means


@pytest.mark.parametrize("solver_cls", [DeltaSolver, KeyValueStoreSolver])
def test_empty_channel_match_df_derives_types(spark, basic_narrow_db, solver_cls):
    """The empty channel-match frame derives its id type from channel_metrics."""
    db = _recast_container_id(basic_narrow_db, T.StringType())
    empty = solver_cls(spark)._empty_channel_match_df(spark, db)
    assert empty.schema["container_id"].dataType == T.StringType()
    assert "channel_id" in empty.columns
    assert empty.count() == 0


def test_kvs_solve_empty_result_preserves_type(spark, basic_narrow_db):
    """KeyValueStoreSolver's ``container_count == 0`` path keeps the string type."""
    db = _recast_container_id(basic_narrow_db, T.StringType())
    query = db.query
    result = query.select(
        query.channel(channel_name="Nonexistent Channel").mean().alias("m")
    ).solve(spark, solver=KeyValueStoreSolver(spark))
    assert result.schema["container_id"].dataType == T.StringType()
    assert result.count() == 0
