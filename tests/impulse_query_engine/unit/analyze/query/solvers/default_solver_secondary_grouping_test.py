# pylint: disable=missing-function-docstring
"""Tests for the optional secondary grouping key on ``DefaultSolver.solve``.

Covers both configuration modes (an existing ``source_column`` and a
``Column``-returning ``deriver``), that the key becomes an output dimension
(one row per ``(container_id, secondary_grouping_key)``), that computed values
match a manual per-partition duration-weighted mean, backward compatibility
when unconfigured, and the dynamic ``container_id`` type invariant.
"""

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.solver_config import (
    SecondaryGroupingConfig,
    SolverConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import basic_narrow_db, spark

CHANNEL = "Vehicle Speed Sensor"


def _db_with_channels(base: MeasurementDB, channels_df) -> MeasurementDB:
    tables = dict(base.config.debug_tables)
    tables["channels"] = channels_df
    return MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=base.ws)


def _expected_partition_means(base: MeasurementDB) -> dict:
    """Duration-weighted mean per ``(container_id, tstart % 2)`` for the VSS channel.

    Restricted to containers whose VSS selection is unambiguous (exactly one
    matching channel), matching the engine's arrival-order-independent pick.
    """
    metrics = base.config.debug_tables["channel_metrics"].toPandas()
    channels = base.config.debug_tables["channels"].toPandas()
    channels["_key"] = channels["tstart"] % 2
    vss = metrics[metrics["channel_name"] == CHANNEL]
    counts = vss.groupby("container_id").size()
    unambiguous = set(counts[counts == 1].index)
    assert unambiguous, "fixture must contain at least one unambiguous container"

    expected = {}
    for _, row in vss[vss["container_id"].isin(unambiguous)].iterrows():
        s = channels[
            (channels["container_id"] == row["container_id"])
            & (channels["channel_id"] == row["channel_id"])
        ]
        for key, grp in s.groupby("_key"):
            d = grp["tend"] - grp["tstart"]
            if d.sum() > 0:
                expected[(row["container_id"], int(key))] = float(
                    (grp["value"] * d).sum() / d.sum()
                )
    assert expected
    return expected


class TestSecondaryGroupingKey:
    def test_source_column_mode_subdivides_and_matches_manual(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """An existing column used as the key subdivides results per (container, key)."""
        channels = basic_narrow_db.config.debug_tables["channels"].withColumn(
            "day", (F.col("tstart") % F.lit(2)).cast("long")
        )
        db = _db_with_channels(basic_narrow_db, channels)
        cfg = SolverConfig(secondary_grouping=SecondaryGroupingConfig(source_column="day"))

        query = db.query
        result = query.select(query.channel(channel_name=CHANNEL).mean().alias("m")).solve(
            spark, solver=DefaultSolver(spark, config=cfg)
        )

        assert "secondary_grouping_key" in result.columns
        rows = result.collect()
        means = {(r.container_id, r.secondary_grouping_key): r.m for r in rows}

        expected = _expected_partition_means(basic_narrow_db)
        assert means.keys() >= expected.keys()
        for key, exp in expected.items():
            assert means[key] == pytest.approx(exp), key
        # The key actually took more than one value (real subdivision), not a
        # single constant partition per container.
        assert len({r.secondary_grouping_key for r in rows}) >= 2

    def test_deriver_mode_matches_source_column_mode(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """A Column-returning deriver produces the same partitioning as the column mode."""
        cfg = SolverConfig(
            secondary_grouping=SecondaryGroupingConfig(
                deriver=lambda df: (F.col("tstart") % F.lit(2)).cast("long")
            )
        )
        query = basic_narrow_db.query
        result = query.select(query.channel(channel_name=CHANNEL).mean().alias("m")).solve(
            spark, solver=DefaultSolver(spark, config=cfg)
        )

        assert "secondary_grouping_key" in result.columns
        means = {(r.container_id, r.secondary_grouping_key): r.m for r in result.collect()}
        expected = _expected_partition_means(basic_narrow_db)
        for key, exp in expected.items():
            assert means[key] == pytest.approx(exp), key

    def test_unconfigured_is_backward_compatible(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """With no secondary grouping configured, the key column is absent."""
        query = basic_narrow_db.query
        result = query.select(query.channel(channel_name=CHANNEL).mean().alias("m")).solve(
            spark, solver=DefaultSolver(spark)
        )
        assert "secondary_grouping_key" not in result.columns
        assert result.count() > 0

    def test_string_container_id_with_secondary_grouping(
        self, spark: SparkSession, basic_narrow_db: MeasurementDB
    ):
        """container_id type is derived dynamically and preserved alongside the key."""
        tables = {
            name: df.withColumn("container_id", F.col("container_id").cast(T.StringType()))
            if "container_id" in df.columns
            else df
            for name, df in basic_narrow_db.config.debug_tables.items()
        }
        db = MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=basic_narrow_db.ws)
        cfg = SolverConfig(
            secondary_grouping=SecondaryGroupingConfig(
                deriver=lambda df: (F.col("tstart") % F.lit(2)).cast("long")
            )
        )
        query = db.query
        result = query.select(query.channel(channel_name=CHANNEL).mean().alias("m")).solve(
            spark, solver=DefaultSolver(spark, config=cfg)
        )
        assert dict(result.dtypes)["container_id"] == "string"
        assert "secondary_grouping_key" in result.columns
        assert result.count() > 0
