# pylint: disable=missing-function-docstring

import os

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.query.solvers.key_value_store_solver import (
    KeyValueStoreSolver,
)
from impulse_query_engine.analyze.query.solvers.solver_config import (
    ChannelMappingConfig,
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB


def _solver(spark: SparkSession) -> KeyValueStoreSolver:
    return KeyValueStoreSolver(
        spark,
        config=SolverConfig(
            project_id="SAMPLE_PROJECT",
            container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
            channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
        ),
    )


def _expected_raw_values(channels_csv_path: str, container_id: int, channel_id: int) -> np.ndarray:
    raw = pd.read_csv(channels_csv_path)
    rows = raw[(raw["container_id"] == container_id) & (raw["channel_id"] == channel_id)]
    return rows.sort_values("tstart")["value"].values.astype(np.float64)


@pytest.fixture
def channels_csv_path() -> str:
    base_path = os.path.dirname(os.path.abspath(__file__))
    base_path = base_path[: base_path.find("tests")]
    return f"{base_path}/tests/unit/data/basic_narrow_csv/channel_data.csv"


class TestUnitConversionSolve:
    def test_solve_with_unit_conversion(
        self,
        spark: SparkSession,
        key_value_store_unit_conversion_db: MeasurementDB,
        channels_csv_path: str,
    ):
        solver = _solver(spark)
        query = key_value_store_unit_conversion_db.query
        vehicle_speed = query.channel_with_alias(channel_alias="vehicle_speed").alias(
            "vehicle_speed"
        )

        pdf = query.select(vehicle_speed).toPandas(spark, solver=solver)
        pdf = pdf.sort_values("container_id").reset_index(drop=True)

        assert pdf["container_id"].tolist() == [1, 2, 3]

        factor = 0.277778
        # Containers 1 and 2 resolve vehicle_speed -> "Vehicle Speed Sensor" (channel 7).
        for cid in (1, 2):
            expected = _expected_raw_values(channels_csv_path, cid, 7) * factor
            row = pdf.loc[pdf["container_id"] == cid].iloc[0]
            np.testing.assert_allclose(row.vehicle_speed.values, expected, rtol=1e-6)

        # Container 3 resolves to channel 7 via Spd_Vhcl / ProjSpecREC_10Hz.
        expected3 = _expected_raw_values(channels_csv_path, 3, 7) * factor
        row3 = pdf.loc[pdf["container_id"] == 3].iloc[0]
        np.testing.assert_allclose(row3.vehicle_speed.values, expected3, rtol=1e-6)

    def test_solve_no_conversion_when_same_unit(
        self,
        spark: SparkSession,
        key_value_store_unit_conversion_db: MeasurementDB,
        channels_csv_path: str,
    ):
        solver = _solver(spark)
        query = key_value_store_unit_conversion_db.query
        engine_speed = query.channel_with_alias(channel_alias="engine_speed").alias("engine_speed")

        pdf = query.select(engine_speed).toPandas(spark, solver=solver)
        pdf = pdf.sort_values("container_id").reset_index(drop=True)

        for cid in (1, 2, 3):
            expected = _expected_raw_values(channels_csv_path, cid, 5)
            row = pdf.loc[pdf["container_id"] == cid].iloc[0]
            np.testing.assert_allclose(row.engine_speed.values, expected, rtol=1e-12)

    def test_solve_no_conversion_when_table_not_configured(
        self,
        spark: SparkSession,
        key_value_store_unit_conversion_db_no_table: MeasurementDB,
        channels_csv_path: str,
    ):
        solver = _solver(spark)
        query = key_value_store_unit_conversion_db_no_table.query
        vehicle_speed = query.channel_with_alias(channel_alias="vehicle_speed").alias(
            "vehicle_speed"
        )

        pdf = query.select(vehicle_speed).toPandas(spark, solver=solver)
        pdf = pdf.sort_values("container_id").reset_index(drop=True)

        # No conversion: values are returned exactly as-is from the raw channel data.
        for cid in (1, 2, 3):
            expected = _expected_raw_values(channels_csv_path, cid, 7)
            row = pdf.loc[pdf["container_id"] == cid].iloc[0]
            np.testing.assert_allclose(row.vehicle_speed.values, expected, rtol=1e-12)

    def test_solve_no_conversion_for_direct_selectors(
        self,
        spark: SparkSession,
        key_value_store_unit_conversion_db: MeasurementDB,
        channels_csv_path: str,
    ):
        solver = _solver(spark)
        query = key_value_store_unit_conversion_db.query
        # Direct selector — no alias, so no unit metadata, so no conversion.
        vehicle_speed_direct = query.channel(
            channel_name="Vehicle Speed Sensor", data_key="TM"
        ).alias("vehicle_speed_direct")

        pdf = query.select(vehicle_speed_direct).toPandas(spark, solver=solver)
        pdf = pdf.sort_values("container_id").reset_index(drop=True)

        for cid in (1, 2):
            expected = _expected_raw_values(channels_csv_path, cid, 7)
            row = pdf.loc[pdf["container_id"] == cid].iloc[0]
            np.testing.assert_allclose(row.vehicle_speed_direct.values, expected, rtol=1e-12)

    def test_solve_same_channel_direct_stays_raw_aliased_converts(
        self,
        spark: SparkSession,
        key_value_store_unit_conversion_db: MeasurementDB,
        channels_csv_path: str,
    ):
        # When a direct selector and an aliased selector resolve to the same
        # (container_id, channel_id) (both land on channel 7), conversion is a
        # property of the alias — the direct selector returns raw values,
        # the aliased selector returns raw * factor (km/h -> m/s).
        solver = _solver(spark)
        query = key_value_store_unit_conversion_db.query
        direct = query.channel(channel_name="Vehicle Speed Sensor", data_key="TM").alias(
            "vehicle_speed_raw"
        )
        aliased = query.channel_with_alias(channel_alias="vehicle_speed").alias(
            "vehicle_speed_converted"
        )

        pdf = query.select(direct, aliased).toPandas(spark, solver=solver)
        pdf = pdf.sort_values("container_id").reset_index(drop=True)

        factor = 0.277778
        for cid in (1, 2):
            raw = _expected_raw_values(channels_csv_path, cid, 7)
            row = pdf.loc[pdf["container_id"] == cid].iloc[0]
            np.testing.assert_allclose(row.vehicle_speed_raw.values, raw, rtol=1e-12)
            np.testing.assert_allclose(row.vehicle_speed_converted.values, raw * factor, rtol=1e-6)

    def test_solve_mixed_direct_and_aliased_disjoint_channels(
        self,
        spark: SparkSession,
        key_value_store_unit_conversion_db: MeasurementDB,
        channels_csv_path: str,
    ):
        # Direct selector targets a *different* channel than the aliased one.
        # Direct: Ambient Air Temperature (channel 6, no conversion).
        # Aliased: vehicle_speed (channel 7, km/h -> m/s).
        #
        # Note: when a direct selector and an aliased selector resolve to the
        # same (container_id, channel_id), the conversion factor stored on the
        # channel row applies to both — the per-channel factor model in
        # KVSTimeSeriesCache cannot distinguish callers.  We therefore only
        # cover the disjoint case here.
        solver = _solver(spark)
        query = key_value_store_unit_conversion_db.query
        direct = query.channel(channel_name="Ambient Air Temperature", data_key="TM").alias(
            "ambient_temp"
        )
        aliased = query.channel_with_alias(channel_alias="vehicle_speed").alias(
            "vehicle_speed_converted"
        )

        pdf = query.select(direct, aliased).toPandas(spark, solver=solver)
        pdf = pdf.sort_values("container_id").reset_index(drop=True)

        factor = 0.277778
        for cid in (1, 2):
            ambient_raw = _expected_raw_values(channels_csv_path, cid, 6)
            speed_raw = _expected_raw_values(channels_csv_path, cid, 7)
            row = pdf.loc[pdf["container_id"] == cid].iloc[0]
            np.testing.assert_allclose(row.ambient_temp.values, ambient_raw, rtol=1e-12)
            np.testing.assert_allclose(
                row.vehicle_speed_converted.values, speed_raw * factor, rtol=1e-6
            )

    def test_solve_cross_family_units_leave_values_unchanged(
        self,
        spark: SparkSession,
        key_value_store_unit_conversion_db: MeasurementDB,
        channels_csv_path: str,
    ):
        # cross_family_alias maps Engine RPM (rotation family) -> m/s
        # (speed family). The group_id mismatch makes the target-side join
        # miss, leaving conversion_factor null and values unchanged.
        solver = _solver(spark)
        query = key_value_store_unit_conversion_db.query
        cross = query.channel_with_alias(channel_alias="cross_family_alias").alias("cross")

        pdf = query.select(cross).toPandas(spark, solver=solver)
        pdf = pdf.sort_values("container_id").reset_index(drop=True)

        # The mapping only references Engine RPM/TM, which exists for containers 1 and 2.
        for cid in (1, 2):
            expected = _expected_raw_values(channels_csv_path, cid, 5)
            row = pdf.loc[pdf["container_id"] == cid].iloc[0]
            np.testing.assert_allclose(row.cross.values, expected, rtol=1e-12)


class TestComputeConversionFactors:
    def test_factor_one_for_identical_units(
        self, spark: SparkSession, key_value_store_unit_conversion_db: MeasurementDB
    ):
        solver = _solver(spark)
        query = key_value_store_unit_conversion_db.query

        channels_df = spark.createDataFrame(
            [(1, 5, "RPM", "RPM"), (2, 5, "RPM", "RPM")],
            schema=["container_id", "channel_id", "source_unit", "target_unit"],
        )

        result = solver._compute_conversion_factors(spark, query, channels_df).collect()
        factors = {row.container_id: row.conversion_factor for row in result}
        assert pytest.approx(factors[1], rel=1e-12) == 1.0
        assert pytest.approx(factors[2], rel=1e-12) == 1.0

    def test_factor_for_known_speed_conversion(
        self, spark: SparkSession, key_value_store_unit_conversion_db: MeasurementDB
    ):
        solver = _solver(spark)
        query = key_value_store_unit_conversion_db.query

        channels_df = spark.createDataFrame(
            [(1, 7, "km/h", "m/s")],
            schema=["container_id", "channel_id", "source_unit", "target_unit"],
        )

        row = solver._compute_conversion_factors(spark, query, channels_df).collect()[0]
        assert row.conversion_factor == pytest.approx(0.277778, rel=1e-6)

    def test_null_factor_for_cross_family(
        self, spark: SparkSession, key_value_store_unit_conversion_db: MeasurementDB
    ):
        solver = _solver(spark)
        query = key_value_store_unit_conversion_db.query

        channels_df = spark.createDataFrame(
            [(1, 5, "RPM", "m/s")],
            schema=["container_id", "channel_id", "source_unit", "target_unit"],
        )

        row = solver._compute_conversion_factors(spark, query, channels_df).collect()[0]
        assert row.conversion_factor is None

    def test_null_factor_for_unknown_unit(
        self, spark: SparkSession, key_value_store_unit_conversion_db: MeasurementDB
    ):
        solver = _solver(spark)
        query = key_value_store_unit_conversion_db.query

        channels_df = spark.createDataFrame(
            [(1, 5, "furlongs/fortnight", "m/s")],
            schema=["container_id", "channel_id", "source_unit", "target_unit"],
        )

        row = solver._compute_conversion_factors(spark, query, channels_df).collect()[0]
        assert row.conversion_factor is None
