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
        # Containers 1 and 2 resolve vehicle_speed -> "Vehicle Speed Sensor" (channel 7);
        # channel_metrics.unit == "km/h" matches channel_mapping.source_unit, so the
        # coalesce yields "km/h" and values scale by ~0.277778 to reach m/s.
        for cid in (1, 2):
            expected = _expected_raw_values(channels_csv_path, cid, 7) * factor
            row = pdf.loc[pdf["container_id"] == cid].iloc[0]
            np.testing.assert_allclose(row.vehicle_speed.values, expected, rtol=1e-6)

        # Container 3 resolves to channel 7 via Spd_Vhcl / ProjSpecREC_10Hz. Its
        # channel_metrics.unit is "m/s" (overrides channel_mapping.source_unit="km/h"
        # via COALESCE), and target_unit is also "m/s", so the conversion factor is
        # 1.0 and values are unchanged from raw.
        expected3 = _expected_raw_values(channels_csv_path, 3, 7)
        row3 = pdf.loc[pdf["container_id"] == 3].iloc[0]
        np.testing.assert_allclose(row3.vehicle_speed.values, expected3, rtol=1e-12)

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


class TestSourceUnitResolution:
    """Effective source_unit = COALESCE(channel_metrics.unit, channel_mapping.source_unit)."""

    def test_source_unit_from_channel_metrics_overrides_mapping(
        self,
        spark: SparkSession,
        key_value_store_unit_conversion_db: MeasurementDB,
        channels_csv_path: str,
    ):
        # Container 3's Spd_Vhcl row has channel_metrics.unit = "m/s" while the
        # mapping's source_unit is "km/h".  Coalesce yields "m/s"; mapping's
        # target_unit is also "m/s"; effective factor = 1.0 (no scaling).
        solver = _solver(spark)
        query = key_value_store_unit_conversion_db.query
        vehicle_speed = query.channel_with_alias(channel_alias="vehicle_speed").alias(
            "vehicle_speed"
        )

        pdf = query.select(vehicle_speed).toPandas(spark, solver=solver)
        row3 = pdf.loc[pdf["container_id"] == 3].iloc[0]
        expected = _expected_raw_values(channels_csv_path, 3, 7)
        np.testing.assert_allclose(row3.vehicle_speed.values, expected, rtol=1e-12)

    def test_source_unit_falls_back_to_mapping_when_metrics_unit_null(
        self,
        spark: SparkSession,
        key_value_store_unit_conversion_db: MeasurementDB,
        channels_csv_path: str,
    ):
        # Containers 1 and 2 have channel_metrics.unit = "km/h" (which equals
        # the mapping's source_unit, so they coalesce identically). To
        # exercise the null-fallback specifically, construct a custom
        # channel_metrics where the unit cell is null for the row of interest
        # — the coalesce must then return the mapping's source_unit.
        from pyspark.sql import functions as F  # noqa: PLR0402  local import

        # Replace the unit cell on (cid=1, ch=7) with null.
        cm = key_value_store_unit_conversion_db.config.debug_tables["channel_metrics"]
        cm_null = cm.withColumn(
            "unit",
            F.when(
                (F.col("container_id") == 1) & (F.col("channel_id") == 7),
                F.lit(None).cast("string"),
            ).otherwise(F.col("unit")),
        )
        key_value_store_unit_conversion_db.config.debug_tables["channel_metrics"] = cm_null

        try:
            solver = _solver(spark)
            query = key_value_store_unit_conversion_db.query
            vehicle_speed = query.channel_with_alias(channel_alias="vehicle_speed").alias(
                "vehicle_speed"
            )
            pdf = query.select(vehicle_speed).toPandas(spark, solver=solver)

            # Container 1: unit null → fall back to mapping source_unit="km/h"
            # → factor 0.277778.
            expected = _expected_raw_values(channels_csv_path, 1, 7) * 0.277778
            row1 = pdf.loc[pdf["container_id"] == 1].iloc[0]
            np.testing.assert_allclose(row1.vehicle_speed.values, expected, rtol=1e-6)
        finally:
            # Restore the fixture so subsequent tests in this session see
            # the original DataFrame.
            key_value_store_unit_conversion_db.config.debug_tables["channel_metrics"] = cm

    def test_source_unit_falls_back_when_channel_metrics_lacks_unit_column(
        self,
        spark: SparkSession,
        key_value_store_unit_conversion_db: MeasurementDB,
        channels_csv_path: str,
    ):
        # Drop the `unit` column from channel_metrics entirely. The solver
        # detects its absence (metrics_has_unit = False) and falls back to
        # the mapping's source_unit directly.
        cm = key_value_store_unit_conversion_db.config.debug_tables["channel_metrics"]
        cm_no_unit = cm.drop("unit")
        key_value_store_unit_conversion_db.config.debug_tables["channel_metrics"] = cm_no_unit

        try:
            solver = _solver(spark)
            query = key_value_store_unit_conversion_db.query
            vehicle_speed = query.channel_with_alias(channel_alias="vehicle_speed").alias(
                "vehicle_speed"
            )
            pdf = query.select(vehicle_speed).toPandas(spark, solver=solver)

            # All three containers: no unit column → mapping source_unit
            # "km/h" wins → factor 0.277778.
            for cid in (1, 2, 3):
                expected = _expected_raw_values(channels_csv_path, cid, 7) * 0.277778
                row = pdf.loc[pdf["container_id"] == cid].iloc[0]
                np.testing.assert_allclose(row.vehicle_speed.values, expected, rtol=1e-6)
        finally:
            key_value_store_unit_conversion_db.config.debug_tables["channel_metrics"] = cm

    def test_channel_metrics_unit_col_is_configurable(
        self,
        spark: SparkSession,
        key_value_store_unit_conversion_db: MeasurementDB,
        channels_csv_path: str,
    ):
        # Rename the physical `unit` column to `phys_unit` on channel_metrics,
        # then point the solver at it via channel_metrics.column_name_mapping.
        # The configurable unit_col property (default "unit") is what the
        # solver references; rename brings the physical name to the internal
        # name.
        cm = key_value_store_unit_conversion_db.config.debug_tables["channel_metrics"]
        cm_renamed = cm.withColumnRenamed("unit", "phys_unit")
        key_value_store_unit_conversion_db.config.debug_tables["channel_metrics"] = cm_renamed

        try:
            solver = KeyValueStoreSolver(
                spark,
                config=SolverConfig(
                    project_id="SAMPLE_PROJECT",
                    container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
                    channel_metrics=TableConfig(column_name_mapping={"phys_unit": "unit"}),
                    channel_mapping=ChannelMappingConfig(
                        filters={"toolbox_id": "container_concept"}
                    ),
                ),
            )
            query = key_value_store_unit_conversion_db.query
            vehicle_speed = query.channel_with_alias(channel_alias="vehicle_speed").alias(
                "vehicle_speed"
            )
            pdf = query.select(vehicle_speed).toPandas(spark, solver=solver)

            # Renamed column carries through: container 3 still resolves to
            # m/s (no scaling); containers 1/2 still scale by 0.277778.
            expected3 = _expected_raw_values(channels_csv_path, 3, 7)
            row3 = pdf.loc[pdf["container_id"] == 3].iloc[0]
            np.testing.assert_allclose(row3.vehicle_speed.values, expected3, rtol=1e-12)

            for cid in (1, 2):
                expected = _expected_raw_values(channels_csv_path, cid, 7) * 0.277778
                row = pdf.loc[pdf["container_id"] == cid].iloc[0]
                np.testing.assert_allclose(row.vehicle_speed.values, expected, rtol=1e-6)
        finally:
            key_value_store_unit_conversion_db.config.debug_tables["channel_metrics"] = cm


class TestAliasUnitConflictDetection:
    """Per-channel unit conversion supports only one (source_unit, target_unit) pair.

    When two aliases on the same physical channel disagree, the solver
    must raise rather than silently mis-converting one of them.
    """

    @staticmethod
    def _mapping_with(spark: SparkSession, rows):
        """Build a channel_mapping DataFrame from rows matching the
        unit-conversion fixture schema."""
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType(
            [
                StructField("project_id", StringType(), nullable=False),
                StructField("toolbox_id", StringType(), nullable=False),
                StructField("channel_alias", StringType(), nullable=False),
                StructField("source_channel", StringType(), nullable=False),
                StructField("data_key", StringType(), nullable=False),
                StructField("priority", IntegerType(), nullable=True),
                StructField("source_unit", StringType(), nullable=True),
                StructField("target_unit", StringType(), nullable=True),
            ]
        )
        return spark.createDataFrame(rows, schema=schema)

    def test_conflict_on_target_unit_raises(
        self, spark: SparkSession, key_value_store_unit_conversion_db: MeasurementDB
    ):
        # Two aliases both resolve to (container_id, channel_id) = (1, 7) and
        # (2, 7) via Vehicle Speed Sensor / TM, but request different
        # target_units. The solver must raise.
        original = key_value_store_unit_conversion_db.config.debug_tables["channel_mapping"]
        conflicting = self._mapping_with(
            spark,
            [
                (
                    "SAMPLE_PROJECT",
                    "container_concept",
                    "vehicle_speed_mph",
                    "Vehicle Speed Sensor",
                    "TM",
                    None,
                    "km/h",
                    "mph",
                ),
                (
                    "SAMPLE_PROJECT",
                    "container_concept",
                    "vehicle_speed_ms",
                    "Vehicle Speed Sensor",
                    "TM",
                    None,
                    "km/h",
                    "m/s",
                ),
            ],
        )
        key_value_store_unit_conversion_db.config.debug_tables["channel_mapping"] = conflicting

        try:
            solver = _solver(spark)
            query = key_value_store_unit_conversion_db.query
            mph = query.channel_with_alias(channel_alias="vehicle_speed_mph").alias("mph")
            ms = query.channel_with_alias(channel_alias="vehicle_speed_ms").alias("ms")

            with pytest.raises(ValueError, match="Conflicting unit conversions") as excinfo:
                query.select(mph, ms).toPandas(spark, solver=solver)

            msg = str(excinfo.value)
            assert "channel_id=7" in msg
            assert "mph" in msg
            assert "m/s" in msg
        finally:
            key_value_store_unit_conversion_db.config.debug_tables["channel_mapping"] = original

    def test_conflict_on_source_unit_raises(
        self, spark: SparkSession, key_value_store_unit_conversion_db: MeasurementDB
    ):
        # Same physical channel, agreeing target_unit, but disagreeing
        # source_unit. (The coalesce in filter_aliased_channel_metrics
        # prefers channel_metrics.unit, but if it's null/absent the
        # mapping's source_unit wins — and these two mappings disagree.)
        original = key_value_store_unit_conversion_db.config.debug_tables["channel_mapping"]
        conflicting = self._mapping_with(
            spark,
            [
                (
                    "SAMPLE_PROJECT",
                    "container_concept",
                    "vehicle_speed_a",
                    "Vehicle Speed Sensor",
                    "TM",
                    None,
                    "km/h",
                    "m/s",
                ),
                (
                    "SAMPLE_PROJECT",
                    "container_concept",
                    "vehicle_speed_b",
                    "Vehicle Speed Sensor",
                    "TM",
                    None,
                    "mph",
                    "m/s",
                ),
            ],
        )
        # Also drop channel_metrics.unit so neither alias has a value to
        # coalesce against — both rely on mapping.source_unit.
        original_cm = key_value_store_unit_conversion_db.config.debug_tables["channel_metrics"]
        cm_no_unit = original_cm.drop("unit")
        key_value_store_unit_conversion_db.config.debug_tables["channel_mapping"] = conflicting
        key_value_store_unit_conversion_db.config.debug_tables["channel_metrics"] = cm_no_unit

        try:
            solver = _solver(spark)
            query = key_value_store_unit_conversion_db.query
            a = query.channel_with_alias(channel_alias="vehicle_speed_a").alias("a")
            b = query.channel_with_alias(channel_alias="vehicle_speed_b").alias("b")

            with pytest.raises(ValueError, match="Conflicting unit conversions") as excinfo:
                query.select(a, b).toPandas(spark, solver=solver)

            msg = str(excinfo.value)
            assert "channel_id=7" in msg
            assert "km/h" in msg
            assert "mph" in msg
        finally:
            key_value_store_unit_conversion_db.config.debug_tables["channel_mapping"] = original
            key_value_store_unit_conversion_db.config.debug_tables["channel_metrics"] = original_cm

    def test_no_conflict_when_aliases_agree(
        self,
        spark: SparkSession,
        key_value_store_unit_conversion_db: MeasurementDB,
        channels_csv_path: str,
    ):
        # Two aliases on the same physical channel agree on (source_unit,
        # target_unit). Both selectors should resolve and produce the same
        # converted values.
        original = key_value_store_unit_conversion_db.config.debug_tables["channel_mapping"]
        agreeing = self._mapping_with(
            spark,
            [
                (
                    "SAMPLE_PROJECT",
                    "container_concept",
                    "vehicle_speed_a",
                    "Vehicle Speed Sensor",
                    "TM",
                    None,
                    "km/h",
                    "m/s",
                ),
                (
                    "SAMPLE_PROJECT",
                    "container_concept",
                    "vehicle_speed_b",
                    "Vehicle Speed Sensor",
                    "TM",
                    None,
                    "km/h",
                    "m/s",
                ),
            ],
        )
        key_value_store_unit_conversion_db.config.debug_tables["channel_mapping"] = agreeing

        try:
            solver = _solver(spark)
            query = key_value_store_unit_conversion_db.query
            a = query.channel_with_alias(channel_alias="vehicle_speed_a").alias("a")
            b = query.channel_with_alias(channel_alias="vehicle_speed_b").alias("b")

            pdf = query.select(a, b).toPandas(spark, solver=solver)
            for cid in (1, 2):
                expected = _expected_raw_values(channels_csv_path, cid, 7) * 0.277778
                row = pdf.loc[pdf["container_id"] == cid].iloc[0]
                np.testing.assert_allclose(row.a.values, expected, rtol=1e-6)
                np.testing.assert_allclose(row.b.values, expected, rtol=1e-6)
        finally:
            key_value_store_unit_conversion_db.config.debug_tables["channel_mapping"] = original


class TestConversionFactorValidation:
    """`unit_conversion.conversion_factor` must be a positive non-null number.

    Catches malformed reference rows early so the user sees a clear error
    instead of silent data corruption (zero/negative) or silent contract
    violation (null).
    """

    @staticmethod
    def _uc_with(spark: SparkSession, rows):
        """Build a unit_conversion DataFrame with an explicit schema so the
        nullable factor case doesn't confuse Spark's type inference."""
        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            StringType,
            StructField,
            StructType,
        )

        schema = StructType(
            [
                StructField("group_id", StringType(), nullable=False),
                StructField("unit", StringType(), nullable=False),
                StructField("conversion_factor", DoubleType(), nullable=True),
                StructField("is_base", BooleanType(), nullable=True),
            ]
        )
        return spark.createDataFrame(rows, schema=schema)

    def _run_with_uc_table(self, spark, db, uc_rows):
        """Replace the unit_conversion debug table, run a vehicle_speed
        aliased query, restore the original. Returns nothing — used inside
        a ``pytest.raises`` block.
        """
        original = db.config.debug_tables["unit_conversion"]
        db.config.debug_tables["unit_conversion"] = self._uc_with(spark, uc_rows)
        try:
            solver = _solver(spark)
            query = db.query
            vehicle_speed = query.channel_with_alias(channel_alias="vehicle_speed").alias(
                "vehicle_speed"
            )
            query.select(vehicle_speed).toPandas(spark, solver=solver)
        finally:
            db.config.debug_tables["unit_conversion"] = original

    def test_zero_factor_raises(
        self, spark: SparkSession, key_value_store_unit_conversion_db: MeasurementDB
    ):
        rows = [
            ("speed", "m/s", 1.0, True),
            ("speed", "km/h", 0.0, False),  # bad
        ]
        with pytest.raises(ValueError, match="Invalid conversion_factor") as excinfo:
            self._run_with_uc_table(spark, key_value_store_unit_conversion_db, rows)
        msg = str(excinfo.value)
        assert "km/h" in msg
        assert "conversion_factor=0" in msg

    def test_negative_factor_raises(
        self, spark: SparkSession, key_value_store_unit_conversion_db: MeasurementDB
    ):
        rows = [
            ("speed", "m/s", 1.0, True),
            ("speed", "km/h", -1.0, False),  # bad
        ]
        with pytest.raises(ValueError, match="Invalid conversion_factor") as excinfo:
            self._run_with_uc_table(spark, key_value_store_unit_conversion_db, rows)
        msg = str(excinfo.value)
        assert "km/h" in msg
        assert "conversion_factor=-1" in msg

    def test_null_factor_raises(
        self, spark: SparkSession, key_value_store_unit_conversion_db: MeasurementDB
    ):
        rows = [
            ("speed", "m/s", 1.0, True),
            ("speed", "km/h", None, False),  # bad
        ]
        with pytest.raises(ValueError, match="Invalid conversion_factor") as excinfo:
            self._run_with_uc_table(spark, key_value_store_unit_conversion_db, rows)
        msg = str(excinfo.value)
        assert "km/h" in msg
        assert "conversion_factor=None" in msg


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
