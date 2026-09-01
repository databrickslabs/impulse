"""End-to-end integration tests for Points-in-Time (POI) channels.

Exercises the full solve pipeline for ``query.poi_channel(...)`` against the shared
``basic_narrow_db`` (wide) and ``narrow_db`` (EAV) fixtures, which carry POI channels
on ``container_id = 1`` alongside the existing sample channels (see conftest /
``poi_channels.csv``):

- ``channel_id = 90`` — a **string** DTC-code channel (``P0301`` / ``P0420`` / ``P0301``)
- ``channel_id = 91`` — a **numeric** DTC-count channel (values ``1, 2, 3``)

Covers: numeric POI unweighted reductions, string POI equality + op gating, the
mix-and-match case (a CONTINUOUS and a POI channel in one expression), and CONTINUOUS
backward-compatibility.
"""

import math

import pytest
import pyspark.sql.types as T
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.metadata.time_series_expression import SeriesValueType
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig


class TestNumericPoi:
    def test_numeric_poi_mean_is_unweighted(self, spark: SparkSession, basic_narrow_db):
        """A numeric POI ``mean()`` is the plain (unweighted) mean of the point values —
        POI points have no duration to weight by, unlike ``SampleSeries.mean()``."""
        solver = DefaultSolver(spark)
        q = basic_narrow_db.query
        dtc_count = q.poi_channel(channel_name="DTC_count")  # values 1, 2, 3

        result = q.select(dtc_count.mean().alias("m")).solve(spark=spark, solver=solver)

        rows = {r.container_id: r.m for r in result.collect()}
        assert rows[1] == 2.0  # unweighted mean of (1, 2, 3)

    def test_numeric_poi_sum_and_count(self, spark: SparkSession, basic_narrow_db):
        solver = DefaultSolver(spark)
        q = basic_narrow_db.query
        dtc_count = q.poi_channel(channel_name="DTC_count")

        result = q.select(
            dtc_count.sum().alias("s"),
            dtc_count.count().alias("c"),
        ).solve(spark=spark, solver=solver)

        row = {r.container_id: r for r in result.collect()}[1]
        assert row.s == 6.0  # 1 + 2 + 3
        assert row.c == 3

    def test_bare_numeric_poi_selection_types_as_points_in_time(
        self, spark: SparkSession, basic_narrow_db
    ):
        """A bare numeric POI selection serializes as ``array<array<double>>``
        (PointsInTimeSeries), not the CONTINUOUS ``binary`` blob type."""
        solver = DefaultSolver(spark)
        q = basic_narrow_db.query
        dtc_count = q.poi_channel(channel_name="DTC_count").alias("pit")

        result = q.select(dtc_count).solve(spark=spark, solver=solver)

        assert result.schema["pit"].dataType == T.ArrayType(T.ArrayType(T.DoubleType()))
        rows = {r.container_id: r.pit for r in result.collect()}
        # three points [t, v], values 1..3 (unweighted, in timestamp order)
        assert [pt[1] for pt in rows[1]] == [1.0, 2.0, 3.0]


class TestStringPoi:
    def test_string_poi_equality_selects_matching_instants(
        self, spark: SparkSession, basic_narrow_db
    ):
        """``string_poi == "P0301"`` yields the instants where the code equals P0301.

        Sampling the count channel at those instants (via ``.where``) picks out the two
        P0301 occurrences, proving the string equality drove the point selection.
        """
        solver = DefaultSolver(spark)
        q = basic_narrow_db.query
        dtc = q.poi_channel(channel_name="DTC", dtype=SeriesValueType.STRING)

        # DTC == "P0301" is a PointsInTime; serialize it directly.
        result = q.select((dtc == "P0301").alias("hits")).solve(spark=spark, solver=solver)

        rows = {r.container_id: r.hits for r in result.collect()}
        # P0301 occurs at the 1st and 3rd of the three DTC timestamps.
        assert len(rows[1]) == 2

    def test_string_poi_count_and_sampling_allowed(self, spark: SparkSession, basic_narrow_db):
        solver = DefaultSolver(spark)
        q = basic_narrow_db.query
        dtc = q.poi_channel(channel_name="DTC", dtype=SeriesValueType.STRING)

        result = q.select(dtc.count().alias("c")).solve(spark=spark, solver=solver)

        assert {r.container_id: r.c for r in result.collect()}[1] == 3

    def test_bare_string_poi_selection_types_as_points_in_time(
        self, spark: SparkSession, basic_narrow_db
    ):
        """A bare string POI selection serializes as ``array<struct<tstart,value>>``
        and collects to (tstart, code) points.

        Unlike the numeric bare selection, the string path is otherwise only
        exercised through ``==`` / ``count`` / ``.where``, all of which reduce the
        series before it is serialized. This drives the raw string
        ``PointsInTimeSeries`` result the whole way through the GROUPED_MAP UDF's
        Arrow serialization.
        """
        solver = DefaultSolver(spark)
        q = basic_narrow_db.query
        dtc = q.poi_channel(channel_name="DTC", dtype=SeriesValueType.STRING).alias("pit")

        result = q.select(dtc).solve(spark=spark, solver=solver)
        assert result.schema["pit"].dataType == T.ArrayType(
            T.StructType(
                [
                    T.StructField("tstart", T.DoubleType()),
                    T.StructField("value", T.StringType()),
                ]
            )
        )
        rows = {r.container_id: r.pit for r in result.collect()}
        # three DTC codes P0301, P0420, P0301 in timestamp order.
        assert [pt["value"] for pt in rows[1]] == ["P0301", "P0420", "P0301"]
        assert [pt["tstart"] for pt in rows[1]] == [
            1499929300000000.0,
            1499931000000000.0,
            1499933000000000.0,
        ]

    @pytest.mark.parametrize("reduction", ["mean", "sum", "min", "max"])
    def test_string_poi_numeric_reduction_rejected_at_build(
        self, spark: SparkSession, basic_narrow_db, reduction
    ):
        """A numeric reduction on a string POI selection is rejected at plan/build time
        (before Spark runs), not as a silent NaN."""
        q = basic_narrow_db.query
        dtc = q.poi_channel(channel_name="DTC", dtype=SeriesValueType.STRING)
        selection = getattr(dtc, reduction)().alias("bad")
        with pytest.raises(TypeError, match="non-numeric"):
            q.select(selection)._determine_result_objects_dtypes()


class TestMixAndMatch:
    """The primary correctness case: a CONTINUOUS and a POI channel in one expression, both
    in the same per-container pandas frame, aligned via ``synchronized``."""

    def test_sample_channel_sampled_at_poi_instants(self, spark: SparkSession, narrow_db):
        """Sample the ``seed`` CONTINUOUS channel at the instants of the numeric POI channel.

        narrow_db container 1: ``seed`` sample channel has values 1..10 over t=0..10; the
        numeric POI channel (91) has points at t = 2, 5, 8. Sampling seed at those instants
        picks the seed values valid there.
        """
        solver = DefaultSolver(spark)
        q = narrow_db.query
        seed = q.channel(seed="0")
        dtc_count = q.poi_channel(channel_name="DTC_count")

        # Sample the sample-series at the POI points (cross-type synchronize).
        result = q.select(seed.where(dtc_count.to_points_in_time()).alias("sampled")).solve(
            spark=spark, solver=solver
        )

        rows = {r.container_id: r.sampled for r in result.collect()}
        # three sampled points at the POI instants t = 2, 5, 8
        assert [pt[0] for pt in rows[1]] == [2.0, 5.0, 8.0]
        for pt in rows[1]:
            assert not math.isnan(pt[1])

    def test_string_poi_and_sample_freeze_frame(self, spark: SparkSession, narrow_db):
        """Freeze-frame: sample the seed channel at the instants where DTC == "P0301"."""
        solver = DefaultSolver(spark)
        q = narrow_db.query
        seed = q.channel(seed="0")
        dtc = q.poi_channel(channel_name="DTC", dtype=SeriesValueType.STRING)

        result = q.select(seed.where(dtc == "P0301").alias("frozen")).solve(
            spark=spark, solver=solver
        )

        rows = {r.container_id: r.frozen for r in result.collect()}
        # P0301 at t = 2 and 8 (EAV fixture); seed sampled there.
        assert [pt[0] for pt in rows[1]] == [2.0, 8.0]

    def test_sample_channel_sampled_at_poi_instants_wide(
        self, spark: SparkSession, basic_narrow_db
    ):
        """Wide-mode counterpart of the mix-and-match case (``basic_narrow_db``).

        Uses a POI numeric channel to filter/sample a real sample channel. Only
        "Ambient Air Temperature" (channel 6) spans all three POI instants in the
        ``basic_narrow_csv`` fixture (the other channels end earlier), so it is the
        channel sampled here.  Proves POI-drives-channel-selection works through the
        wide (columns-on-channel_metrics) path, not just the EAV pivot path.
        """
        solver = DefaultSolver(spark)
        q = basic_narrow_db.query
        amb = q.channel(channel_name="Ambient Air Temperature")
        dtc_count = q.poi_channel(channel_name="DTC_count")  # points at 3 POI instants

        result = q.select(amb.where(dtc_count.to_points_in_time()).alias("sampled")).solve(
            spark=spark, solver=solver
        )

        rows = {r.container_id: r.sampled for r in result.collect()}
        # The three POI instants (microsecond epochs) from basic_narrow_csv/poi_channels.csv.
        poi_instants = [1499929300000000.0, 1499931000000000.0, 1499933000000000.0]
        assert [pt[0] for pt in rows[1]] == poi_instants
        # Each instant sampled a real Ambient-Air-Temp value (not a miss / NaN).
        for pt in rows[1]:
            assert not math.isnan(pt[1])

    def test_string_poi_freeze_frame_wide(self, spark: SparkSession, basic_narrow_db):
        """Wide-mode freeze-frame: sample "Ambient Air Temperature" where DTC == "P0301".

        In ``basic_narrow_csv`` P0301 occurs at the 1st and 3rd DTC instants, so the
        string-POI equality predicate selects exactly those two instants of the
        sample channel — the freeze-frame case resolved via the wide channel path.
        """
        solver = DefaultSolver(spark)
        q = basic_narrow_db.query
        amb = q.channel(channel_name="Ambient Air Temperature")
        dtc = q.poi_channel(channel_name="DTC", dtype=SeriesValueType.STRING)

        result = q.select(amb.where(dtc == "P0301").alias("frozen")).solve(
            spark=spark, solver=solver
        )

        rows = {r.container_id: r.frozen for r in result.collect()}
        # P0301 at the 1st and 3rd instants (basic_narrow_csv/poi_channels.csv).
        assert [pt[0] for pt in rows[1]] == [1499929300000000.0, 1499933000000000.0]
        for pt in rows[1]:
            assert not math.isnan(pt[1])


class TestBackwardCompat:
    def test_sample_channel_unaffected_by_poi(self, spark: SparkSession, basic_narrow_db):
        """An ordinary CONTINUOUS ``channel(...)`` selection is unchanged by POI support."""
        solver = DefaultSolver(spark)
        q = basic_narrow_db.query
        rpm = q.channel(channel_name="Engine RPM")

        result = q.select(rpm.mean().alias("rpm_mean")).solve(spark=spark, solver=solver)

        rows = {r.container_id for r in result.collect()}
        assert rows == {1, 2, 3}


class TestSingleValueColumnPoiTable:
    """A ``poi_channels`` table may carry only one of the two value columns.

    ``schema.py`` types are reference-only (not enforced on read), so a customer
    table with just ``value_double`` (numeric channels) or just ``value_string``
    (string channels) is a legitimate shape. The POI union must not require both
    columns to be present.
    """

    @staticmethod
    def _db_with_poi(basic_narrow_db, spark, drop_col: str) -> MeasurementDB:
        """Clone ``basic_narrow_db`` with one value column dropped from poi_channels."""
        tables = {
            "container_metrics": spark.read.table("spark_catalog.silver.container_metrics"),
            "channel_metrics": spark.read.table("spark_catalog.silver.channel_metrics"),
            "channels": spark.read.table("spark_catalog.silver.channels"),
            "poi_channels": basic_narrow_db.poi_channels(spark).drop(drop_col),
        }
        return MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=basic_narrow_db.ws)

    def test_numeric_only_poi_table_solves(self, spark: SparkSession, basic_narrow_db):
        """A numeric-only poi_channels table (no ``value_string``) resolves a numeric
        POI selection without raising an ``AnalysisException`` on the missing column."""
        db = self._db_with_poi(basic_narrow_db, spark, drop_col="value_string")
        q = db.query
        dtc_count = q.poi_channel(channel_name="DTC_count")

        result = q.select(dtc_count.sum().alias("s")).solve(
            spark=spark, solver=DefaultSolver(spark)
        )

        assert {r.container_id: r.s for r in result.collect()}[1] == 6.0  # 1 + 2 + 3

    def test_string_only_poi_table_solves(self, spark: SparkSession, basic_narrow_db):
        """A string-only poi_channels table (no ``value_double``) resolves a string
        POI selection without raising an ``AnalysisException`` on the missing column."""
        db = self._db_with_poi(basic_narrow_db, spark, drop_col="value_double")
        q = db.query
        dtc = q.poi_channel(channel_name="DTC", dtype=SeriesValueType.STRING)

        result = q.select(dtc.count().alias("c")).solve(spark=spark, solver=DefaultSolver(spark))

        assert {r.container_id: r.c for r in result.collect()}[1] == 3
