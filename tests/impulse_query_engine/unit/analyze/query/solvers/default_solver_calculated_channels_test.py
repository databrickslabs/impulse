# pylint: disable=missing-function-docstring
"""End-to-end tests for QueryBuilder.solve_calculated_channels + DefaultSolver.

Exercises the narrow calculated-channel output against the wide-only
`basic_narrow_db` fixture: real computed values, output schema, deterministic
channel_id, dynamic container_id typing, validation, and empty results.
"""

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest

from impulse_query_engine.analyze.query.channels.calculated_channel import (
    CalculatedChannel,
)
from impulse_query_engine.analyze.query.aggregations.stats_aggregator import (
    StatsAggregator,
)
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from impulse_query_engine.model.series.sample_series import SampleSeries
from tests.conftest import basic_narrow_db, spark  # noqa: F401  (pytest fixtures)

# Known datum from tests/unit/data/basic_narrow_csv/channel_data.csv:
# container 1, channel 5 (Engine RPM), second RLE row.
_C1_RPM_TSTART = 1499929245761999
_C1_RPM_VALUE = 1081.0


def _id_val(key, col="identity"):
    """Extract a single identity key's value from the ``MapType`` identity column."""
    return F.col(col).getItem(key)


def _recast_container_id(db: MeasurementDB, cid_type: T.DataType) -> MeasurementDB:
    """Clone a ``for_debug`` db, casting ``container_id`` on every table."""
    tables = {
        name: (
            df.withColumn("container_id", F.col("container_id").cast(cid_type))
            if "container_id" in df.columns
            else df
        )
        for name, df in db.config.debug_tables.items()
    }
    return MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=db.ws)


class TestCalculatedChannelValues:
    def test_scaling_produces_real_values(self, spark, basic_narrow_db):
        q = basic_narrow_db.query
        cc = CalculatedChannel(
            q.channel(channel_name="Engine RPM") * 2,
            {"channel_name": "rpm_x2", "data_key": "CALC"},
        )
        result = q.select(cc).solve_calculated_channels(spark, solver=DefaultSolver(spark))

        row = (
            result.filter((F.col("container_id") == 1) & (F.col("tstart") == _C1_RPM_TSTART))
            .select(
                "value",
                _id_val("channel_name").alias("cn"),
                _id_val("data_key").alias("dk"),
            )
            .collect()
        )
        assert len(row) == 1
        assert row[0]["value"] == pytest.approx(_C1_RPM_VALUE * 2)
        assert row[0]["cn"] == "rpm_x2"
        assert row[0]["dk"] == "CALC"

    def test_all_rows_carry_identity(self, spark, basic_narrow_db):
        q = basic_narrow_db.query
        cc = CalculatedChannel(
            q.channel(channel_name="Engine RPM") + 1.0,
            {"channel_name": "rpm_plus_1", "data_key": "CALC"},
        )
        result = q.select(cc).solve_calculated_channels(spark, solver=DefaultSolver(spark))
        distinct = {tuple(sorted(r["identity"].items())) for r in result.collect()}
        assert distinct == {(("channel_name", "rpm_plus_1"), ("data_key", "CALC"))}
        assert result.count() > 0

    def test_arbitrary_identity_keys_round_trip(self, spark, basic_narrow_db):
        # Identity is a self-describing map, so non-{channel_name,data_key} keys work.
        q = basic_narrow_db.query
        cc = CalculatedChannel(
            q.channel(channel_name="Engine RPM") * 2,
            {"sensor_id": "s1", "unit": "rpm"},
        )
        result = q.select(cc).solve_calculated_channels(spark, solver=DefaultSolver(spark))
        assert result.schema["identity"].dataType == T.MapType(T.StringType(), T.StringType())
        row = result.select(
            _id_val("sensor_id").alias("sid"), _id_val("unit").alias("unit")
        ).first()
        assert row["sid"] == "s1"
        assert row["unit"] == "rpm"

    def test_multi_channel_sync_matches_core_model(self, spark, basic_narrow_db):
        """A two-channel sum matches an in-process SampleSeries synchronization."""
        q = basic_narrow_db.query
        rpm = q.channel(channel_name="Engine RPM")
        speed = q.channel(channel_name="Vehicle Speed Sensor")
        cc = CalculatedChannel(rpm + speed, {"channel_name": "rpm_plus_speed", "data_key": "CALC"})
        result = q.select(cc).solve_calculated_channels(spark, solver=DefaultSolver(spark))

        # Container 1 has both channel 5 (RPM) and channel 7 (Speed).
        got = {
            r["tstart"]: r["value"] for r in result.filter(F.col("container_id") == 1).collect()
        }
        assert got, "expected calculated rows for container 1"

        # Expected: build the two source series and add them via the same core model.
        channels = basic_narrow_db.channels(spark).filter(F.col("container_id") == 1)

        def _series(channel_id):
            rows = sorted(
                (
                    (r["tstart"], r["tend"], r["value"])
                    for r in channels.filter(F.col("channel_id") == channel_id).collect()
                ),
                key=lambda x: x[0],
            )
            return SampleSeries([r[0] for r in rows], [r[1] for r in rows], [r[2] for r in rows])

        expected_series = _series(5) + _series(7)
        expected = {int(ts): val for ts, _, val in expected_series.get_data()}

        assert set(got) == set(expected)
        for ts, val in expected.items():
            assert got[ts] == pytest.approx(val)


class TestOutputSchema:
    def test_output_columns_and_order(self, spark, basic_narrow_db):
        q = basic_narrow_db.query
        cc = CalculatedChannel(
            q.channel(channel_name="Engine RPM") * 2,
            {"channel_name": "rpm_x2", "data_key": "CALC"},
        )
        result = q.select(cc).solve_calculated_channels(spark, solver=DefaultSolver(spark))
        # Identity is a single self-describing map column after the silver columns.
        assert result.columns == [
            "container_id",
            "channel_id",
            "tstart",
            "tend",
            "value",
            "identity",
        ]
        assert result.schema["tstart"].dataType == T.LongType()
        assert result.schema["value"].dataType == T.DoubleType()
        assert result.schema["identity"].dataType == T.MapType(T.StringType(), T.StringType())

    @pytest.mark.parametrize(
        "cid_type", [T.StringType(), T.IntegerType(), T.LongType()], ids=lambda t: t.simpleString()
    )
    def test_dynamic_container_id_type(self, spark, basic_narrow_db, cid_type):
        db = _recast_container_id(basic_narrow_db, cid_type)
        q = db.query
        cc = CalculatedChannel(
            q.channel(channel_name="Engine RPM") * 2, {"channel_name": "rpm_x2"}
        )
        result = q.select(cc).solve_calculated_channels(spark, solver=DefaultSolver(spark))
        assert result.schema["container_id"].dataType == cid_type
        # channel_id type follows the source channels table (IntegerType here).
        assert result.schema["channel_id"].dataType == T.IntegerType()
        assert result.count() > 0


class TestChannelId:
    def test_deterministic_across_runs(self, spark, basic_narrow_db):
        q = basic_narrow_db.query

        def _ids():
            cc = CalculatedChannel(
                q.channel(channel_name="Engine RPM") * 2,
                {"channel_name": "rpm_x2", "data_key": "CALC"},
            )
            result = q.select(cc).solve_calculated_channels(spark, solver=DefaultSolver(spark))
            return {r["channel_id"] for r in result.select("channel_id").distinct().collect()}

        assert _ids() == _ids()

    def test_distinct_identities_distinct_ids(self, spark, basic_narrow_db):
        q = basic_narrow_db.query
        cc_a = CalculatedChannel(q.channel(channel_name="Engine RPM") * 2, {"channel_name": "a"})
        cc_b = CalculatedChannel(q.channel(channel_name="Engine RPM") * 3, {"channel_name": "b"})
        result = q.select(cc_a, cc_b).solve_calculated_channels(spark, solver=DefaultSolver(spark))
        by_name = {
            r["cn"]: r["channel_id"]
            for r in result.select(_id_val("channel_name").alias("cn"), "channel_id")
            .distinct()
            .collect()
        }
        assert by_name["a"] != by_name["b"]

    def test_emitted_id_matches_channel_property(self, spark, basic_narrow_db):
        # The solver emits the channel's own deterministic channel_id — no
        # separate id derivation in the solver.
        q = basic_narrow_db.query
        cc = CalculatedChannel(
            q.channel(channel_name="Engine RPM") * 2, {"channel_name": "rpm_x2"}
        )
        result = q.select(cc).solve_calculated_channels(spark, solver=DefaultSolver(spark))
        ids = {r["channel_id"] for r in result.select("channel_id").distinct().collect()}
        assert ids == {cc.channel_id}


class TestValidation:
    def test_plain_selector_rejected(self, spark, basic_narrow_db):
        q = basic_narrow_db.query
        q.select(q.channel(channel_name="Engine RPM"))
        with pytest.raises(ValueError, match="requires all selections to be"):
            q.solve_calculated_channels(spark, solver=DefaultSolver(spark))

    def test_aggregation_rejected(self, spark, basic_narrow_db):
        q = basic_narrow_db.query
        agg = StatsAggregator(
            input_expressions=[q.channel(channel_name="Engine RPM")], statistics=["mean"]
        )
        q.select(agg)
        with pytest.raises(ValueError, match="requires all selections to be"):
            q.solve_calculated_channels(spark, solver=DefaultSolver(spark))

    def test_mismatched_identity_keys_allowed(self, spark, basic_narrow_db):
        # Identity is a self-describing map, so heterogeneous key sets are fine.
        q = basic_narrow_db.query
        cc_a = CalculatedChannel(q.channel(channel_name="Engine RPM") * 2, {"channel_name": "a"})
        cc_b = CalculatedChannel(
            q.channel(channel_name="Engine RPM") * 3, {"channel_name": "b", "data_key": "CALC"}
        )
        result = q.select(cc_a, cc_b).solve_calculated_channels(spark, solver=DefaultSolver(spark))
        assert result.schema["identity"].dataType == T.MapType(T.StringType(), T.StringType())
        assert result.count() > 0


class TestEmptyResults:
    def test_no_matching_channels_empty_frame(self, spark, basic_narrow_db):
        q = basic_narrow_db.query
        cc = CalculatedChannel(
            q.channel(channel_name="Nonexistent Channel") * 2,
            {"channel_name": "nope", "data_key": "CALC"},
        )
        result = q.select(cc).solve_calculated_channels(spark, solver=DefaultSolver(spark))
        assert result.count() == 0
        assert result.columns == [
            "container_id",
            "channel_id",
            "tstart",
            "tend",
            "value",
            "identity",
        ]
        # Empty branch returns the same schema → identity is a map too.
        assert result.schema["identity"].dataType == T.MapType(T.StringType(), T.StringType())

    def test_base_solver_not_supported(self, spark, basic_narrow_db):
        from impulse_query_engine.analyze.query.solvers.blob_solver import BlobSolver

        q = basic_narrow_db.query
        cc = CalculatedChannel(q.channel(channel_name="Engine RPM") * 2, {"channel_name": "x"})
        q.select(cc)
        with pytest.raises(NotImplementedError, match="calculated channels"):
            q.solve_calculated_channels(spark, solver=BlobSolver())
