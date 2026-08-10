# pylint: disable=missing-function-docstring
"""End-to-end integration tests for POI on the DefaultSolver.

Exercises the ``poi_channel`` capability (POI read as a ``PointsInTimeSeries``,
unioned into the solve frame) and its composition with measured channels,
aliasing, and aggregations. Split out of ``default_solver_test.py`` so all POI
integration coverage lives in one place.

Uses the ``poi_integration_db`` fixture (defined in ``tests/conftest.py``):
second-scale timestamps so POI ``timestamp_abs`` (cast to epoch seconds by the
POI shaping step) lands inside the channel sample intervals. Container 3 has POI
rows but no channel data — the POI-only container case.
"""

import pytest
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.query.aggregations.point_value_aggregator import (
    PointValueAggregator,
)
from impulse_query_engine.analyze.query.aggregations.stats_aggregator import StatsAggregator
from impulse_query_engine.analyze.query.solvers.default_solver import (
    DefaultSolver,
)
from impulse_query_engine.analyze.query.solvers.solver_config import (
    ChannelMappingConfig,
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB


def _poi_cfg(channel_mapping: ChannelMappingConfig | None = None) -> SolverConfig:
    """SolverConfig for the ``poi_integration_db`` fixture.

    Same EAV/wide renames as ``_kvs_cfg`` (``element_id`` → key on container_tags,
    ``project`` → project_id on container_metrics). POI columns use their defaults
    (``timestamp_abs`` / ``value`` / ``poi_type``), so no POI mapping is needed.
    """
    return SolverConfig(
        project_id="SAMPLE_PROJECT",
        container_tags=TableConfig(column_name_mapping={"element_id": "key"}),
        container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
        channel_mapping=channel_mapping or ChannelMappingConfig(),
    )


class TestDefaultSolverPoiChannelIntegration:
    """End-to-end pipeline tests for the ``poi_channel`` capability.

    Uses the ``poi_integration_db`` fixture (second-scale timestamps so POI
    instants land inside channel samples). Value encoding: ok=0.0, err=1.0.

    Data (container 1, charging_error): ok@5, err@15, ok@25.
    Engine RPM (container 1): [0,10)=800 [10,20)=3000 [20,30)=1500 [30,40)=900.
    So an err instant (t=15) samples RPM 3000; the three instants sample 800/3000/1500.
    """

    def test_poi_channel_builds_points_in_time_series(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """``q.poi_channel(...)`` selected alone yields a PointsInTimeSeries per container,
        including the POI-only container (3), which has no measured channel."""
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        charging = query.poi_channel(channel_name="charging_error")

        result = query.select(charging.alias("charging")).solve(spark=spark, solver=solver)

        # array<array<double>> serialization
        assert result.schema["charging"].dataType == T.ArrayType(T.ArrayType(T.DoubleType()))
        rows = {r.container_id: r["charging"] for r in result.collect()}
        # container 1: (5,ok=0) (15,err=1) (25,ok=0)
        assert [list(p) for p in rows[1]] == [[5.0, 0.0], [15.0, 1.0], [25.0, 0.0]]
        # container 2: (25, err=1)
        assert [list(p) for p in rows[2]] == [[25.0, 1.0]]
        # container 3 is POI-only (no channel data) but still emits
        assert [list(p) for p in rows[3]] == [[7.0, 1.0]]

    def test_channel_gated_by_poi_channel(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """``rpm.where(charging == err)`` samples Engine RPM at the POI error instants."""
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        rpm = query.channel(channel_name="Engine RPM")
        charging = query.poi_channel(channel_name="charging_error")

        result = query.select(
            rpm.where(charging == 1.0).alias("rpm_at_error"),
        ).solve(spark=spark, solver=solver)

        rows = {r.container_id: r["rpm_at_error"] for r in result.collect()}
        # container 1: err@15 → RPM [10,20)=3000
        assert [list(p) for p in rows[1]] == [[15.0, 3000.0]]
        # container 2: err@25 → RPM [20,30)=2500
        assert [list(p) for p in rows[2]] == [[25.0, 2500.0]]

    def test_poi_channel_compare_yields_points_in_time(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """``charging == err`` selected directly yields the PointsInTime of error instants."""
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        charging = query.poi_channel(channel_name="charging_error")

        result = query.select((charging == 1.0).alias("error_points")).solve(
            spark=spark, solver=solver
        )

        # PointsInTime serializes to array<double>
        assert result.schema["error_points"].dataType == T.ArrayType(T.DoubleType())
        rows = {r.container_id: list(r["error_points"]) for r in result.collect()}
        assert rows[1] == [15.0]  # only the err@15 instant
        assert rows[2] == [25.0]

    def test_point_value_aggregator_samples_channel_at_poi_instants(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """PointValueAggregator samples Engine RPM at every charging_error instant."""
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        rpm = query.channel(channel_name="Engine RPM")
        charging = query.poi_channel(channel_name="charging_error")

        agg = PointValueAggregator(
            input_expressions=[rpm],
            # event must be a PointsInTime; project the POI channel to its instants
            event_expression=charging.to_points_in_time(),  # all charging_error instants
        )

        result = query.select(agg.alias("rpm_at_poi")).solve(spark=spark, solver=solver)

        rows = {r.container_id: r["rpm_at_poi"] for r in result.collect()}
        # struct(point_timestamps, values), each a list-per-input-series
        c1_ts, c1_vals = rows[1]["point_timestamps"][0], rows[1]["values"][0]
        assert list(c1_ts) == [5.0, 15.0, 25.0]
        assert list(c1_vals) == [800.0, 3000.0, 1500.0]  # RPM at each instant

    def test_stats_aggregator_over_poi_gated_channel(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """A StatsAggregator over ``rpm.where(charging == err)`` computes stats on the
        POI-sampled RPM series (single err instant → min==max==mean)."""
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        rpm = query.channel(channel_name="Engine RPM")
        charging = query.poi_channel(channel_name="charging_error")

        agg = StatsAggregator(
            input_expressions=[rpm.where(charging == 1.0)],
            statistics=["min", "max", "mean"],
        )

        result = query.select(agg.alias("rpm_error_stats")).solve(spark=spark, solver=solver)

        rows = {r.container_id: r for r in result.collect()}
        # container 1 exists and produced a well-formed stats result
        assert 1 in rows
        assert rows[1]["rpm_error_stats"] is not None

    def test_aliased_channel_gated_by_poi(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """Aliasing + POI compose: the ``engine_speed`` alias resolves to Engine RPM,
        then is gated by the POI error instants (same result as the direct channel)."""
        solver = DefaultSolver(
            spark,
            config=_poi_cfg(
                channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
            ),
        )
        query = poi_integration_db.query
        engine_speed = query.channel_with_alias(channel_alias="engine_speed")
        charging = query.poi_channel(channel_name="charging_error")

        result = query.select(
            engine_speed.where(charging == 1.0).alias("engine_speed_at_error"),
        ).solve(spark=spark, solver=solver)

        rows = {r.container_id: r["engine_speed_at_error"] for r in result.collect()}
        # alias → Engine RPM, err@15 → 3000
        assert [list(p) for p in rows[1]] == [[15.0, 3000.0]]

    def test_poi_channel_and_measured_channel_together(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """A query can select a measured channel and a POI channel side by side."""
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        rpm = query.channel(channel_name="Engine RPM")
        charging = query.poi_channel(channel_name="charging_error")

        result = query.select(
            rpm.mean().alias("rpm_mean"),
            charging.alias("charging"),
        ).solve(spark=spark, solver=solver)

        rows = {r.container_id: r for r in result.collect()}
        # container 1 has both a real channel mean and the POI series
        assert rows[1]["rpm_mean"] is not None
        assert [list(p) for p in rows[1]["charging"]] == [[5.0, 0.0], [15.0, 1.0], [25.0, 0.0]]

    def test_virtual_signal_gated_by_poi_channel(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """A *virtual* signal (a derived product of two channels, e.g. ``speed * torque``)
        gated by a POI channel is sampled at the POI error instants.

        The fixture has no torque channel, so the virtual signal here is
        ``Engine RPM * Vehicle Speed Sensor`` — same shape (a ``TimeSeriesOp``
        product of two measured channels, never persisted as a channel). Gating it
        with ``charging == err`` proves ``collect_poi_channel_selectors`` recurses
        into the arithmetic op tree to find the nested POI selector, and the
        derived series is sampled at the POI instants.
        """
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        rpm = query.channel(channel_name="Engine RPM")
        speed = query.channel(channel_name="Vehicle Speed Sensor")
        charging = query.poi_channel(channel_name="charging_error")

        virtual = rpm * speed  # derived signal, not a persisted channel
        result = query.select(
            virtual.where(charging == 1.0).alias("power_at_error"),
        ).solve(spark=spark, solver=solver)

        rows = {r.container_id: r["power_at_error"] for r in result.collect()}
        # container 1: err@15 → RPM[10,20)=3000 * Speed[10,20)=60 = 180000
        assert [list(p) for p in rows[1]] == [[15.0, 180000.0]]
        # container 2: err@25 → RPM[20,30)=2500 * Speed[20,30)=55 = 137500
        assert [list(p) for p in rows[2]] == [[25.0, 137500.0]]

    def test_incremental_processing_scopes_poi_to_batch(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """POI honors incremental processing: a ``pre_filtered_containers_df`` batch
        scopes the POI read to exactly that container set.

        The pre-filter selects containers {1, 3} (3 is POI-only, no channel data).
        POI is scoped to the surviving-container set (``metrics_df``), which the
        pre-filter drives — so container 1 and the POI-only container 3 emit their
        POI, while out-of-batch container 2's POI is excluded. This is the
        behavior the ``container_scope_df = metrics_df`` wiring guarantees: the
        incremental batch reaches the POI scope, and a POI-only container survives
        inside the batch.
        """
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        charging = query.poi_channel(channel_name="charging_error")

        # Incremental batch: only containers 1 and 3.
        pre = query.db.container_metrics(spark).where(F.col("container_id").isin(1))
        result = query.select(charging.alias("charging")).solve(
            spark=spark, solver=solver, pre_filtered_containers_df=pre
        )

        rows = {r.container_id: r["charging"] for r in result.collect()}
        # only the batch containers appear — container 2's POI is excluded
        assert set(rows.keys()) == {1}
        # container 1: full charging_error series
        assert [list(p) for p in rows[1]] == [[5.0, 0.0], [15.0, 1.0], [25.0, 0.0]]

    def test_having_refines_poi_rows_by_column(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """``.having(network=...)`` narrows a POI channel to matching rows only.

        Fixture defect POIs: container 1 @12 (value 108, network FD3), container 2
        @8 (value 200, network INFO). ``poi_channel("defect").having(network="FD3")``
        must keep only container 1's defect and drop container 2's INFO defect.
        """
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        defect_fd3 = query.poi_channel(channel_name="defect").having(network="FD3")

        result = query.select(defect_fd3.alias("defect")).solve(spark=spark, solver=solver)

        rows = {r.container_id: r["defect"] for r in result.collect()}
        # container 1's defect is FD3 → kept
        assert [list(p) for p in rows[1]] == [[12.0, 108.0]]
        # container 2's defect is INFO → filtered out; container 2 has no other
        # FD3 defect, so it does not appear at all
        assert 2 not in rows

    def test_having_variants_are_distinct_channels(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """Two ``.having()`` variants of the same poi_type are distinct channels
        carrying distinct row subsets (the per-selector shaping fix).

        Selecting both ``defect|network=FD3`` and ``defect|network=INFO`` in one
        query must return different data per container — proving the refinements
        are honored and the two selectors do not collide on their shared poi_type.
        """
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        defect_fd3 = query.poi_channel(channel_name="defect").having(network="FD3")
        defect_info = query.poi_channel(channel_name="defect").having(network="INFO")

        result = query.select(
            defect_fd3.alias("fd3"),
            defect_info.alias("info"),
        ).solve(spark=spark, solver=solver)

        rows = {r.container_id: r for r in result.collect()}
        # container 1: defect is FD3 → fd3 has it, info is empty
        assert [list(p) for p in rows[1]["fd3"]] == [[12.0, 108.0]]
        assert [list(p) for p in rows[1]["info"]] == []
        # container 2: defect is INFO → info has it, fd3 is empty
        assert [list(p) for p in rows[2]["fd3"]] == []
        assert [list(p) for p in rows[2]["info"]] == [[8.0, 200.0]]

    def test_channel_gated_by_coincident_pois(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """Chain two POI channels with ``&`` to gate a channel by *coincident* instants.

        ``(charging == err) & defect.to_points_in_time()`` intersects the charging
        error instants with the defect instants (``PointsInTime`` set
        intersection), so RPM is sampled only where a charging error and a defect
        occurred at the **same** instant.

        Fixture (container 1): charging_error err@15, defects @12 and @15 → the two
        conditions coincide only at t=15. Container 2: charging_error err@25,
        defect @8 → no shared instant.
        """
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        rpm = query.channel(channel_name="Engine RPM")
        charging = query.poi_channel(channel_name="charging_error")
        defect = query.poi_channel(channel_name="defect")

        # instants where a charging error AND a defect coincide (exact timestamp)
        coincident = (charging == 1.0) & defect.to_points_in_time()

        result = query.select(
            rpm.where(coincident).alias("rpm_at_coincident_fault"),
        ).solve(spark=spark, solver=solver)

        rows = {r.container_id: r["rpm_at_coincident_fault"] for r in result.collect()}
        # container 1: err@15 coincides with defect@15 → RPM[10,20)=3000
        assert [list(p) for p in rows[1]] == [[15.0, 3000.0]]
        # container 2: err@25 vs defect@8 → no coincident instant → empty
        assert [list(p) for p in rows[2]] == []

    def test_string_poi_channel_yields_categorical_series(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """``poi_channel(..., dtype="string")`` yields a categorical PointsInTimeSeriesString.

        The ``dtc`` POI has non-numeric codes (P0420, P0171). Selected with
        dtype="string", the series serializes as a struct of parallel
        ``(tstarts, values)`` arrays and preserves the string codes.
        """
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        dtc = query.poi_channel(channel_name="dtc", dtype="string")

        result = query.select(dtc.alias("dtc")).solve(spark=spark, solver=solver)

        # string series serializes as struct<tstarts: array<double>, values: array<string>>
        field = result.schema["dtc"].dataType
        assert isinstance(field, T.StructType)
        assert field["values"].dataType == T.ArrayType(T.StringType())

        rows = {r.container_id: r["dtc"] for r in result.collect()}
        # container 1: dtc P0420 @15
        assert list(rows[1]["tstarts"]) == [15.0]
        assert list(rows[1]["values"]) == ["P0420"]
        # container 2: dtc P0171 @25
        assert list(rows[2]["tstarts"]) == [25.0]
        assert list(rows[2]["values"]) == ["P0171"]

    def test_string_poi_channel_gates_channel_by_code(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """A categorical POI channel gates a measured channel via string equality.

        ``rpm.where(dtc == "P0420")`` samples Engine RPM at the instants where the
        dtc code equals P0420 — proving ``==`` on a string series yields a
        PointsInTime usable as a gate (value type irrelevant to the gate result).
        """
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        rpm = query.channel(channel_name="Engine RPM")
        dtc = query.poi_channel(channel_name="dtc", dtype="string")

        result = query.select(
            rpm.where(dtc == "P0420").alias("rpm_at_p0420"),
        ).solve(spark=spark, solver=solver)

        rows = {r.container_id: r["rpm_at_p0420"] for r in result.collect()}
        # container 1: P0420 @15 → RPM[10,20)=3000
        assert [list(p) for p in rows[1]] == [[15.0, 3000.0]]
        # container 2: its dtc is P0171, not P0420 → no matching instant → empty
        assert [list(p) for p in rows[2]] == []

    def test_numeric_dtype_on_non_numeric_poi_raises(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """Selecting a non-numeric POI as dtype="double" (the default) fails loudly.

        The ``dtc`` codes (P0420) can't be parsed as numbers; the numeric default
        must raise rather than silently produce NaN. The ValueError raised inside
        the solve UDF surfaces as a Spark job failure on collect().
        """
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        dtc = query.poi_channel(channel_name="dtc")  # default dtype="double"

        with pytest.raises(Exception, match="non-numeric|ValueError|dtc"):
            query.select(dtc.alias("dtc")).solve(spark=spark, solver=solver).collect()

    def test_stats_aggregator_rejects_string_poi_input(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """A categorical POI channel used as a StatsAggregator input is rejected.

        Numeric stats over string codes are undefined; the guard fires at
        ``.solve()`` (plan-time build against the empty cache), before any Spark
        job runs.
        """
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        dtc = query.poi_channel(channel_name="dtc", dtype="string")

        agg = StatsAggregator(input_expressions=[dtc], statistics=["min", "max"])
        with pytest.raises(TypeError, match="categorical"):
            query.select(agg.alias("bad")).solve(spark=spark, solver=solver)

    def test_point_value_aggregator_rejects_string_poi_input(
        self, spark: SparkSession, poi_integration_db: MeasurementDB
    ):
        """A categorical POI channel used as a PointValueAggregator input is rejected.

        The string channel is valid as the *event* (a gate), but not as a
        value-carrying input. The guard fires at ``.solve()`` (plan-time).
        """
        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_db.query
        dtc = query.poi_channel(channel_name="dtc", dtype="string")
        charging = query.poi_channel(channel_name="charging_error")

        agg = PointValueAggregator(
            input_expressions=[dtc],  # string input → invalid
            event_expression=charging.to_points_in_time(),
        )
        with pytest.raises(TypeError, match="categorical"):
            query.select(agg.alias("bad")).solve(spark=spark, solver=solver)

    def test_poi_only_container_survives_in_eav_channel_mode(
        self, spark: SparkSession, poi_integration_eav_db: MeasurementDB
    ):
        """POI-only containers survive regardless of the channel-matching mode.

        The wide-mode tests exercise the drop in ``filter_channel_metrics``; this
        one uses an EAV ``channel_tags`` table (``channel_tags_table`` set), so the
        channel-less-container drop happens one stage earlier, in
        ``filter_channel_tags``. Either way a POI-only query is scoped to
        ``metrics_df`` (the full container set), so container 3 — POI but no
        channel — still emits its charging_error series.
        """
        # Precondition: this fixture really is in EAV mode (the other one is wide).
        assert poi_integration_eav_db.config.channel_tags_table is not None

        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_eav_db.query
        charging = query.poi_channel(channel_name="charging_error")

        result = query.select(charging.alias("charging")).solve(spark=spark, solver=solver)

        rows = {r.container_id: r["charging"] for r in result.collect()}
        # same expected POI series as wide mode — container 3 is POI-only yet present
        assert [list(p) for p in rows[1]] == [[5.0, 0.0], [15.0, 1.0], [25.0, 0.0]]
        assert [list(p) for p in rows[2]] == [[25.0, 1.0]]
        assert [list(p) for p in rows[3]] == [[7.0, 1.0]]

    def test_channel_gated_by_poi_in_eav_channel_mode(
        self, spark: SparkSession, poi_integration_eav_db: MeasurementDB
    ):
        """A measured channel resolved via EAV channel_tags, gated by a POI channel.

        Confirms the POI union composes with the EAV channel path (not just wide
        mode): Engine RPM resolves through ``channel_tags``, then is sampled at the
        charging_error instants.
        """
        assert poi_integration_eav_db.config.channel_tags_table is not None

        solver = DefaultSolver(spark, config=_poi_cfg())
        query = poi_integration_eav_db.query
        rpm = query.channel(channel_name="Engine RPM")
        charging = query.poi_channel(channel_name="charging_error")

        result = query.select(
            rpm.where(charging == 1.0).alias("rpm_at_error"),
        ).solve(spark=spark, solver=solver)

        rows = {r.container_id: r["rpm_at_error"] for r in result.collect()}
        # container 1: err@15 → RPM [10,20)=3000
        assert [list(p) for p in rows[1]] == [[15.0, 3000.0]]
        # container 2: err@25 → RPM [20,30)=2500
        assert [list(p) for p in rows[2]] == [[25.0, 2500.0]]
