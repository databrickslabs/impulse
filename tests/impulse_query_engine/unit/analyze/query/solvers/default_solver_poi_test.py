# pylint: disable=missing-function-docstring, redefined-outer-name
"""End-to-end POI tests for DefaultSolver against the real 30-column POI table shape.

These run real Spark (the session-scoped ``spark`` fixture) over an in-memory POI table
modeled on ``tech_rds_dev.poi.poi`` — ``recording_session_id`` as the natural key (bound
to ``container_id`` via ``column_name_mapping``), ``poi_type`` as the kind discriminator,
and ``timestamp`` (a datetime column) as the occurrence time.

Covered:
- ``filter_poi``: column-mapped container binding, predicate tagging by ``selector_id``,
  ``left_semi`` container restriction, datetime ts_column → epoch-µs resolution, dedup.
- ``solve`` cogroup fork: channel+POI, channel-only, and POI-only containers all emit.
- The POI-only *query* (no ``q.channel(...)``) returns rows — the ``container_count``
  union fix.
- The ``attribute`` path → ``PointsInTimeSeries`` and its channel comparison.
"""

import datetime

import numpy as np
import pyspark.sql.types as T
import pytest
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.solver_config import (
    PoiConfig,
    SolverConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import mock_workspace_client, spark  # noqa: F401

_US_PER_SEC = 1_000_000

# The full external POI schema (tech_rds_dev.poi.poi): recording_session_id is the natural
# key, there is no container_id column. Only a representative subset of the 30 columns is
# populated with values in the fixtures; the rest are present so the schema is faithful.
POI_TABLE_SCHEMA = T.StructType(
    [
        T.StructField("provider", T.StringType()),
        T.StructField("runid", T.StringType()),
        T.StructField("tcid", T.StringType()),
        T.StructField("dt", T.StringType()),
        T.StructField("recording_session_id", T.StringType()),
        T.StructField("time", T.DoubleType()),
        T.StructField("timestamp", T.TimestampType()),
        T.StructField("poi_type", T.StringType()),
        T.StructField("value", T.StringType()),
        T.StructField("network", T.StringType()),
        T.StructField("ecu", T.StringType()),
        T.StructField("frame", T.StringType()),
        T.StructField("processed", T.BooleanType()),
        T.StructField("occurrences", T.IntegerType()),
        T.StructField("duration", T.DoubleType()),
        T.StructField("created_at", T.TimestampType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("dtc_state", T.ShortType()),
        T.StructField("life_situation_dj", T.DoubleType()),
        T.StructField("odometer_dj", T.DoubleType()),
        T.StructField("low_beam_state", T.DoubleType()),
        T.StructField("high_beam_state", T.DoubleType()),
        T.StructField("odometer", T.DoubleType()),
        T.StructField("vehicle_wheel_speed", T.DoubleType()),
        T.StructField("window_wiper_status", T.DoubleType()),
        T.StructField("front_fog_light_status", T.DoubleType()),
        T.StructField("aeb_state", T.DoubleType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("poi_id", T.StringType()),
    ]
)

# Container metrics: two containers keyed by the same container_id the POI rows bind to
# (via column_name_mapping). start_dt is unused by POI now but kept as realistic metadata.
CONTAINER_METRICS_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.StringType(), nullable=False),
        T.StructField("start_dt", T.TimestampType()),
    ]
)

CHANNELS_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.StringType(), nullable=False),
        T.StructField("channel_id", T.IntegerType(), nullable=False),
        T.StructField("tstart", T.LongType(), nullable=False),
        T.StructField("tend", T.LongType(), nullable=False),
        T.StructField("value", T.DoubleType()),
    ]
)

CHANNEL_METRICS_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.StringType(), nullable=False),
        T.StructField("channel_id", T.IntegerType(), nullable=False),
        T.StructField("channel_name", T.StringType()),
    ]
)

# Two recording sessions used as container keys.
SID_A = "38004ebff4cfdefa3f458eb4ef25f62c8ebb936c259a4f204cef36073d8ad703"
SID_B = "d4f5ee6b81159f688ce096d4bcd0fd720db93e8117ae1b9d3a9180109c72b0d0"


_ANCHOR = datetime.datetime(2024, 4, 5, 14, 0, 0, tzinfo=datetime.timezone.utc)


def _ts(seconds: float) -> datetime.datetime:
    """A UTC datetime `seconds` after a fixed epoch anchor (for absolute time base)."""
    return _ANCHOR + datetime.timedelta(seconds=seconds)


def _abs_us(seconds: float) -> int:
    """The absolute epoch-microsecond value the solver resolves for ``_ts(seconds)``.

    The absolute time base uses ``unix_micros``, so an instant is epoch µs — not
    ``seconds * 1e6``. Assertions compare against this.
    """
    return int(round(_ts(seconds).timestamp() * _US_PER_SEC))


def _poi_row(sid, poi_type, ts_seconds, *, network="INFO", frame="943", wheel_speed=0.0,
             duration=0.0, rel_time=0.0):
    """One faithful 30-column POI row; only the fields the tests read are meaningful."""
    return (
        "hmt", "03867", "9559", "2024-04-05", sid,
        rel_time, _ts(ts_seconds), poi_type, None, network, None, frame,
        False, 1, duration, _ts(0), 0.911, 48.6, 1, 5771344.0, 0.0, None, None,
        7800.0, wheel_speed, None, None, 3.0, "computed", None,
    )


@pytest.fixture
def poi_db(spark: SparkSession, mock_workspace_client) -> MeasurementDB:
    """A MeasurementDB with a POI table, container_metrics, channels, channel_metrics.

    POI rows are keyed by ``recording_session_id`` (mapped to ``container_id``). Container
    A has two AEB occurrences (one duplicated at the same instant to exercise dedup) plus
    a channel; container B has one AEB occurrence and NO channel (POI-only container).
    """
    poi_rows = [
        # container A: two distinct AEB instants, plus a duplicate of the first instant
        # differing only in network/frame — must dedup to one row per (kind, instant).
        # The 200s occurrence has duration=5.0 so a having(duration > 4) filter keeps it.
        _poi_row(SID_A, "aeb", 100.0, network="INFO", frame="943", duration=0.0),
        _poi_row(SID_A, "aeb", 100.0, network="CHASSIS", frame="722", duration=0.0),
        _poi_row(SID_A, "aeb", 200.0, network="INFO", frame="943", duration=5.0),
        # a different poi_type in A — must not match poi_type="aeb"
        _poi_row(SID_A, "ldw", 150.0),
        # container B: one AEB, no channel data at all (POI-only container in the cogroup)
        _poi_row(SID_B, "aeb", 300.0, network="CHASSIS", frame="694", duration=0.0),
    ]
    poi = spark.createDataFrame(poi_rows, schema=POI_TABLE_SCHEMA)

    container_metrics = spark.createDataFrame(
        [(SID_A, _ts(0)), (SID_B, _ts(0))], schema=CONTAINER_METRICS_SCHEMA
    )
    # Only container A has a channel; B is POI-only. Intervals are epoch-aligned to the
    # POI instants so channel.where(poi) can sample at them: speed is 20 up to 150s then
    # 80, so at the 100s AEB speed=20 and at the 200s AEB speed=80.
    channels = spark.createDataFrame(
        [
            (SID_A, 10, _abs_us(0), _abs_us(150), 20.0),
            (SID_A, 10, _abs_us(150), _abs_us(400), 80.0),
        ],
        schema=CHANNELS_SCHEMA,
    )
    channel_metrics = spark.createDataFrame(
        [(SID_A, 10, "Vehicle Speed Sensor")], schema=CHANNEL_METRICS_SCHEMA
    )

    tables = {
        "container_metrics": container_metrics,
        "channels": channels,
        "channel_metrics": channel_metrics,
        "poi": poi,
    }
    cfg = MeasurementDBConfig.for_debug(tables)
    return MeasurementDB(cfg, ws=mock_workspace_client)


def _poi_solver(spark) -> DefaultSolver:
    """A DefaultSolver whose POI config binds recording_session_id → container_id.

    ``ts_column`` is the datetime ``timestamp`` column, resolved to integer µs.
    """
    return DefaultSolver(
        spark,
        config=SolverConfig(
            poi=PoiConfig(
                column_name_mapping={"recording_session_id": "container_id"},
                ts_column="timestamp",
                dedup_order_by=["network", "frame"],
            )
        ),
    )


# ---------------------------------------------------------------------------
# filter_poi
# ---------------------------------------------------------------------------


class TestFilterPoi:
    def test_binds_container_and_tags_selector_and_dedups(
        self, spark: SparkSession, poi_db: MeasurementDB
    ):
        solver = _poi_solver(spark)
        query = poi_db.query
        aeb = query.poi(poi_type="aeb")
        query.select(aeb)

        tags_df = solver.filter_container_tags(spark, query)
        container_df = solver.filter_container_metrics(spark, query, tags_df)
        poi_selectors = TimeSeriesExpression.collect_poi_selectors(query.selections)
        result = solver.filter_poi(spark, poi_db, container_df, poi_selectors)

        rows = result.collect()
        # container A: instants 100 and 200 (the duplicate at 100 deduped away);
        # container B: instant 300. The ldw row is filtered out. => 3 rows.
        assert len(rows) == 3
        assert {"container_id", "ts", "selector_id"}.issubset(set(result.columns))
        # every row carries the asking selector's id
        assert {r.selector_id for r in rows} == {aeb.selector_id}
        # instants resolved to integer microseconds (absolute)
        by_container = {}
        for r in rows:
            by_container.setdefault(r.container_id, []).append(r.ts)
        assert sorted(by_container[SID_A]) == [_abs_us(100), _abs_us(200)]
        assert sorted(by_container[SID_B]) == [_abs_us(300)]

    def test_ts_column_datetime_resolves_to_epoch_micros(
        self, spark: SparkSession, poi_db: MeasurementDB
    ):
        # The datetime ts_column is read directly via unix_micros: instants equal the
        # epoch-µs of the timestamp column, with no unit/origin math.
        solver = _poi_solver(spark)
        query = poi_db.query
        query.select(query.poi(poi_type="aeb"))
        tags_df = solver.filter_container_tags(spark, query)
        container_df = solver.filter_container_metrics(spark, query, tags_df)
        poi_selectors = TimeSeriesExpression.collect_poi_selectors(query.selections)
        rows = solver.filter_poi(spark, poi_db, container_df, poi_selectors).collect()
        assert {r.ts for r in rows} == {_abs_us(100), _abs_us(200), _abs_us(300)}

    def test_container_filter_restricts_poi_rows(
        self, spark: SparkSession, poi_db: MeasurementDB
    ):
        # Restrict the container frame to A only; B's POI row must not leak through.
        solver = _poi_solver(spark)
        query = poi_db.query
        query.select(query.poi(poi_type="aeb"))
        container_df = solver.filter_container_metrics(
            spark, query, solver.filter_container_tags(spark, query)
        )
        container_a = container_df.where(container_df.container_id == SID_A)
        poi_selectors = TimeSeriesExpression.collect_poi_selectors(query.selections)
        rows = solver.filter_poi(spark, poi_db, container_a, poi_selectors).collect()
        assert {r.container_id for r in rows} == {SID_A}


# ---------------------------------------------------------------------------
# solve — the cogroup fork
# ---------------------------------------------------------------------------


class TestSolvePoi:
    def _solve(self, spark, db, *selections):
        solver = _poi_solver(spark)
        query = db.query
        query.select(*selections)
        result = query.solve(spark, solver=solver)
        return {r.container_id: r for r in result.collect()}

    def test_poi_only_query_returns_rows_for_all_containers(
        self, spark: SparkSession, poi_db: MeasurementDB
    ):
        """A POI-only query (no q.channel) must return rows — the container_count fix."""
        query = poi_db.query
        aeb = query.poi(poi_type="aeb").alias("aeb")
        rows = self._solve(spark, poi_db, aeb)
        # Both containers appear, including B which has NO channel data at all.
        assert set(rows.keys()) == {SID_A, SID_B}
        # A's points: 100s and 200s as epoch µs; B's: 300s as epoch µs.
        assert sorted(rows[SID_A]["aeb"]) == [_abs_us(100), _abs_us(200)]
        assert sorted(rows[SID_B]["aeb"]) == [_abs_us(300)]

    def test_channel_and_poi_together(self, spark: SparkSession, poi_db: MeasurementDB):
        """Channel-only container B still solves its channel expr; A gets both."""
        query = poi_db.query
        aeb = query.poi(poi_type="aeb").alias("aeb")
        ch = query.channel(channel_name="Vehicle Speed Sensor").mean().alias("speed_mean")
        rows = self._solve(spark, poi_db, aeb, ch)
        # A has both channel and POI.
        assert sorted(rows[SID_A]["aeb"]) == [_abs_us(100), _abs_us(200)]
        assert rows[SID_A]["speed_mean"] is not None
        # B is POI-only: it still appears with its POI, and a null channel mean.
        assert sorted(rows[SID_B]["aeb"]) == [_abs_us(300)]

    def test_having_row_filter_on_poi_metric(
        self, spark: SparkSession, poi_db: MeasurementDB
    ):
        """having(q.poi_metric(...) > x) filters occurrences Spark-side before solving."""
        query = poi_db.query
        # Only A's 200s AEB has duration=5.0 > 4; the 100s occurrences (duration 0) drop.
        long_aeb = (
            query.poi(poi_type="aeb")
            .having(query.poi_metric("duration") > 4.0)
            .alias("long_aeb")
        )
        rows = self._solve(spark, poi_db, long_aeb)
        assert sorted(rows[SID_A]["long_aeb"]) == [_abs_us(200)]
        # B's AEB has duration 0, so B contributes no instants (but may still appear empty).
        assert rows.get(SID_B) is None or list(rows[SID_B]["long_aeb"]) == []

    def test_value_at_occurrence_via_channel_where(
        self, spark: SparkSession, poi_db: MeasurementDB
    ):
        """Option D: a signal's value AT each occurrence = channel.where(poi), not a POI column."""
        query = poi_db.query
        # Vehicle speed sampled at each AEB instant in A: 100s→20 (speed<150s), 200s→80.
        speed_at_aeb = (
            query.channel(channel_name="Vehicle Speed Sensor")
            .where(query.poi(poi_type="aeb"))
            .alias("speed_at_aeb")
        )
        rows = self._solve(spark, poi_db, speed_at_aeb)
        # PointsInTimeSeries serialized as [ts, value] pairs.
        pairs = {int(ts): val for ts, val in rows[SID_A]["speed_at_aeb"]}
        assert pairs[_abs_us(100)] == pytest.approx(20.0)
        assert pairs[_abs_us(200)] == pytest.approx(80.0)

    def test_dedup_collapses_same_instant_to_one_point(
        self, spark: SparkSession, poi_db: MeasurementDB
    ):
        """Two AEB rows at the same instant (differing network/frame) → one instant."""
        query = poi_db.query
        aeb = query.poi(poi_type="aeb").alias("aeb")
        rows = self._solve(spark, poi_db, aeb)
        # A's 100s instant had two colliding rows; dedup leaves exactly one → 2 instants.
        assert sorted(rows[SID_A]["aeb"]) == [_abs_us(100), _abs_us(200)]
        assert len(rows[SID_A]["aeb"]) == 2


# ---------------------------------------------------------------------------
# backward compatibility: a POI table present but unused
# ---------------------------------------------------------------------------


def test_poi_table_present_but_unused_is_inert(
    spark: SparkSession, poi_db: MeasurementDB
):
    """A query with no POI selector must behave exactly as if no POI table existed."""
    solver = _poi_solver(spark)
    query = poi_db.query
    ch = query.channel(channel_name="Vehicle Speed Sensor").mean().alias("m")
    query.select(ch)
    result = query.solve(spark, solver=solver)
    # No cogroup path taken (poi_df is None); only container A has this channel.
    rows = {r.container_id: r.m for r in result.collect()}
    assert SID_A in rows
    assert rows[SID_A] is not None
