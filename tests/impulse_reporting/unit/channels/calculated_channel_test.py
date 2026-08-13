# pylint: disable=missing-function-docstring
"""Unit tests for the reporting-layer CalculatedChannel class."""

import pytest
import pyspark.sql.types as T

from impulse_query_engine.analyze.metadata.time_series_expression import TimeSeriesSelector
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_reporting.channels.calculated_channel import CalculatedChannel
from tests.conftest import basic_narrow_db, spark  # noqa: F401  (pytest fixtures)

_IDENTITY = {"channel_name": "speed_kmh", "data_key": "CALC"}


def _channel(name="speed_kmh", identity=None):
    # TimeSeriesSelector builds to a SampleSeries; * scalar keeps it a SampleSeries.
    expr = TimeSeriesSelector(None) * 3.6
    return CalculatedChannel(name=name, expr=expr, identity=identity or dict(_IDENTITY))


class TestConstruction:
    def test_stores_identity(self):
        ch = _channel()
        assert ch.identity == _IDENTITY

    def test_attributes_normalized_to_str(self):
        # attributes are optional; when given they are coerced to a str->str dict.
        ch = CalculatedChannel(
            name="speed_kmh",
            expr=TimeSeriesSelector(None) * 3.6,
            identity=dict(_IDENTITY),
            attributes={"unit": "kmh", "scale": 3.6},
        )
        assert ch.attributes == {"unit": "kmh", "scale": "3.6"}

    def test_attributes_default_empty(self):
        # No attributes → empty dict, not None.
        assert _channel().attributes == {}

    def test_get_id_deterministic_and_identity_derived(self):
        # Same identity → same id regardless of name; different identity → different id.
        a = _channel(name="a")
        b = _channel(name="b")
        assert a.get_id() == b.get_id()
        c = _channel(identity={"channel_name": "other", "data_key": "CALC"})
        assert c.get_id() != a.get_id()

    def test_get_id_positive_int32(self):
        ch = _channel()
        assert 0 <= ch.get_id() <= 0x7FFFFFFF

    def test_get_expression_returns_wrapped_expression(self):
        # Returns the wrapped query-engine CalculatedChannel expression. Channels are
        # dispatched via their own narrow solve, not collect_solvable_expressions, so
        # this is never passed to the wide batch solve.
        ch = _channel()
        assert ch.get_expression() is ch.expression

    def test_get_expression_str_is_wrapped_expression_str(self):
        ch = _channel()
        assert ch.get_expression_str() == str(ch.expression)

    def test_channel_type_str(self):
        assert _channel().get_channel_type_str() == "CALCULATED_CHANNEL"

    def test_rejects_non_sample_series_expr(self):
        # `> 0` yields an Intervals-producing op, not a SampleSeries.
        with pytest.raises(ValueError, match="SampleSeries"):
            CalculatedChannel(
                name="bad", expr=(TimeSeriesSelector(None) > 0), identity=dict(_IDENTITY)
            )

    def test_accepts_arbitrary_identity_keys(self):
        # Identity persists as a map, so any non-empty key set is valid.
        ch = CalculatedChannel(
            name="ok",
            expr=(TimeSeriesSelector(None) * 2),
            identity={"sensor_id": "s1", "unit": "rpm"},
        )
        assert ch.identity == {"sensor_id": "s1", "unit": "rpm"}

    def test_rejects_empty_identity(self):
        with pytest.raises(ValueError, match="non-empty identity"):
            CalculatedChannel(name="bad", expr=(TimeSeriesSelector(None) * 2), identity={})


class TestMetadata:
    def test_as_dict_keys_and_values(self):
        ch = _channel()
        d = ch.as_dict()
        assert set(d) == {
            "channel_id",
            "report_id",
            "channel_type",
            "channel_description",
            "channel_expression",
            "identity",
            "definition_hash",
            "attributes",
        }
        assert d["channel_id"] == ch.get_id()
        assert d["report_id"] == -1
        assert d["channel_type"] == "CALCULATED_CHANNEL"
        assert d["identity"] == _IDENTITY
        assert isinstance(d["definition_hash"], int)

    def test_as_spark_row_field_count(self):
        assert len(_channel().as_spark_row()) == 8

    def test_definition_hash_ignores_name_and_desc(self):
        a = CalculatedChannel("a", TimeSeriesSelector(None) * 3.6, dict(_IDENTITY), desc="one")
        b = CalculatedChannel("b", TimeSeriesSelector(None) * 3.6, dict(_IDENTITY), desc="two")
        assert a.determine_definition_hash() == b.determine_definition_hash()

    def test_definition_hash_changes_with_expression(self):
        a = CalculatedChannel("a", TimeSeriesSelector(None) * 3.6, dict(_IDENTITY))
        b = CalculatedChannel("a", TimeSeriesSelector(None) * 2.0, dict(_IDENTITY))
        assert a.determine_definition_hash() != b.determine_definition_hash()

    def test_definition_hash_independent_of_identity_key_order(self):
        # Same identity, different key insertion order → same hash (no spurious
        # "changed" classification in incremental runs).
        a = CalculatedChannel(
            "a", TimeSeriesSelector(None) * 3.6, {"channel_name": "s", "data_key": "CALC"}
        )
        b = CalculatedChannel(
            "a", TimeSeriesSelector(None) * 3.6, {"data_key": "CALC", "channel_name": "s"}
        )
        assert a.determine_definition_hash() == b.determine_definition_hash()

    def test_definition_hash_changes_with_identity_value(self):
        a = CalculatedChannel(
            "a", TimeSeriesSelector(None) * 3.6, {"channel_name": "s", "data_key": "CALC"}
        )
        b = CalculatedChannel(
            "a", TimeSeriesSelector(None) * 3.6, {"channel_name": "other", "data_key": "CALC"}
        )
        assert a.determine_definition_hash() != b.determine_definition_hash()


class TestDetermineCalculatedChannels:
    def test_returns_none_when_empty(self, spark):
        assert (
            CalculatedChannel.determine_calculated_channels(spark, [], query=None, solver=None)
            is None
        )

    def test_returns_fact_columns_with_matching_channel_id(self, spark, basic_narrow_db):
        q = basic_narrow_db.query
        ch = CalculatedChannel(
            name="rpm_x2",
            expr=q.channel(channel_name="Engine RPM") * 2,
            identity={"channel_name": "rpm_x2", "data_key": "CALC"},
        )
        df = CalculatedChannel.determine_calculated_channels(
            spark, [ch], query=q, solver=DefaultSolver(spark)
        )
        # Identity lives on the dimension (joined via channel_id), not the fact.
        assert df.columns == [
            "container_id",
            "channel_id",
            "tstart",
            "tend",
            "value",
        ]
        ids = {r["channel_id"] for r in df.select("channel_id").distinct().collect()}
        assert ids == {ch.get_id()}


_FACT_SCHEMA = T.StructType(
    [
        T.StructField("container_id", T.LongType(), False),
        T.StructField("channel_id", T.LongType(), False),
        T.StructField("tstart", T.LongType(), False),
        T.StructField("tend", T.LongType(), False),
        T.StructField("value", T.DoubleType(), True),
    ]
)


class TestDetermineChannelMetrics:
    def test_returns_none_when_fact_none(self, spark):
        assert (
            CalculatedChannel.determine_channel_metrics(spark, [], None, attribute_columns=[])
            is None
        )

    def test_duration_weighted_values(self, spark):
        ch = CalculatedChannel("a", TimeSeriesSelector(None) * 1.0, {"channel_name": "s"})
        cid = ch.get_id()
        # Two intervals with different durations: [0,1) value 10, [1,3) value 20.
        # duration-weighted mean = (10*1 + 20*2) / (1+2) = 50/3.
        fact = spark.createDataFrame(
            [(1, cid, 0, 1, 10.0), (1, cid, 1, 3, 20.0)], schema=_FACT_SCHEMA
        )
        out = CalculatedChannel.determine_channel_metrics(spark, [ch], fact, attribute_columns=[])
        rows = out.collect()
        assert len(rows) == 1
        r = rows[0]
        assert r["container_id"] == 1
        assert r["channel_id"] == cid
        assert r["value_type"] == "double"
        assert r["duration"] == 3  # max(tend) - min(tstart) = 3 - 0
        assert r["min"] == 10.0
        assert r["max"] == 20.0
        assert r["mean"] == pytest.approx(50.0 / 3.0)
        assert r["channel_name"] == "s"

    def test_nan_values_ignored_in_min_max_mean(self, spark):
        ch = CalculatedChannel("a", TimeSeriesSelector(None) * 1.0, {"channel_name": "s"})
        cid = ch.get_id()
        # [0,1) value 10, [1,2) NaN → NaN excluded from min/max/weighted-sum, but its
        # duration still counts in the denominator (matches SampleSeries.mean).
        fact = spark.createDataFrame(
            [(1, cid, 0, 1, 10.0), (1, cid, 1, 2, float("nan"))], schema=_FACT_SCHEMA
        )
        r = CalculatedChannel.determine_channel_metrics(
            spark, [ch], fact, attribute_columns=[]
        ).collect()[0]
        assert r["min"] == 10.0
        assert r["max"] == 10.0
        # (10*1) / (1+1) = 5.0
        assert r["mean"] == pytest.approx(5.0)

    def test_zero_total_duration_mean_is_null_not_error(self, spark):
        # A group of only zero-duration point-in-time samples (tstart == tend) has
        # sum(dur) == 0. Under ANSI mode (Spark 4.0 default) plain division would
        # raise DIVIDE_BY_ZERO; try_divide yields a null mean instead.
        ch = CalculatedChannel("a", TimeSeriesSelector(None) * 1.0, {"channel_name": "s"})
        cid = ch.get_id()
        fact = spark.createDataFrame(
            [(1, cid, 5, 5, 10.0), (1, cid, 7, 7, 20.0)], schema=_FACT_SCHEMA
        )
        r = CalculatedChannel.determine_channel_metrics(
            spark, [ch], fact, attribute_columns=[]
        ).collect()[0]
        assert r["duration"] == 2  # max(tend) - min(tstart) = 7 - 5
        assert r["mean"] is None
        # min/max still resolve from the values.
        assert r["min"] == 10.0
        assert r["max"] == 20.0

    def test_dynamic_identity_columns_union(self, spark):
        # Two channels with DIFFERENT identity keys → output has the union, null
        # where a channel omits a key.
        ch1 = CalculatedChannel("a", TimeSeriesSelector(None) * 1.0, {"channel_name": "s1"})
        ch2 = CalculatedChannel(
            "b", TimeSeriesSelector(None) * 1.0, {"channel_name": "s2", "data_key": "K"}
        )
        fact = spark.createDataFrame(
            [(1, ch1.get_id(), 0, 1, 1.0), (1, ch2.get_id(), 0, 1, 2.0)],
            schema=_FACT_SCHEMA,
        )
        out = CalculatedChannel.determine_channel_metrics(
            spark, [ch1, ch2], fact, attribute_columns=[]
        )
        assert "channel_name" in out.columns
        assert "data_key" in out.columns
        by_id = {r["channel_id"]: r for r in out.collect()}
        assert by_id[ch1.get_id()]["channel_name"] == "s1"
        assert by_id[ch1.get_id()]["data_key"] is None  # ch1 has no data_key
        assert by_id[ch2.get_id()]["data_key"] == "K"

    def test_attribute_columns_config_selected(self, spark):
        # A configured attribute key surfaces as a column; unconfigured attributes
        # do not. A channel omitting the key gets null.
        ch1 = CalculatedChannel(
            "a", TimeSeriesSelector(None) * 1.0, {"channel_name": "s1"}, attributes={"unit": "kmh"}
        )
        ch2 = CalculatedChannel(
            "b", TimeSeriesSelector(None) * 1.0, {"channel_name": "s2"}, attributes={"scale": "2"}
        )
        fact = spark.createDataFrame(
            [(1, ch1.get_id(), 0, 1, 1.0), (1, ch2.get_id(), 0, 1, 2.0)],
            schema=_FACT_SCHEMA,
        )
        out = CalculatedChannel.determine_channel_metrics(
            spark, [ch1, ch2], fact, attribute_columns=["unit"]
        )
        assert "unit" in out.columns
        assert "scale" not in out.columns  # not configured
        by_id = {r["channel_id"]: r for r in out.collect()}
        assert by_id[ch1.get_id()]["unit"] == "kmh"
        assert by_id[ch2.get_id()]["unit"] is None

    def test_no_attribute_columns_by_default(self, spark):
        ch = CalculatedChannel(
            "a", TimeSeriesSelector(None) * 1.0, {"channel_name": "s"}, attributes={"unit": "kmh"}
        )
        fact = spark.createDataFrame([(1, ch.get_id(), 0, 1, 1.0)], schema=_FACT_SCHEMA)
        out = CalculatedChannel.determine_channel_metrics(spark, [ch], fact, attribute_columns=[])
        assert out.columns == [
            "container_id",
            "channel_id",
            "channel_name",
            "value_type",
            "duration",
            "min",
            "max",
            "mean",
        ]

    def test_identity_wins_on_attribute_collision(self, spark):
        # A key present in BOTH identity and attribute_columns yields the identity
        # value, and only one column.
        ch = CalculatedChannel(
            "a",
            TimeSeriesSelector(None) * 1.0,
            {"channel_name": "s", "unit": "identity_unit"},
            attributes={"unit": "attr_unit"},
        )
        fact = spark.createDataFrame([(1, ch.get_id(), 0, 1, 1.0)], schema=_FACT_SCHEMA)
        out = CalculatedChannel.determine_channel_metrics(
            spark, [ch], fact, attribute_columns=["unit"]
        )
        assert out.columns.count("unit") == 1
        assert out.collect()[0]["unit"] == "identity_unit"

    def test_kpis_subset_only_emits_selected(self, spark):
        # Selecting a subset yields only those KPI columns (plus the fixed
        # container/channel/identity/value_type columns).
        ch = CalculatedChannel("a", TimeSeriesSelector(None) * 1.0, {"channel_name": "s"})
        fact = spark.createDataFrame([(1, ch.get_id(), 0, 2, 10.0)], schema=_FACT_SCHEMA)
        out = CalculatedChannel.determine_channel_metrics(
            spark, [ch], fact, attribute_columns=[], kpis=["mean"]
        )
        assert out.columns == [
            "container_id",
            "channel_id",
            "channel_name",
            "value_type",
            "mean",
        ]
        assert out.collect()[0]["mean"] == pytest.approx(10.0)

    def test_kpis_order_is_preserved(self, spark):
        # The KPI columns appear in the configured order (tail of the schema).
        ch = CalculatedChannel("a", TimeSeriesSelector(None) * 1.0, {"channel_name": "s"})
        fact = spark.createDataFrame([(1, ch.get_id(), 0, 2, 10.0)], schema=_FACT_SCHEMA)
        out = CalculatedChannel.determine_channel_metrics(
            spark, [ch], fact, attribute_columns=[], kpis=["max", "min", "duration"]
        )
        assert out.columns[-3:] == ["max", "min", "duration"]

    def test_kpis_default_is_the_four(self, spark):
        # kpis=None → default duration, min, max, mean (in order).
        ch = CalculatedChannel("a", TimeSeriesSelector(None) * 1.0, {"channel_name": "s"})
        fact = spark.createDataFrame([(1, ch.get_id(), 0, 2, 10.0)], schema=_FACT_SCHEMA)
        out = CalculatedChannel.determine_channel_metrics(spark, [ch], fact, attribute_columns=[])
        assert out.columns[-4:] == ["duration", "min", "max", "mean"]

    def test_unknown_kpi_raises(self, spark):
        ch = CalculatedChannel("a", TimeSeriesSelector(None) * 1.0, {"channel_name": "s"})
        fact = spark.createDataFrame([(1, ch.get_id(), 0, 2, 10.0)], schema=_FACT_SCHEMA)
        with pytest.raises(ValueError, match="Unknown calculated-channel KPI"):
            CalculatedChannel.determine_channel_metrics(
                spark, [ch], fact, attribute_columns=[], kpis=["bogus"]
            )
