# pylint: disable=missing-function-docstring
"""Unit tests for the reporting-layer CalculatedChannel class."""

import pytest

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

    def test_get_expression_is_none(self):
        # Channels drive their own solve, so they are excluded from the batch solve.
        assert _channel().get_expression() is None

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
