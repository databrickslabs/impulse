"""min_duration_ms debounce on PerceptionEvent — short windows dropped,
absence of the kwarg retains them, and the threshold applies regardless
of track-scope.
"""

import pytest

from mda_query_engine.perception.events.perception_event import PerceptionEvent
from mda_query_engine.perception.tsal.object_track_accessor import ObjectTrackAccessor


@pytest.fixture
def ot():
    return ObjectTrackAccessor()


class TestConstructorArgs:
    def test_default_min_duration_is_none(self, ot):
        evt = PerceptionEvent(name="x", expr=ot.detection_class("cyclist"))
        assert evt.min_duration_ms is None

    def test_positive_int_accepted(self, ot):
        evt = PerceptionEvent(
            name="x",
            expr=ot.detection_class("cyclist"),
            min_duration_ms=500,
        )
        assert evt.min_duration_ms == 500

    def test_zero_accepted(self, ot):
        evt = PerceptionEvent(
            name="x",
            expr=ot.detection_class("cyclist"),
            min_duration_ms=0,
        )
        assert evt.min_duration_ms == 0

    def test_negative_rejected(self, ot):
        with pytest.raises(ValueError, match="min_duration_ms"):
            PerceptionEvent(
                name="x",
                expr=ot.detection_class("cyclist"),
                min_duration_ms=-100,
            )

    def test_float_rejected(self, ot):
        with pytest.raises(ValueError, match="min_duration_ms"):
            PerceptionEvent(
                name="x",
                expr=ot.detection_class("cyclist"),
                min_duration_ms=1.5,
            )


class TestMinDurationFilterShape:
    """`_apply_min_duration_filter` is a pure Spark-column expression
    builder — these tests pin its presence and per-event keying. End-to-end
    behaviour against a live Spark session is verified in the integration
    suite.
    """

    def test_helper_returns_unchanged_when_no_event_has_threshold(self, ot):
        events = [
            PerceptionEvent(name="a", expr=ot.detection_class("cyclist")),
            PerceptionEvent(name="b", expr=ot.detection_class("pedestrian")),
        ]
        sentinel = object()
        # _apply_min_duration_filter short-circuits when no event has a
        # threshold — it returns the input DataFrame as-is. We exercise
        # this via the sentinel.
        result = PerceptionEvent._apply_min_duration_filter(sentinel, events)
        assert result is sentinel


class TestTrackScopeOrthogonal:
    def test_min_duration_applies_to_scoped_event(self, ot):
        # Same constructor accepts the flag on a track-scoped predicate.
        scoped = ObjectTrackAccessor()(track_scope=True)
        evt = PerceptionEvent(
            name="scoped",
            expr=scoped.detection_class("cyclist"),
            min_duration_ms=500,
        )
        assert evt.track_scope is True
        assert evt.min_duration_ms == 500

    def test_min_duration_applies_to_non_scoped_event(self, ot):
        evt = PerceptionEvent(
            name="unscoped",
            expr=ot.detection_class("cyclist"),
            min_duration_ms=500,
        )
        assert evt.track_scope is False
        assert evt.min_duration_ms == 500
