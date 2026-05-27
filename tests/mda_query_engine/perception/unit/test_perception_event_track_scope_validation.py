"""Predicate-mixing validation on PerceptionEvent — a single predicate may
not mix track-scoped and non-track-scoped PerceptionSelectors.
"""

import pytest

from mda_query_engine.perception.events.perception_event import PerceptionEvent
from mda_query_engine.perception.tsal.object_track_accessor import ObjectTrackAccessor


@pytest.fixture
def ot():
    return ObjectTrackAccessor()


@pytest.fixture
def ot_scoped():
    return ObjectTrackAccessor()(track_scope=True)


class TestRejectsMixedPredicates:
    def test_mixing_scoped_and_unscoped_raises(self, ot, ot_scoped):
        mixed = ot_scoped.detection_class("cyclist") & ot.azimuth("front_left")
        with pytest.raises(ValueError, match="track-scoped"):
            PerceptionEvent(name="mixed", expr=mixed)

    def test_mixing_via_or_also_raises(self, ot, ot_scoped):
        mixed = ot_scoped.detection_class("cyclist") | ot.azimuth("front_left")
        with pytest.raises(ValueError, match="track-scoped"):
            PerceptionEvent(name="mixed_or", expr=mixed)

    def test_mixing_deeper_in_tree_raises(self, ot, ot_scoped):
        mixed = (
            ot_scoped.detection_class("cyclist")
            & (ot_scoped.distance_m < 8.0)
            & ot.azimuth("front_left")
        )
        with pytest.raises(ValueError, match="track-scoped"):
            PerceptionEvent(name="deeply_mixed", expr=mixed)


class TestAcceptsConsistentPredicates:
    def test_all_scoped_accepted(self, ot_scoped):
        expr = ot_scoped.detection_class("cyclist") & ot_scoped.azimuth("front_left")
        evt = PerceptionEvent(name="scoped", expr=expr)
        assert evt.track_scope is True

    def test_all_unscoped_accepted(self, ot):
        expr = ot.detection_class("cyclist") & ot.azimuth("front_left")
        evt = PerceptionEvent(name="unscoped", expr=expr)
        assert evt.track_scope is False

    def test_single_scoped_leaf_accepted(self, ot_scoped):
        evt = PerceptionEvent(
            name="single", expr=ot_scoped.detection_class("cyclist")
        )
        assert evt.track_scope is True

    def test_single_unscoped_leaf_accepted(self, ot):
        evt = PerceptionEvent(
            name="single", expr=ot.detection_class("cyclist")
        )
        assert evt.track_scope is False
