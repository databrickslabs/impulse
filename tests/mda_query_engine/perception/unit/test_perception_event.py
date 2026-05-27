"""PerceptionEvent — event type identifier and definition-hash stability."""

import pytest

from mda_query_engine.perception.events.perception_event import PerceptionEvent
from mda_query_engine.perception.tsal.object_track_accessor import ObjectTrackAccessor


@pytest.fixture
def ot():
    return ObjectTrackAccessor()


class TestPerceptionEventTypeString:
    def test_get_event_type_str_is_perception_event(self, ot):
        evt = PerceptionEvent(
            name="cyclist_present",
            expr=ot.detection_class("cyclist"),
        )
        assert evt.get_event_type_str() == "PERCEPTION_EVENT"


class TestPerceptionEventDefinitionHash:
    def test_hash_is_stable_across_identical_predicates(self, ot):
        evt_a = PerceptionEvent(name="cyclist_a", expr=ot.detection_class("cyclist"))
        evt_b = PerceptionEvent(name="cyclist_b", expr=ot.detection_class("cyclist"))
        # Name differs but expression is identical: hashes must match because
        # only the expression contributes to the definition hash.
        assert evt_a.determine_definition_hash() == evt_b.determine_definition_hash()

    def test_hash_changes_when_predicate_changes(self, ot):
        evt_cyclist = PerceptionEvent(
            name="x", expr=ot.detection_class("cyclist")
        )
        evt_pedestrian = PerceptionEvent(
            name="x", expr=ot.detection_class("pedestrian")
        )
        assert (
            evt_cyclist.determine_definition_hash()
            != evt_pedestrian.determine_definition_hash()
        )

    def test_hash_changes_when_compound_predicate_changes(self, ot):
        evt_a = PerceptionEvent(
            name="x",
            expr=ot.detection_class("cyclist") & ot.azimuth("front_left"),
        )
        evt_b = PerceptionEvent(
            name="x",
            expr=ot.detection_class("cyclist") & ot.azimuth("rear_right"),
        )
        assert evt_a.determine_definition_hash() != evt_b.determine_definition_hash()

    def test_hash_is_int_fitting_long(self, ot):
        evt = PerceptionEvent(name="x", expr=ot.detection_class("cyclist"))
        h = evt.determine_definition_hash()
        assert isinstance(h, int)
        # Signed 64-bit
        assert -(2**63) <= h < 2**63


class TestPerceptionEventDict:
    def test_as_dict_includes_event_type(self, ot):
        evt = PerceptionEvent(
            name="cyclist_present",
            expr=ot.detection_class("cyclist"),
            desc="A cyclist is present in the front_left azimuth",
        )
        d = evt.as_dict()
        assert d["event_type"] == "PERCEPTION_EVENT"
        assert d["event_name"] == "cyclist_present"
        assert d["definition_hash"] == evt.determine_definition_hash()
