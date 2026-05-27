"""Event types that consume ``object_tracks`` alongside scalar channels."""

from .perception_event import PerceptionEvent
from .sequence_validation import validate_sequence_of_events_track_scope
from .side_car import materialize_event_instance_object_rows

__all__ = [
    "PerceptionEvent",
    "validate_sequence_of_events_track_scope",
    "materialize_event_instance_object_rows",
]
