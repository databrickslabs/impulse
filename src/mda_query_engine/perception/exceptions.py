class PerceptionNotConfigured(RuntimeError):
    """Raised when a perception-only surface is accessed on a ``MeasurementDB``
    that was constructed without a ``perception_config``.

    Surface methods that raise this: ``object_tracks``, ``frame_embeddings``,
    ``perception_channels``, ``channel_value_labels``, ``playlist_items``,
    ``perception_event_instance_objects``, ``validate_sequence_of_events``,
    ``event_instance_fact`` on ``MeasurementDB``; the ``object_track``
    property on ``QueryBuilder``.
    """
