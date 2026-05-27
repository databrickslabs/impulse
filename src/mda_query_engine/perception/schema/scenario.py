"""Scenario-layer schemas for LakeVision perception extension.

Implements:
- ADR-6  (perception_data_model.md): object_tracks as the fused per-object scenario search table
- ADR-10 (perception_data_model.md): TSAL-gated downsampling strategy for object_tracks
- ADR-8  (perception_data_model.md): per-frame embeddings in Mosaic AI Vector Search
- Phase 2 (scenario search), Phase 3 (similarity search) data models

`playlist_items` lives in Impulse Core (`mda_reporting.persist.fact_schema`):
it operates on `event_instance_fact` and has no perception dependency.
"""

import pyspark.sql.types as T

# One fused row per tracked object per frame.
# Sensor-agnostic: populated from fused output, not per-sensor raw geometry.
# Stores only attributes needed for scenario search — not raw 3D geometry (see annotation.py).
# Ingestion mode is controlled by ObjectTracksConfig (tsal_gated or full_stride).
OBJECT_TRACKS = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("frame_ts", T.LongType(), nullable=False),  # microseconds; aligns to channels.tstart
        T.StructField("object_id", T.LongType(), nullable=False),  # stable tracked identity across frames
        T.StructField("detection_class", T.StringType()),  # pedestrian, car, cyclist, truck, motorcycle, bus
        T.StructField("distance_m", T.DoubleType()),  # range from ego; primary source: LiDAR
        T.StructField("lane_offset", T.IntegerType()),  # relative to ego lane: −2, −1, 0, +1, +2
        T.StructField("relative_velocity_ms", T.DoubleType()),  # negative = approaching; primary: radar Doppler
        T.StructField(
            "azimuth", T.StringType()
        ),  # sector enum: front, front_left, front_right, left, right, rear_left, rear_right, rear
        T.StructField("confidence", T.DoubleType()),
        T.StructField("source", T.StringType()),  # pipe-delimited sensor provenance, e.g. lidar|radar|camera
    ]
)

# Perception side-car for per-object windowed perception events.
# Populated only when a PerceptionEvent's predicate has track_scope=True selectors.
# Core's event_instance_fact is unchanged; perception-aware reads LEFT JOIN this side-car
# via MeasurementDB.event_instance_fact(spark) to recover object_id.
# Physical location: <catalog>.perception_silver.perception_event_instance_objects.
# Primary key: (container_id, event_id, event_instance_id) — at most one row per
# event_instance_fact row. Idempotent write so partial-failure retries are safe.
PERCEPTION_EVENT_INSTANCE_OBJECTS = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("event_id", T.LongType(), nullable=False),
        T.StructField("event_instance_id", T.LongType(), nullable=False),
        T.StructField("object_id", T.LongType(), nullable=False),
    ]
)

# One embedding vector per camera frame, at ~5 Hz (front camera, PoC scope).
# Two embedding types coexist via embedding_type: 'image' (Phase 3) and 'text' (Phase 5).
# Scene-level similarity is computed at query time by averaging frame vectors within a
# time window — do not pre-aggregate at ingest time (ADR-8).
FRAME_EMBEDDINGS = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("channel_id", T.IntegerType(), nullable=False),  # FK to perception_channels
        T.StructField("frame_ts", T.LongType(), nullable=False),  # microseconds; aligns to perception_channels
        T.StructField("embedding_type", T.StringType(), nullable=False),  # 'image' (Phase 3) or 'text' (Phase 5)
        T.StructField("embedding", T.ArrayType(T.FloatType()), nullable=False),  # dense vector
        T.StructField("model_version", T.StringType(), nullable=False),  # required for reproducibility
        T.StructField("frame_description", T.StringType()),  # auto-generated text; populated for 'text' type only
    ]
)

