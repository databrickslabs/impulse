"""Phase 4 annotation schemas for LakeVision perception extension.

Implements:
- ADR-7  (perception_data_model.md): build Phase 4 only when a downstream consumer exists
- ADR-9  (perception_data_model.md): OpenLABEL is an export format, not a storage format
- Phase 4 (ML audit trail) data model: per-sensor geometry for labeling tool handoff,
  ML training data export, and homologation evidence

These tables are the Delta-native, columnar projection of OpenLABEL semantics.
OpenLABEL JSON is generated on demand from these tables at the labeling tool boundary —
it is a derived, regenerable artifact; these tables are the source of truth.

Per ADR-7: do not populate these tables until a labeling tool, safety case workflow,
or ML training pipeline is scoped and ready to ingest.
"""

import pyspark.sql.types as T

# 2D bounding boxes from camera sensors.
# One row per detected object per camera sensor per frame.
CAMERA_OBJECT_DETECTIONS = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("frame_ts", T.LongType(), nullable=False),  # microseconds
        T.StructField("object_id", T.LongType(), nullable=False),
        T.StructField("detection_class", T.StringType()),
        T.StructField("confidence", T.DoubleType()),
        T.StructField("sensor_id", T.StringType()),  # e.g. front, rear_left
        T.StructField("x1", T.IntegerType()),  # pixel coordinates, top-left
        T.StructField("y1", T.IntegerType()),
        T.StructField("x2", T.IntegerType()),  # pixel coordinates, bottom-right
        T.StructField("y2", T.IntegerType()),
    ]
)

# 3D bounding cuboids from LiDAR.
# Vehicle frame: x=forward, y=left, z=up, meters.
LIDAR_OBJECT_DETECTIONS = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("frame_ts", T.LongType(), nullable=False),
        T.StructField("object_id", T.LongType(), nullable=False),
        T.StructField("detection_class", T.StringType()),
        T.StructField("confidence", T.DoubleType()),
        T.StructField("sensor_id", T.StringType()),  # e.g. roof_lidar
        T.StructField("cx", T.DoubleType()),  # cuboid center x
        T.StructField("cy", T.DoubleType()),
        T.StructField("cz", T.DoubleType()),
        T.StructField("length", T.DoubleType()),
        T.StructField("width", T.DoubleType()),
        T.StructField("height", T.DoubleType()),
        T.StructField("yaw_rad", T.DoubleType()),
    ]
)

# Polar returns from radar.
# radial_velocity_ms: negative = approaching.
RADAR_OBJECT_DETECTIONS = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("frame_ts", T.LongType(), nullable=False),
        T.StructField("object_id", T.LongType(), nullable=False),
        T.StructField("detection_class", T.StringType()),
        T.StructField("confidence", T.DoubleType()),
        T.StructField("sensor_id", T.StringType()),
        T.StructField("range_m", T.DoubleType()),
        T.StructField("azimuth_rad", T.DoubleType()),
        T.StructField("radial_velocity_ms", T.DoubleType()),  # negative = approaching
    ]
)

# Polynomial lane boundary per camera sensor per frame.
# c0–c3 are 3rd-order polynomial coefficients in vehicle frame lateral offset (meters).
LANE_MARKINGS = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("frame_ts", T.LongType(), nullable=False),
        T.StructField("sensor_id", T.StringType()),
        T.StructField("boundary", T.StringType()),  # left or right
        T.StructField("marking_type", T.StringType()),  # solid, dashed, double_solid, …
        T.StructField("c0", T.DoubleType()),
        T.StructField("c1", T.DoubleType()),
        T.StructField("c2", T.DoubleType()),
        T.StructField("c3", T.DoubleType()),
    ]
)

_POINT_2D = T.StructType(
    [T.StructField("x", T.DoubleType()), T.StructField("y", T.DoubleType())]
)

# Drivable area polygon per sensor per frame.
# boundary_pts: array of (x, y) structs in vehicle frame, meters.
FREE_SPACE = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("frame_ts", T.LongType(), nullable=False),
        T.StructField("sensor_id", T.StringType()),
        T.StructField("boundary_pts", T.ArrayType(_POINT_2D)),
    ]
)

_WAYPOINT = T.StructType(
    [
        T.StructField("x", T.DoubleType()),
        T.StructField("y", T.DoubleType()),
        T.StructField("z", T.DoubleType()),
        T.StructField("t", T.LongType()),  # microseconds
    ]
)

# Per-object predicted waypoints from the planning stack.
# waypoints: array of (x, y, z, t) structs in vehicle frame.
PREDICTED_TRAJECTORIES = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("frame_ts", T.LongType(), nullable=False),
        T.StructField("object_id", T.LongType(), nullable=False),
        T.StructField("detection_class", T.StringType()),
        T.StructField("horizon_ms", T.IntegerType()),
        T.StructField("waypoints", T.ArrayType(_WAYPOINT)),
    ]
)
