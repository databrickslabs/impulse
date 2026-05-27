"""Silver-layer schemas for LakeVision perception extension.

Implements ADR-4 (perception_data_model.md): perception_channels as parallel Silver
table for media files (camera frames, LiDAR scans, other binary payloads in UC Volumes).
"""

import pyspark.sql.types as T

# File-path index for camera frames, LiDAR scans, and other binary media in UC Volumes.
# Uses the same container_id + channel_id abstraction as Impulse core channels.
# channel_id maps to a row in channel_tags with key=sensor_type.
# Scene cutting: join on container_id and timestamp BETWEEN event.start_ts AND event.end_ts.
PERCEPTION_CHANNELS = T.StructType(
    [
        T.StructField("container_id", T.LongType(), nullable=False),
        T.StructField("channel_id", T.IntegerType(), nullable=False),
        T.StructField("timestamp", T.LongType(), nullable=False),  # microseconds
        T.StructField("file_path", T.StringType(), nullable=False),  # UC Volume path
        T.StructField("format", T.StringType()),  # jpeg, png, mp4, h264, pcd, …
    ]
)
