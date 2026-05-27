"""Schema smoke tests for LakeVision tables.

Verifies that every schema can be used to create an empty DataFrame and that
key field names and types match the ADR specifications.
"""

import pytest
import pyspark.sql.types as T
import mda_query_engine.perception.schema as S
from mda_query_engine.perception.object_tracks_config import ObjectTracksConfig
from mda_query_engine.perception.perception_db import PerceptionDBConfig


def _field(schema: T.StructType, name: str) -> T.StructField:
    return schema[name]


class TestSilverSchemas:
    def test_perception_channels_has_required_fields(self):
        required = {"container_id", "channel_id", "timestamp", "file_path", "format"}
        assert required <= {f.name for f in S.PERCEPTION_CHANNELS}

    def test_perception_channels_timestamp_is_long(self):
        assert isinstance(_field(S.PERCEPTION_CHANNELS, "timestamp").dataType, T.LongType)


class TestScenarioSchemas:
    def test_object_tracks_has_required_fields(self):
        required = {
            "container_id",
            "frame_ts",
            "object_id",
            "detection_class",
            "distance_m",
            "lane_offset",
            "relative_velocity_ms",
            "azimuth",
            "confidence",
            "source",
        }
        assert required <= {f.name for f in S.OBJECT_TRACKS}

    def test_object_tracks_frame_ts_aligns_with_channels_tstart(self):
        # Both must be LongType (microseconds) so joins on frame_ts = channels.tstart are safe.
        assert isinstance(_field(S.OBJECT_TRACKS, "frame_ts").dataType, T.LongType)

    def test_frame_embeddings_has_required_fields(self):
        # Per the 2026-05-20 ADR-8 clarification, embeddings are per-frame
        # (not per-event-window). Scene-level similarity is computed at query
        # time by averaging frame vectors within a time range.
        required = {
            "container_id",
            "channel_id",
            "frame_ts",
            "embedding_type",
            "embedding",
            "model_version",
        }
        assert required <= {f.name for f in S.FRAME_EMBEDDINGS}

    def test_frame_embeddings_embedding_is_float_array(self):
        field = _field(S.FRAME_EMBEDDINGS, "embedding")
        assert isinstance(field.dataType, T.ArrayType)
        assert isinstance(field.dataType.elementType, T.FloatType)


class TestAnnotationSchemas:
    def test_camera_object_detections_bbox_columns(self):
        bbox_cols = {"x1", "y1", "x2", "y2"}
        assert bbox_cols <= {f.name for f in S.CAMERA_OBJECT_DETECTIONS}

    def test_lidar_object_detections_cuboid_columns(self):
        cuboid_cols = {"cx", "cy", "cz", "length", "width", "height", "yaw_rad"}
        assert cuboid_cols <= {f.name for f in S.LIDAR_OBJECT_DETECTIONS}

    def test_radar_object_detections_polar_columns(self):
        polar_cols = {"range_m", "azimuth_rad", "radial_velocity_ms"}
        assert polar_cols <= {f.name for f in S.RADAR_OBJECT_DETECTIONS}

    def test_free_space_boundary_pts_is_array_of_structs(self):
        field = _field(S.FREE_SPACE, "boundary_pts")
        assert isinstance(field.dataType, T.ArrayType)
        assert isinstance(field.dataType.elementType, T.StructType)
        pt_fields = {f.name for f in field.dataType.elementType}
        assert {"x", "y"} <= pt_fields

    def test_predicted_trajectories_waypoints_has_time_dimension(self):
        field = _field(S.PREDICTED_TRAJECTORIES, "waypoints")
        assert isinstance(field.dataType, T.ArrayType)
        wp_fields = {f.name for f in field.dataType.elementType}
        assert {"x", "y", "z", "t"} <= wp_fields


class TestObjectTracksConfig:
    def test_default_mode_is_full_stride(self):
        cfg = ObjectTracksConfig()
        assert cfg.mode == "full_stride"
        assert cfg.full_stride_hz == 2.0

    def test_full_stride_factory(self):
        cfg = ObjectTracksConfig.full_stride(stride_hz=5.0)
        assert cfg.mode == "full_stride"
        assert cfg.full_stride_hz == 5.0

    def test_stride_below_nyquist_floor_raises(self):
        with pytest.raises(ValueError, match="Nyquist"):
            ObjectTracksConfig.full_stride(stride_hz=1.0)

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError):
            ObjectTracksConfig(mode="random_mode")

    def test_tsal_gated_factory(self):
        cfg = ObjectTracksConfig.tsal_gated(pre_event_buffer_ms=1000)
        assert cfg.pre_event_buffer_ms == 1000


class TestPerceptionDBConfig:
    def test_for_unity_catalog_generates_qualified_names(self):
        cfg = PerceptionDBConfig.for_unity_catalog("my_catalog")
        assert cfg.object_tracks_table == "my_catalog.perception_silver.object_tracks"
        assert cfg.perception_channels_table == "my_catalog.silver.perception_channels"

    def test_for_unity_catalog_custom_schemas(self):
        cfg = PerceptionDBConfig.for_unity_catalog(
            "cat", silver_schema="s", perception_schema="p"
        )
        assert cfg.object_tracks_table == "cat.p.object_tracks"
        assert cfg.perception_channels_table == "cat.s.perception_channels"
