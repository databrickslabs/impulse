"""Unit tests for mda_query_engine.perception.openlabel — OpenLABEL document builder.

These tests cover the dataset-agnostic builder. NuScenes- or A2D2-specific
wrapper behavior (e.g. annotator string conventions) is exercised by the
respective adapter integration tests.
"""

import json
import math
import uuid

import pytest

from mda_query_engine.perception.openlabel import (
    OPENLABEL_SCHEMA_VERSION,
    _default_sensor_kind,
    _stable_uuid,
    _yaw_to_quat_xyzw,
    build_openlabel_for_event,
    serialize,
)


# ── _yaw_to_quat_xyzw ────────────────────────────────────────────────────────


class TestYawToQuatXyzw:
    def test_zero_yaw_returns_identity(self):
        qx, qy, qz, qw = _yaw_to_quat_xyzw(0.0)
        assert (qx, qy, qz) == (0.0, 0.0, 0.0)
        assert qw == pytest.approx(1.0, abs=1e-12)

    def test_90deg_yaw_returns_z_axis_rotation(self):
        qx, qy, qz, qw = _yaw_to_quat_xyzw(math.pi / 2)
        assert qx == 0.0 and qy == 0.0
        assert qz == pytest.approx(math.sin(math.pi / 4), abs=1e-12)
        assert qw == pytest.approx(math.cos(math.pi / 4), abs=1e-12)

    def test_unit_norm(self):
        qx, qy, qz, qw = _yaw_to_quat_xyzw(1.234)
        norm = math.sqrt(qx * qx + qy * qy + qz * qz + qw * qw)
        assert norm == pytest.approx(1.0, abs=1e-12)


# ── _stable_uuid ─────────────────────────────────────────────────────────────


class TestStableUuid:
    def test_deterministic_across_calls(self):
        assert _stable_uuid("a", 1) == _stable_uuid("a", 1)

    def test_different_inputs_produce_different_uuids(self):
        assert _stable_uuid("a", 1) != _stable_uuid("a", 2)
        assert _stable_uuid("a", 1) != _stable_uuid("b", 1)

    def test_returns_valid_uuid_string(self):
        uuid.UUID(_stable_uuid("scene", 42))


# ── _default_sensor_kind ─────────────────────────────────────────────────────


class TestDefaultSensorKind:
    @pytest.mark.parametrize("channel,expected", [
        ("CAM_FRONT", "camera"),
        ("CAM_BACK_LEFT", "camera"),
        ("LIDAR_TOP", "lidar"),
        ("RADAR_FRONT_LEFT", "radar"),
        ("MISC", "other"),
        ("unknown_sensor", "other"),
    ])
    def test_prefix_classification(self, channel, expected):
        assert _default_sensor_kind(channel) == expected


# ── build_openlabel_for_event ────────────────────────────────────────────────


def _lidar_det(*, object_id=7, frame_ts=1000, cls="car",
               cx=1.0, cy=2.0, cz=0.0,
               length=4.0, width=1.7, height=1.5,
               yaw_rad=0.0, sensor_id="LIDAR_TOP",
               confidence=1.0, container_id=1) -> dict:
    return {
        "container_id": container_id,
        "frame_ts": frame_ts,
        "object_id": object_id,
        "detection_class": cls,
        "confidence": confidence,
        "sensor_id": sensor_id,
        "cx": cx, "cy": cy, "cz": cz,
        "length": length, "width": width, "height": height,
        "yaw_rad": yaw_rad,
    }


def _media(*, channel_name, timestamp=1000, file_path="/x.jpg",
           channel_id=100, container_id=1, fmt="jpg") -> dict:
    return {
        "container_id": container_id,
        "channel_id": channel_id,
        "channel_name": channel_name,
        "timestamp": timestamp,
        "file_path": file_path,
        "format": fmt,
    }


def _args(**overrides):
    base = dict(
        event_id="evt-1",
        event_name="hard_brake",
        container_id=42,
        scene_name="scene-0103",
        start_ts_us=1000,
        end_ts_us=2000,
        lidar_detections=[_lidar_det(object_id=7, frame_ts=1000)],
        media_paths=[_media(channel_name="CAM_FRONT", timestamp=1000, file_path="/uc/cam.jpg")],
        playlist_id="pl-1",
        playlist_version=1,
    )
    base.update(overrides)
    return base


class TestBuildOpenLabelForEvent:
    def test_metadata_uses_default_annotator_when_not_supplied(self):
        doc = build_openlabel_for_event(**_args())
        meta = doc["openlabel"]["metadata"]
        assert meta["schema_version"] == OPENLABEL_SCHEMA_VERSION
        assert meta["annotator"] == "ground_truth"
        assert "mda_query_engine/perception" in meta["exporter"]

    def test_annotator_parameter_is_passed_through(self):
        doc = build_openlabel_for_event(**_args(annotator="nuscenes_ground_truth"))
        assert doc["openlabel"]["metadata"]["annotator"] == "nuscenes_ground_truth"

    def test_exporter_parameter_is_passed_through(self):
        doc = build_openlabel_for_event(**_args(exporter="custom/exporter.py"))
        assert doc["openlabel"]["metadata"]["exporter"] == "custom/exporter.py"

    def test_metadata_name_includes_event(self):
        doc = build_openlabel_for_event(**_args())
        name = doc["openlabel"]["metadata"]["name"]
        assert "hard_brake" in name
        assert "evt-1" in name

    def test_objects_dict_keyed_by_stable_uuid(self):
        doc = build_openlabel_for_event(**_args())
        objs = doc["openlabel"]["objects"]
        assert len(objs) == 1
        u = next(iter(objs))
        assert u == _stable_uuid(42, 7)
        assert objs[u]["type"] == "car"

    def test_repeated_object_id_collapses_into_single_object(self):
        doc = build_openlabel_for_event(**_args(
            lidar_detections=[
                _lidar_det(object_id=7, frame_ts=1000),
                _lidar_det(object_id=7, frame_ts=1500, cx=2.0),
            ],
        ))
        assert len(doc["openlabel"]["objects"]) == 1
        assert len(doc["openlabel"]["frames"]) == 2

    def test_frames_keyed_by_zero_based_index(self):
        doc = build_openlabel_for_event(**_args(
            lidar_detections=[
                _lidar_det(object_id=7, frame_ts=1000),
                _lidar_det(object_id=8, frame_ts=1500),
            ],
            media_paths=[
                _media(channel_name="CAM_FRONT", timestamp=1000, file_path="/a.jpg"),
                _media(channel_name="CAM_FRONT", timestamp=1500, file_path="/b.jpg"),
            ],
        ))
        assert set(doc["openlabel"]["frames"].keys()) == {"0", "1"}

    def test_frame_intervals_span_all_frames(self):
        doc = build_openlabel_for_event(**_args(
            lidar_detections=[
                _lidar_det(object_id=7, frame_ts=1000),
                _lidar_det(object_id=7, frame_ts=1500),
                _lidar_det(object_id=7, frame_ts=2000),
            ],
            media_paths=[],
        ))
        assert doc["openlabel"]["frame_intervals"] == [{"frame_start": 0, "frame_end": 2}]

    def test_streams_use_default_prefix_heuristic_when_no_map_supplied(self):
        doc = build_openlabel_for_event(**_args(
            media_paths=[
                _media(channel_name="CAM_FRONT", timestamp=1000, file_path="/c.jpg"),
                _media(channel_name="LIDAR_TOP", timestamp=1000, file_path="/l.pcd"),
                _media(channel_name="RADAR_FRONT", timestamp=1000, file_path="/r.bin"),
                _media(channel_name="MISC", timestamp=1000, file_path="/m.dat"),
            ],
        ))
        streams = doc["openlabel"]["streams"]
        assert streams["CAM_FRONT"]["type"] == "camera"
        assert streams["LIDAR_TOP"]["type"] == "lidar"
        assert streams["RADAR_FRONT"]["type"] == "radar"
        assert streams["MISC"]["type"] == "other"

    def test_channel_kind_map_overrides_prefix_heuristic(self):
        # The caller knows MY_SENSOR is a thermal camera even though the name
        # doesn't start with CAM. The map wins.
        doc = build_openlabel_for_event(**_args(
            media_paths=[
                _media(channel_name="thermal_front", timestamp=1000, file_path="/t.tif"),
            ],
            channel_kind_map={"thermal_front": "camera"},
        ))
        assert doc["openlabel"]["streams"]["thermal_front"]["type"] == "camera"

    def test_sensor_kind_resolver_used_when_map_lacks_entry(self):
        doc = build_openlabel_for_event(**_args(
            media_paths=[
                _media(channel_name="weird_sensor_42", timestamp=1000, file_path="/w.dat"),
            ],
            sensor_kind_resolver=lambda name: "lidar" if "weird" in name else "other",
        ))
        assert doc["openlabel"]["streams"]["weird_sensor_42"]["type"] == "lidar"

    def test_channel_kind_map_takes_precedence_over_resolver(self):
        doc = build_openlabel_for_event(**_args(
            media_paths=[
                _media(channel_name="foo", timestamp=1000, file_path="/f.dat"),
            ],
            channel_kind_map={"foo": "radar"},
            sensor_kind_resolver=lambda name: "lidar",
        ))
        assert doc["openlabel"]["streams"]["foo"]["type"] == "radar"

    def test_stream_description_prefix_applied(self):
        doc = build_openlabel_for_event(**_args(
            media_paths=[_media(channel_name="CAM_FRONT", timestamp=1000, file_path="/a.jpg")],
            stream_description_prefix="NuScenes ",
        ))
        assert doc["openlabel"]["streams"]["CAM_FRONT"]["description"] == "NuScenes CAM_FRONT"

    def test_cuboid_payload_has_seven_pose_plus_three_size(self):
        doc = build_openlabel_for_event(**_args(
            lidar_detections=[_lidar_det(
                object_id=7, frame_ts=1000,
                cx=1.0, cy=2.0, cz=3.0,
                length=4.0, width=1.7, height=1.5,
                yaw_rad=0.0,
            )],
            media_paths=[],
        ))
        cuboid = doc["openlabel"]["frames"]["0"]["objects"]
        u = next(iter(cuboid))
        val = cuboid[u]["object_data"]["cuboid"][0]["val"]
        # (cx, cy, cz, qx, qy, qz, qw, length, width, height) — 10 values, yaw=0 → qw=1.
        assert len(val) == 10
        assert val[:3] == [1.0, 2.0, 3.0]
        assert val[3:6] == [0.0, 0.0, 0.0]
        assert val[6] == pytest.approx(1.0, abs=1e-12)
        assert val[7:] == [4.0, 1.7, 1.5]

    def test_empty_detections_still_produces_valid_skeleton(self):
        doc = build_openlabel_for_event(**_args(
            lidar_detections=[],
            media_paths=[],
        ))
        assert doc["openlabel"]["objects"] == {}
        assert doc["openlabel"]["frames"] == {}
        assert doc["openlabel"]["frame_intervals"] == [{"frame_start": 0, "frame_end": 0}]

    def test_coordinate_system_lists_streams_as_children(self):
        doc = build_openlabel_for_event(**_args(
            media_paths=[
                _media(channel_name="CAM_FRONT", timestamp=1000, file_path="/a.jpg"),
                _media(channel_name="LIDAR_TOP", timestamp=1000, file_path="/b.pcd"),
            ],
        ))
        cs = doc["openlabel"]["coordinate_systems"]["vehicle-iso8855"]
        assert set(cs["children"]) == {"CAM_FRONT", "LIDAR_TOP"}


class TestSerialize:
    def test_round_trips_through_json(self):
        doc = build_openlabel_for_event(**_args())
        parsed = json.loads(serialize(doc))
        assert parsed["openlabel"]["metadata"]["schema_version"] == OPENLABEL_SCHEMA_VERSION

    def test_uses_indent_for_readability(self):
        text = serialize(build_openlabel_for_event(**_args()))
        assert "\n  " in text
