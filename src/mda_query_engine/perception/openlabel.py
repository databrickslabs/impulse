"""OpenLABEL 1.0.0 JSON export from LakeVision Delta tables.

OpenLABEL is an export format, not a storage format. Annotations live in
`lidar_object_detections`, `camera_object_detections`, and friends; this module
projects them into the nested OpenLABEL document shape that labeling tools
(Voxel51, Encord, …) expect.

One package per TSAL event window. Each package contains:
  - Metadata          (schema version, annotator, exporter)
  - Coordinate systems (vehicle-iso8855 — the LakeVision vehicle frame)
  - Streams           (per-sensor channel definitions for the window)
  - Objects           (UUID-keyed dict of static per-object info)
  - Frames            (timestamp-keyed dict of per-frame object_data: cuboids, file URIs)

The Delta tables remain the queryable source of truth; OpenLABEL files are
derived, regenerable artifacts.
"""

from __future__ import annotations

import json
import math
import uuid
from typing import Any, Callable


OPENLABEL_SCHEMA_VERSION = "1.0.0"

# OpenLABEL recognises `camera` and `lidar` as first-class sensor types.
# `radar` and `other` are accepted as custom values; labeling tools ignore
# what they don't understand. Thermal also maps to `camera` (same intrinsics).
_OPENLABEL_SENSOR_TYPES = {"camera", "lidar", "radar", "other"}


def _yaw_to_quat_xyzw(yaw_rad: float) -> tuple[float, float, float, float]:
    """Yaw-only rotation around z-axis → quaternion in OpenLABEL order (qx, qy, qz, qw)."""
    half = yaw_rad / 2.0
    return (0.0, 0.0, math.sin(half), math.cos(half))


def _stable_uuid(*parts: Any) -> str:
    """Deterministic UUID derived from input parts. Same inputs → same UUID across runs."""
    name = "|".join(str(p) for p in parts)
    return str(uuid.uuid5(uuid.NAMESPACE_OID, name))


def _default_sensor_kind(channel_name: str) -> str:
    """Fallback resolver: classify by channel-name prefix.

    Used when no `channel_kind_map` or `sensor_kind_resolver` is supplied by the
    caller. Adapter code that has `channel_tags.sensor_type` available should
    supply that mapping instead so the export reflects governance metadata.
    """
    if channel_name.startswith("CAM"):
        return "camera"
    if channel_name.startswith("LIDAR"):
        return "lidar"
    if channel_name.startswith("RADAR"):
        return "radar"
    return "other"


def build_openlabel_for_event(
    *,
    event_id: str,
    event_name: str,
    container_id: int,
    scene_name: str,
    start_ts_us: int,
    end_ts_us: int,
    lidar_detections: list[dict],
    media_paths: list[dict],
    playlist_id: str,
    playlist_version: int,
    annotator: str = "ground_truth",
    exporter: str = "mda_query_engine/perception/openlabel.py",
    stream_description_prefix: str = "",
    channel_kind_map: dict[str, str] | None = None,
    sensor_kind_resolver: Callable[[str], str] | None = None,
) -> dict:
    """Compose one OpenLABEL document for a single event window.

    Inputs:
      `lidar_detections` — rows from `lidar_object_detections` whose
                           container_id + frame_ts fall inside
                           [start_ts_us, end_ts_us]. Each row dict has
                           {container_id, frame_ts, object_id, detection_class,
                            confidence, sensor_id, cx, cy, cz, length, width,
                            height, yaw_rad}.

      `media_paths`      — rows from `perception_channels` for the same window,
                           with `channel_name` joined in by the caller from
                           `channel_tags`. Each row dict has {container_id,
                           channel_id, channel_name, timestamp, file_path, format}.

      `annotator`        — value written to `metadata.annotator`. Caller passes
                           a string identifying the labeling source (e.g.
                           ``"<dataset>_ground_truth"`` or ``"voxel51_human_v3"``).

      `exporter`         — value written to `metadata.exporter`.

      `stream_description_prefix` — prepended to the per-stream description.
                           Used to namespace streams by source (e.g. a
                           ``"<dataset> "`` prefix yields ``"<dataset> CAM_FRONT"``).

      `channel_kind_map` — optional dict mapping channel_name → OpenLABEL sensor
                           type (one of "camera", "lidar", "radar", "other").
                           Caller builds this from `channel_tags.sensor_type`
                           so the export reflects governance metadata rather
                           than channel-name string heuristics.

      `sensor_kind_resolver` — optional callable taking a channel_name and
                           returning the OpenLABEL sensor type. Used as a
                           fallback when `channel_kind_map` does not contain
                           the channel_name. If neither is supplied, the
                           default prefix heuristic (``CAM``/``LIDAR``/``RADAR``)
                           is applied.
    """
    resolver = sensor_kind_resolver or _default_sensor_kind

    def resolve_kind(channel_name: str) -> str:
        if channel_kind_map is not None and channel_name in channel_kind_map:
            return channel_kind_map[channel_name]
        return resolver(channel_name)

    # Stable object UUID per (container_id, object_id) so the same object across
    # frames maps to one OpenLABEL object.
    object_uuid: dict[int, str] = {}
    objects: dict[str, dict] = {}
    for det in lidar_detections:
        oid = int(det["object_id"])
        if oid in object_uuid:
            continue
        u = _stable_uuid(container_id, oid)
        object_uuid[oid] = u
        objects[u] = {
            "name": f"obj_{oid:020d}",
            "type": det["detection_class"],
        }

    # Streams: one entry per unique sensor in media_paths.
    streams: dict[str, dict] = {}
    for m in media_paths:
        chan = m["channel_name"]
        if chan in streams:
            continue
        streams[chan] = {
            "type": resolve_kind(chan),
            "description": f"{stream_description_prefix}{chan}",
        }

    # Frames: keyed by frame number (integer index into the sorted list of
    # unique frame timestamps).
    all_timestamps = sorted(
        {int(d["frame_ts"]) for d in lidar_detections}
        | {int(m["timestamp"]) for m in media_paths}
    )
    frame_index_by_ts = {ts: i for i, ts in enumerate(all_timestamps)}

    frames: dict[str, dict] = {}
    for ts in all_timestamps:
        fid = str(frame_index_by_ts[ts])
        per_stream_uris = {
            m["channel_name"]: {"uri": m["file_path"]}
            for m in media_paths
            if int(m["timestamp"]) == ts
        }
        frames[fid] = {
            "frame_properties": {
                "timestamp": str(ts),
                "streams": per_stream_uris,
            },
            "objects": {},
        }

    # Populate per-frame object cuboids.
    for det in lidar_detections:
        ts = int(det["frame_ts"])
        fid = str(frame_index_by_ts[ts])
        oid = int(det["object_id"])
        u = object_uuid[oid]
        qx, qy, qz, qw = _yaw_to_quat_xyzw(float(det["yaw_rad"]))
        frames[fid]["objects"][u] = {
            "object_data": {
                "cuboid": [
                    {
                        "name": "shape",
                        "val": [
                            float(det["cx"]), float(det["cy"]), float(det["cz"]),
                            qx, qy, qz, qw,
                            float(det["length"]), float(det["width"]), float(det["height"]),
                        ],
                        "coordinate_system": "vehicle-iso8855",
                    }
                ],
                "num": [
                    {"name": "confidence", "val": float(det["confidence"])},
                ],
                "text": [
                    {"name": "sensor_id", "val": str(det["sensor_id"])},
                ],
            }
        }

    return {
        "openlabel": {
            "metadata": {
                "schema_version": OPENLABEL_SCHEMA_VERSION,
                "name": f"{event_name}__{event_id}",
                "annotator": annotator,
                "exporter": exporter,
                "comment": (
                    f"Exported from LakeVision lidar_object_detections for event {event_id} "
                    f"(container {container_id}, scene {scene_name}, "
                    f"window [{start_ts_us}, {end_ts_us}], playlist {playlist_id} v{playlist_version})."
                ),
            },
            "coordinate_systems": {
                "vehicle-iso8855": {
                    "type": "local",
                    "parent": "",
                    "pose_wrt_parent": {},
                    "children": list(streams.keys()),
                }
            },
            "streams": streams,
            "objects": objects,
            "frames": frames,
            "frame_intervals": [{"frame_start": 0, "frame_end": max(0, len(all_timestamps) - 1)}],
        }
    }


def serialize(openlabel_doc: dict) -> str:
    return json.dumps(openlabel_doc, indent=2, sort_keys=False)
