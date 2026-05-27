"""Fixtures for LakeVision pipeline integration tests.

Each demo gets its own fixture that returns a populated temp catalog/schema
with synthetic Bronze data in place — small enough to run fast, structured
enough to flow through the demo's library functions and SQL transforms.
"""

from __future__ import annotations

import sys
import types as _types
import uuid
from dataclasses import asdict
from pathlib import Path

import pytest

# Add the BYOD demo root so adapter/lib packages are importable without
# nuscenes-devkit installed. The FakeLoader below replaces NuScenesLoader —
# the devkit is never instantiated in this local test tier.
_DEMO_ROOT = Path(__file__).resolve().parents[3] / "demos" / "byod"
if str(_DEMO_ROOT) not in sys.path:
    sys.path.insert(0, str(_DEMO_ROOT))

from adapters.nuscenes.loader import (  # noqa: E402
    SENSOR_CHANNEL_IDS,
    Annotation,
    EgoPose,
    Sample,
    SampleData,
    Scene,
    stable_int_id,
)


# ── Fake loader ──────────────────────────────────────────────────────────────


class _FakeNuScenesLoader:
    """Minimal loader backed by pre-built dataclasses — no devkit or filesystem required."""

    def __init__(
        self,
        scenes_data: list[Scene],
        samples_by_scene: dict[str, list[Sample]],
        ego_by_sample_token: dict[str, EgoPose],
        anns_by_sample_token: dict[str, list[Annotation]],
        sample_data_by_sample_token: dict[str, list[SampleData]],
        volume_root: str,
    ) -> None:
        self._scenes = scenes_data
        self._samples_by_scene = samples_by_scene
        self._ego = ego_by_sample_token
        self._anns = anns_by_sample_token
        self._sample_data = sample_data_by_sample_token
        self._volume_root = volume_root

    @property
    def dataroot(self) -> str:
        return self._volume_root

    @property
    def dataset_version(self) -> str:
        return "v1.0-mini"

    def scenes(self):
        return iter(list(self._scenes))

    def list_scenes(self) -> list[Scene]:
        return list(self._scenes)

    def samples_in_scene(self, scene: Scene):
        return iter(list(self._samples_by_scene.get(scene.scene_token, [])))

    def ego_pose_for_sample(self, sample: Sample) -> EgoPose:
        return self._ego[sample.sample_token]

    def annotations_in_sample(self, sample: Sample):
        return iter(list(self._anns.get(sample.sample_token, [])))

    def sample_data_for_sample(self, sample: Sample):
        return iter(list(self._sample_data.get(sample.sample_token, [])))

    def all_sample_data_in_scene(self, scene: Scene):
        for sample in self.samples_in_scene(scene):
            yield from self.sample_data_for_sample(sample)


def _build_fake_loader(volume_root: str) -> _FakeNuScenesLoader:
    """Build a FakeLoader: 2 scenes × 3 samples × 2 annotations (car + pedestrian).

    Ego moves forward at 5 m/s on the x-axis. Car is 10 m ahead; pedestrian
    is 5 m ahead and 3.5 m to the left. Both are ground truth (confidence=1.0).
    """
    T0 = 1_600_000_000_000_000  # microseconds (2020-09-13)
    DT = 500_000  # 500 ms → 2 Hz keyframe rate

    SCENE_TOKENS = ["a" * 32, "b" * 32]
    LOG_TOKEN = "c" * 32

    # 32-char hex sample tokens (valid for stable_int_id, first-16 non-zero)
    SAMPLE_TOKENS: dict[str, list[str]] = {
        "a" * 32: ["0001" + "0" * 28, "0002" + "0" * 28, "0003" + "0" * 28],
        "b" * 32: ["0101" + "0" * 28, "0102" + "0" * 28, "0103" + "0" * 28],
    }
    INSTANCE_TOKENS: dict[str, dict[str, str]] = {
        "a" * 32: {"car": "cafe" + "0" * 28, "ped": "beef" + "0" * 28},
        "b" * 32: {"car": "babe" + "0" * 28, "ped": "face" + "0" * 28},
    }

    all_scenes: list[Scene] = []
    samples_by_scene: dict[str, list[Sample]] = {}
    ego_by_sample: dict[str, EgoPose] = {}
    anns_by_sample: dict[str, list[Annotation]] = {}
    sample_data_by_sample: dict[str, list[SampleData]] = {}

    for si, scene_token in enumerate(SCENE_TOKENS):
        container_id = stable_int_id(scene_token)
        T_scene = T0 + si * 5_000_000  # scenes are 5 s apart
        sample_tokens = SAMPLE_TOKENS[scene_token]
        inst = INSTANCE_TOKENS[scene_token]

        scene = Scene(
            container_id=container_id,
            scene_token=scene_token,
            name=f"scene-{si:03d}",
            description=f"synthetic scene {si}",
            log_token=LOG_TOKEN,
            nbr_samples=3,
            first_sample_token=sample_tokens[0],
            last_sample_token=sample_tokens[2],
        )
        all_scenes.append(scene)

        samples: list[Sample] = []
        for j, stok in enumerate(sample_tokens):
            ts = T_scene + j * DT
            x_ego = float(j) * 2.5 + si * 1000.0  # scenes offset by 1 km in global x

            samples.append(Sample(
                container_id=container_id,
                sample_token=stok,
                timestamp_us=ts,
                scene_token=scene_token,
                sensor_data_tokens={
                    "CAM_FRONT": f"cd_cam_{si}_{j:02d}",
                    "LIDAR_TOP": f"cd_lid_{si}_{j:02d}",
                },
            ))

            ego_by_sample[stok] = EgoPose(
                container_id=container_id,
                timestamp_us=ts,
                translation=(x_ego, 0.0, 0.5),
                rotation=(1.0, 0.0, 0.0, 0.0),  # identity — ego faces +x
            )

            anns_by_sample[stok] = [
                Annotation(
                    container_id=container_id,
                    sample_token=stok,
                    timestamp_us=ts,
                    instance_token=inst["car"],
                    object_id=stable_int_id(inst["car"]),
                    category_name="vehicle.car",
                    detection_class="car",
                    translation=(x_ego + 10.0, 0.0, 0.5),  # 10 m ahead in global frame
                    size=(1.7, 4.0, 1.5),
                    rotation=(1.0, 0.0, 0.0, 0.0),
                    num_lidar_pts=15,
                    num_radar_pts=3,
                    visibility_token="4",
                ),
                Annotation(
                    container_id=container_id,
                    sample_token=stok,
                    timestamp_us=ts,
                    instance_token=inst["ped"],
                    object_id=stable_int_id(inst["ped"]),
                    category_name="human.pedestrian.adult",
                    detection_class="pedestrian",
                    translation=(x_ego + 5.0, 3.5, 0.9),  # 5 m ahead, 3.5 m left
                    size=(0.7, 0.7, 1.8),
                    rotation=(1.0, 0.0, 0.0, 0.0),
                    num_lidar_pts=8,
                    num_radar_pts=0,
                    visibility_token="3",
                ),
            ]

            sample_data_by_sample[stok] = [
                SampleData(
                    container_id=container_id,
                    sample_token=stok,
                    sensor_name="CAM_FRONT",
                    channel_id=SENSOR_CHANNEL_IDS["CAM_FRONT"],
                    timestamp_us=ts,
                    file_path=f"{volume_root}/samples/CAM_FRONT/{stok[:8]}.jpg",
                    fileformat="jpg",
                    width=1600,
                    height=900,
                ),
                SampleData(
                    container_id=container_id,
                    sample_token=stok,
                    sensor_name="LIDAR_TOP",
                    channel_id=SENSOR_CHANNEL_IDS["LIDAR_TOP"],
                    timestamp_us=ts,
                    file_path=f"{volume_root}/samples/LIDAR_TOP/{stok[:8]}.pcd.bin",
                    fileformat="pcd.bin",
                    width=0,
                    height=0,
                ),
            ]

        samples_by_scene[scene_token] = samples

    return _FakeNuScenesLoader(
        scenes_data=all_scenes,
        samples_by_scene=samples_by_scene,
        ego_by_sample_token=ego_by_sample,
        anns_by_sample_token=anns_by_sample,
        sample_data_by_sample_token=sample_data_by_sample,
        volume_root=volume_root,
    )


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def synthetic_nuscenes_bronze(spark):
    """Populate a temp schema with synthetic nuScenes-shaped Bronze tables.

    Returns:
        {
            "catalog": str,        # spark_catalog
            "schema": str,         # synth_nuscenes_<run>
            "scenes": list[str],   # synthetic scene tokens
            "volume_root": str,    # fake UC Volume path prefix
        }
    """
    import pyspark.sql.functions as F
    from pyspark.sql import Row

    import mda_query_engine.schema as core_schema
    from adapters.nuscenes import ingest as _ingest
    from adapters.nuscenes.object_tracks import map_scene_to_object_tracks
    from adapters.nuscenes.scalar_source import derive_for_scene
    from mda_query_engine.perception.schema.scenario import OBJECT_TRACKS
    from mda_query_engine.perception.schema.silver import PERCEPTION_CHANNELS

    run_id = uuid.uuid4().hex[:8]
    catalog = "spark_catalog"
    schema_name = f"synth_nuscenes_{run_id}"
    volume_root = f"/Volumes/{catalog}/{schema_name}_silver/raw/v1.0-mini"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")

    loader = _build_fake_loader(volume_root)

    cfg = _types.SimpleNamespace(
        t_container_tags=f"{catalog}.{schema_name}.container_tags",
        t_container_metrics=f"{catalog}.{schema_name}.container_metrics",
        t_channel_tags=f"{catalog}.{schema_name}.channel_tags",
        dataset_version="v1.0-mini",
    )

    # Step 2 — metadata tables (container_tags, container_metrics, channel_tags)
    _ingest.ingest_metadata(spark, loader, cfg)

    # Step 3 — channels (scalar_source derivation)
    channel_rows = [
        Row(**asdict(cv))
        for scene in loader.list_scenes()
        for cv in derive_for_scene(loader, scene)
    ]
    channels_df = spark.createDataFrame(channel_rows, core_schema.CHANNELS_SCHEMA)
    channels_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema_name}.channels"
    )

    # channel_metrics — one row per distinct (container_id, channel_id); mirrors
    # what the Impulse pipeline produces so the downstream invariant is testable.
    channel_metrics_df = channels_df.groupBy("container_id", "channel_id").agg(
        F.count("*").cast("int").alias("sample_count"),
        F.lit(None).cast("string").alias("value_type"),
        F.lit(0.0).cast("float").alias("nan_ratio"),
        (F.min("tstart") / 1e6).cast("float").alias("begin_s"),
        (F.max("tend") / 1e6).cast("float").alias("end_s"),
        ((F.max("tend") - F.min("tstart")) / 1000).cast("int").alias("duration_ms"),
        F.count("*").cast("int").alias("original_sample_count"),
        F.lit(2.0).cast("float").alias("original_sr"),
        F.min("value").cast("float").alias("min"),
        F.max("value").cast("float").alias("max"),
        F.avg("value").cast("float").alias("mean"),
        F.stddev("value").cast("float").alias("std"),
        F.lit(None).cast("float").alias("pz1"),
        F.lit(None).cast("float").alias("pz10"),
        F.lit(None).cast("float").alias("pz90"),
        F.lit(None).cast("float").alias("pz99"),
    )
    channel_metrics_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema_name}.channel_metrics"
    )

    # Step 4 — perception_channels
    perception_rows = list(_ingest.perception_paths(loader))
    perception_df = spark.createDataFrame(perception_rows, PERCEPTION_CHANNELS)
    perception_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema_name}.perception_channels"
    )

    # Step 5 — object_tracks
    track_rows = [
        row
        for scene in loader.list_scenes()
        for row in map_scene_to_object_tracks(loader, scene)
    ]
    tracks_df = spark.createDataFrame(track_rows, OBJECT_TRACKS)
    tracks_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema_name}.object_tracks"
    )

    yield {
        "catalog": catalog,
        "schema": schema_name,
        "scenes": [s.scene_token for s in loader.list_scenes()],
        "volume_root": volume_root,
    }

    spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema_name} CASCADE")


@pytest.fixture
def synthetic_a2d2_bronze(spark, tmp_path):
    """Populate a temp schema with synthetic A2D2-shaped Bronze tables.

    Returns the same dict shape as `synthetic_nuscenes_bronze` so tests can
    parametrise over both configs and assert config-agnostic pipeline behaviour
    (the central guarantee of the BYOD demo).

    Fill in when authoring tests/mda_query_engine/perception/integration/test_a2d2_pipeline.py.
    """
    pytest.skip(
        "synthetic_a2d2_bronze fixture not yet implemented — "
        "add when test_a2d2_pipeline.py is authored"
    )
