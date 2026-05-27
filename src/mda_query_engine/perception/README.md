# LakeVision — Perception Extension for Impulse

LakeVision adds perception-data primitives on top of Impulse core (TSAL + signal ingest),
giving programs working with multi-modal sensor data (LiDAR, camera, radar, annotations)
a complete governed platform.

> **1. How it works:** `[docs/01_how_it_works.md](docs/01_how_it_works.md)` — end-to-end walkthrough of every LakeVision table with example data, the problems each one solves, and the supported event vocabulary. Start here if you're new to the package or evaluating it for a customer engagement.
>
> **2. Building a BYOD adapter:** `[docs/02_building_an_adapter.md](docs/02_building_an_adapter.md)` — what it takes to bring your own data into the platform, which of the four reference adapters (NuScenes, A2D2, PandaSet, mdf4_sample) to copy as a baseline for your scenario, and the common challenges to expect (coordinate frames, timestamp alignment, channel ID allocation).
>
> **3. Authoring events for your data:** `[docs/03_authoring_events.md](docs/03_authoring_events.md)` — how to write `BasicEvent`, `ContainerEvent`, `SequenceOfEvents`, and `PerceptionEvent` predicates against your data. Production patterns (registered derived channels) and exploration patterns (`perception_signal(...)`), layered composition (events of events), and version pinning for regulatory reproducibility.
>
> **4. Roadmap:** `[docs/04_roadmap.md](docs/04_roadmap.md)` — what is planned beyond the shipped demo, in what order, and why. Covers the `PerceptionEvent` and `derived_channels` centerpiece, solver routing, the labelling-infrastructure motivation, and the full sequence of planned capabilities.

## Python package

LakeVision ships as `src/mda_query_engine/perception/` — a Python package following the same conventions
as Impulse core (`mda_query_engine`, `mda_reporting`).

```python
from lakevision import PerceptionDB, PerceptionDBConfig

cfg = PerceptionDBConfig.for_unity_catalog("my_catalog")
db  = PerceptionDB(cfg)

object_tracks    = db.object_tracks(spark)
frame_embeddings = db.frame_embeddings(spark)
```

## Modules


| Module                 | Provides                                                                                                                                                           |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `geometry`             | 3D quaternion / rotation helpers, box-corner generation, point-to-image projection                                                                                 |
| `windowing`            | Event-window filtering shared across adapters                                                                                                                      |
| `openlabel`            | OpenLABEL exchange-format builders                                                                                                                                 |
| `perception_db`        | `PerceptionDB` accessor for perception tables (`object_tracks`, `frame_embeddings`, `perception_channels`, `playlist_items`, and the per-sensor annotation tables) |
| `object_tracks_config` | Per-adapter `ObjectTracksConfig` schemas                                                                                                                           |
| `playlists`            | `event_fact_to_playlist_items` helper for building named, versioned playlists from `event_instance_fact` rows                                                      |
| `scalar_metrics`       | `derive_channel_metrics_from_channels` helper for computing per-channel scalar metrics                                                                             |
| `schema/`              | Spark `StructType` definitions for the silver tables and the per-sensor annotation tables                                                                          |


## Design principles

- **TSAL finds the event. Annotation tables explain it.**
Do not put geometric annotations into `channels`; do not run scenario search over annotation tables.
- **Pixel values are never stored in Delta binary columns.**
Raw video/frames land in UC Volumes; `perception_channels` holds file-path pointers.
- **Ingest at native fidelity; downsample at materialisation time per workload.**
- **OpenLABEL and OpenSCENARIO are exchange formats, not storage formats.**
- **Build annotation tables only when a downstream consumer is scoped.**

## Relationship to Impulse core

```
Impulse core (src/mda_query_engine, src/mda_reporting)
  └── provides: channels, channel_tags, container_tags, TSAL, event_instance_fact

LakeVision (src/mda_query_engine/perception/)
  └── requires: Impulse core (container_id, TSAL events)
  └── adds: perception_channels, object_tracks, frame_embeddings, playlist_items,
             annotation tables (camera_object_detections, lidar_object_detections,
             radar_object_detections, lane_markings, free_space, predicted_trajectories)
```

`PerceptionDB` sits alongside `MeasurementDB` — it does not replace it.