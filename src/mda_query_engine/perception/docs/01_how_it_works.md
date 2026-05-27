# How LakeVision works

LakeVision is the perception extension for the Databricks Labs Impulse package. It adds the tables, schemas, and event abstractions that take you from raw sensor recordings to queryable scenarios, governed annotations and scenario definitions, and reproducible homologation evidence. This document explains what we built, why, and how each piece fits together.

The examples below reflect what the lakevision reference demo writes against a NuScenes scene today. The same data flow and roadmap apply to any dataset adapted into the platform — A2D2, PandaSet, customer MDF4 recordings, and so on.

## Contents

- [What ADAS and AV teams are dealing with](#what-adas-and-av-teams-are-dealing-with) — the three structural problems LakeVision exists to solve
- [Three common workflows](#three-common-workflows) — scenario mining, sensor KPI / hardware benchmarking, labelling and training-data export
- [How LakeVision is organized](#how-lakevision-is-organized) — the Impulse Core / LakeVision split and the Bronze / Silver / Gold layers
- [Walk-through of the data model](#walk-through-of-the-data-model) — every table that ships today, with example rows
- [Event detection](#event-detection) — overview and the event vocabulary table; full authoring walk-throughs live in `[03_authoring_events.md](03_authoring_events.md)`
- [What the BYOD demo proves today](#what-the-byod-demo-proves-today) — the four-notebook pipeline and the reference adapters
- [Getting started](#getting-started) — running the demo and writing your first adapter
- [What's coming](#whats-coming) — pointer to `[04_roadmap.md](04_roadmap.md)` for the full plan, sequenced by customer demand

Other docs in this directory: `[02_building_an_adapter.md](02_building_an_adapter.md)` for plugging your dataset in, `[03_authoring_events.md](03_authoring_events.md)` for writing events against your data, `[04_roadmap.md](04_roadmap.md)` for what's planned beyond the demo.

## What ADAS and AV teams are dealing with

Test fleets generate enormous volumes of synchronized sensor data: camera, LiDAR, radar, CAN bus, IMU, and GPS, multiplied by hours of recording per vehicle and hundreds to thousands of vehicles in a program. Across the perception-platform engagements we work on, three challenges come up again and again.

**1. Scenario search is hard to compose.** A typical engineering question reads like "find every recording where a cyclist appeared from the front-left blind spot while we were doing more than 30 kph." Answering this requires joining time-series scalars (vehicle speed) with per-object geometry (cyclist position and azimuth). Most teams end up writing one-off SQL or program-specific Python scripts each time the question changes.

**2. Per-object storage costs explode.** A naive "one row per object per frame" table at 10 Hz across a thousand recordings produces 1.6 billion rows. Liquid clustering helps, but only when the predicate hits an indexed column. Engineers learn to avoid the table, which defeats the purpose of building it.

**3. Lineage is fragile.** Regulatory homologation programs (UNECE R157, SOTIF ISO 21448, NCAP) require reproducing which exact data supported which decision. Six months after a model release, most teams cannot tell you whether a derived signal like `cyclist_present_front_left` meant the same thing then as it does today, or which scenarios were in the training set for model version 3.7.

LakeVision addresses all three. It ships as a Python package alongside Impulse core and shares the same conventions, the same Spark scaffolding, and the same Unity Catalog governance.

## Three common workflows

These three workflows ground the rest of the document. Each one touches a specific set of tables, and the architecture sections that follow are the proof that each workflow is supported end-to-end.

### Scenario mining at fleet scale

You want to find every recording where a specific situation occurred — a cyclist cut-in, a hard braking event near a pedestrian, a lane departure at speed.

You write the predicate as a `BasicEvent` over scalar channels. Impulse evaluates the predicate using TSAL and writes the matched event windows to `event_instance_fact`. You then save the matched windows to a named, versioned playlist in `playlist_items`. If you need per-object detail inside the windows, you join `object_tracks`.

**Tables touched:** `channels`, `channel_tags`, `event_instance_fact`, `playlist_items`, `object_tracks`.

### Sensor KPI and hardware benchmarking

You want to compare detection statistics across sensor configurations — for example, the detection range distribution for a roof-rack LiDAR rig versus a production in-car sensor set, on the same recordings.

The `object_tracks` table records the originating sensor per row in a `source` column (`lidar`, `radar`, `lidar|camera`, and so on). You filter on `source`, group by detection class and distance band, and plot. No external ground truth is required because the comparison is between hardware variants on the same recordings.

**Tables touched:** `object_tracks`, `event_instance_fact` (to scope to matched scenarios).

### Labelling and training-data export

You want to ship a labelled dataset to Voxel51 or Encord, or build a homologation evidence pack for a regulator.

You start from a `playlist_items` collection, which enumerates the relevant event windows. For each window, you join `perception_channels` to get the file paths to the camera frames and LiDAR scans, and you join `camera_object_detections` and `lidar_object_detections` to get the per-sensor geometry. The export job writes one OpenLABEL JSON document per event window into a Unity Catalog Volume.

**Tables touched:** `playlist_items`, `perception_channels`, `camera_object_detections`, `lidar_object_detections`, plus the OpenLABEL JSON outputs.

## How LakeVision is organized

Two design principles run through the whole package.

**TSAL finds the event window. Annotation tables explain what happened inside.** TSAL is the Time Series Algebra Library Impulse uses to evaluate predicates over scalar channels — for example, `(speed > 30) & (pedestrian_distance < 15)`. It is cheap to scan because the underlying `channels` table is run-length encoded. Annotation tables like `object_tracks` are more expensive to scan but contain the per-object detail. The cheap layer narrows fleet-scale data down to interesting windows; the expensive layer fills in the detail inside those windows.

**Pixels are never stored as Delta binary columns.** Raw video frames, LiDAR scans, and other binary media live in Unity Catalog Volumes. Delta tables hold the file-path index and the metadata you actually filter on. This matches how Voxel51, Encord, PyTorch DataLoader, and HuggingFace Datasets all expect to read frames — as file paths, not as bytes embedded in a database column.

The data model has three layers, split between Impulse Core (the generic time-series foundation) and LakeVision (the perception extensions on top). The boundary is principled: anything that operates on scalar signals or detected events lives in Impulse Core; anything that needs a file-path index because its data cannot live in Delta is a domain extension (LakeVision for perception, future packages for other domains).

**What we extended in Impulse Core for this work.** Several capabilities the ADAS use case needed are generic to any time-series workload, not just perception, so they land in Impulse Core rather than LakeVision:

- `channel_value_labels` — decoder table for ordinal channels (so `TrafficLight = Red` stays queryable as a label, not a magic integer).
- `playlist_items` — named, versioned scenario sets on top of `event_instance_fact`.
- `derived_channels` — planned governance layer for named, versioned signals derived from any event class. Lands in Impulse Core because labelling and lineage are universal, not perception-specific.
- `ChannelSource` registry on `MeasurementDB` — extensibility hook letting `BasicEvent` predicates resolve names against recorded, registered, and ephemeral signals uniformly.
- Source-agnostic `SequenceOfEvents` — planned refactor letting the existing temporal-sequence algorithm run against any registered source without adding a new event class.

Future domain extensions (industrial telemetry, medical monitoring, audio) inherit these Core capabilities and add their own file-path-indexed silver tables, just like LakeVision adds `perception_channels`.

**Bronze — raw recordings as files in Unity Catalog Volumes.** MDF4 logs, JPEG frames, PCD point clouds, annotation JSON. Bronze is files, not Delta tables.

**Impulse Core — Silver.** Governed, queryable tables:

- `container_tags`, `container_metrics` — recording-level metadata
- `channels`, `channel_tags`, `channel_metrics` — scalar time series and their metadata
- `channel_value_labels` — small decoder table that lets non-numeric concepts like `TrafficLight = Red` be stored as numeric codes in `channels` while staying queryable as labels
- `event_instance_fact` + `event_dimension` — solver output: one row per detected event window, with `event_dimension` carrying the human-readable event name joined on `event_id`. Raw, append-only, a moving target as new predicates and recordings land

**Impulse Core — Gold.** Named, versioned, curated outputs:

- `playlist_items` — immutable, versioned snapshots curated from `event_instance_fact` rows. The hand-off boundary to downstream tools (KPI rollups, OpenLABEL exports, training-data manifests, dashboards) — anything that needs a stable, citeable set of windows pulls a playlist, not raw `event_instance_fact`.

**LakeVision — Silver.** Perception extensions:

- `perception_channels` — file-path index mapping frames to source files
- `camera_object_detections`, `lidar_object_detections`, `radar_object_detections` — schemas designed based on how OpenLABEL stores detections per sensor modality (2D bbox for camera and thermal, 3D cuboid for LiDAR, polar returns for radar)
- `object_tracks` — fused per-object view over camera, LiDAR, and radar detections, used for scenario search across the fleet

**LakeVision — Gold.** Named, versioned, and exported artifacts:

- `frame_embeddings` — per-frame vectors with `model_version` tracking, source of truth for the Vector Search indexes
- OpenLABEL JSON exports — one per event window, written to Unity Catalog Volumes
- Vector Search indexes (Mosaic AI) — one per `embedding_type` value on `frame_embeddings`

## Walk-through of the data model

The rest of this section walks each table in the order data flows through the platform, with example rows that the BYOD demo writes today.

### `container_tags` — recording metadata

The unit of organization is a *container*, which is typically one continuous recording: a NuScenes scene, an MDF4 file, a thirty-minute drive. The `container_tags` table uses an entity-attribute-value pattern, so adapters can attach arbitrary metadata without a schema migration.

Example rows for a NuScenes scene:

```
container_id | key             | value
1            | recording_name  | scene-0061
1            | dataset_source  | nuscenes
1            | epoch_start_ns  | 1716221234000000000
1            | location        | boston-seaport
```

An OEM-shaped recording would add fields like vehicle VIN, software version, campaign identifier, and ODD (Operational Design Domain) tags as additional rows. None of this requires a schema change.

### `channels` — scalar time series

This is the Impulse core table. It stores any signal that reduces to a scalar `DOUBLE` over time: vehicle speed, steering angle, ego acceleration, traffic-light state, aggregated detection counts. Rows are run-length encoded, so a vehicle holding 34 km/h for 1.6 seconds is one row, not sixteen rows at 10 Hz.

```
container_id | channel_id | tstart  | tend    | value
1            | 10         | 1000000 | 2640000 | 34.0   -- Vehicle_Speed_kph
1            | 42         | 1000000 | 1100000 | 0.92   -- Pedestrian_Max_Confidence
1            | 43         | 1000000 | 1100000 | 3      -- Pedestrian_Count
1            | 44         | 1000000 | 1100000 | 8.5    -- Pedestrian_Nearest_Distance_m
```

Timestamps are microseconds, recording-relative. Channel names live in a separate metadata table (`channel_tags`, below), not as a column on `channels`. This keeps the rows narrow and Parquet-compressible.

### `channel_tags` — channel metadata

Every channel gets a `channel_name` tag. Detection channels also carry `detection_class` and `detection_aggregate` tags, which let downstream tooling resolve a query like "max confidence for pedestrians" against the metadata rather than by parsing channel name strings.

```
container_id | channel_id | key                 | value
1            | 10         | channel_name        | Vehicle_Speed_kph
1            | 42         | channel_name        | Pedestrian_Max_Confidence
1            | 42         | detection_class     | pedestrian
1            | 42         | detection_aggregate | max_confidence
```

Adding a new channel category — for example, `TwoWheeler_Count` for a customer that distinguishes scooters from bicycles — is two additional rows. No schema change required.

### `channel_value_labels` — decoding ordinal channels

Some scalar channels store ordinal codes rather than continuous measurements — traffic-light state, AEB or lane-departure warning state, gear position, speed-limit sign recognition. For example, `Traffic_Light_State` might hold 0 for unknown, 1 for red, 2 for yellow, and 3 for green. The mapping from numeric value to human-readable label lives in `channel_value_labels`:

```
channel_id | numeric_value | label
60         | 0             | unknown
60         | 1             | red
60         | 2             | yellow
60         | 3             | green
```

This table is currently a schema-only stub. None of the active adapters write ordinal channels yet — NuScenes, A2D2, and PandaSet do not include traffic-light state. The table gets populated as soon as the first adapter that needs it ships.

### `perception_channels` — file-path index for camera, LiDAR, thermal

This is the parallel Silver table for binary media. One row per frame per sensor, holding the Unity Catalog Volume path.

```
container_id | channel_id | timestamp | file_path                                       | format
1            | 20         | 1000000   | /Volumes/adas/raw/scene-0061/CAM_FRONT/000.jpg  | jpeg
1            | 20         | 1083333   | /Volumes/adas/raw/scene-0061/CAM_FRONT/001.jpg  | jpeg
1            | 22         | 1050000   | /Volumes/adas/raw/scene-0061/LIDAR_TOP/000.pcd  | pcd
1            | 25         | 1000000   | /Volumes/adas/raw/scene-0061/THERMAL/000.tif    | tiff_16bit
```

Timestamps align to the same recording-relative microsecond domain as `channels`, so scene-cutting from an event window to a set of frames is one join: `WHERE timestamp BETWEEN event.start_ts AND event.end_ts`. The `channel_id` maps to a row in `channel_tags` with `key=sensor_type`, which records whether the sensor is an RGB camera, a thermal imager, a LiDAR, and so on.

### `object_tracks` — fused per-object detections

This is where perception output lands once sensor fusion has run. The prevailing pattern is for fusion to run upstream — in the vehicle's Tier-1 ECU stack or in a separate perception service — and Databricks receives already-fused tracks. The alternative is fusion-on-Databricks, where a GPU job reads per-sensor detection tables and writes the fused output. Either pattern produces the same `object_tracks` schema.

The table is sensor-agnostic: one row per tracked object per frame, regardless of how many sensors observed it.

```
container_id | frame_ts | object_id | detection_class | distance_m | lane_offset | relative_velocity_ms | azimuth     | confidence | source
1            | 1000000  | 100       | pedestrian      | 8.5        | 0           | -0.3                 | front       | 0.94       | lidar|camera
1            | 1000000  | 101       | pedestrian      | 12.1       | +1          | -0.1                 | front_left  | 0.85       | lidar|camera
1            | 1000000  | 102       | cyclist         | 18.7       | -1          | -3.4                 | front_left  | 0.78       | lidar|radar|camera
1            | 1100000  | 100       | pedestrian      | 8.2        | 0           | -0.3                 | front       | 0.94       | lidar|camera
1            | 1100000  | 102       | cyclist         | 15.3       | -1          | -3.4                 | front_left  | 0.81       | lidar|radar|camera
```

The `azimuth` column is a sector enum (`front`, `front_left`, `left`, `rear_left`, and so on) rather than a continuous angle. Scenario queries naturally say "came from the left," not "was at 267.3 degrees." The `source` column records sensor provenance, which lets KPI workflows filter by which sensor saw the object. LiDAR distance is sub-centimeter accurate; monocular camera depth degrades past about 20 meters.

**Downsampling matters at fleet scale.** A full-resolution `object_tracks` table at a thousand recordings is 1.6 billion rows. `ObjectTracksConfig` provides two modes to keep this tractable:

- **Full-stride** (the default). Rows are written at a fixed rate for the entire recording. The minimum stride is 2 Hz, which is the Nyquist floor for sub-second ADAS events like a cyclist cut-in. At 2 Hz across a thousand recordings, this produces about 108 million rows. This is the discovery-friendly default — you can investigate any time point in the recording without first defining your scenarios.
- **TSAL-gated.** Rows are written only inside TSAL event windows, plus a configurable buffer (500 milliseconds before and after by default). At a typical 5 percent event coverage, this brings a thousand recordings down to about 81 million rows. Use this once you have an established playlist and want the storage saving.

The trade-off with TSAL-gated mode is that object continuity outside the buffer is lost. If you need to investigate what an object was doing five seconds before an event opened, you either need to extend the buffer or switch the recording to full-stride mode.

### `event_instance_fact` — detected event windows

This is the shared output table for every event class. `BasicEvent`, `ContainerEvent`, and `SequenceOfEvents` all write here. Downstream tooling — playlists, KPI dashboards, OpenLABEL exports — is event-class-agnostic because every event class shares this output.

```
container_id | event_id              | event_instance_id | start_ts | end_ts
1            | pedestrian_high_speed | evt_001           | 1100000  | 2640000
1            | pedestrian_high_speed | evt_002           | 4400000  | 5100000
1            | cyclist_left_cut_in   | evt_003           | 1800000  | 2200000
```

The `event_id` is stable across pipeline runs (it's a hash of the event definition), and `event_instance_id` identifies a specific occurrence. Human-readable event names live on a separate `event_dimension` table that joins on `event_id`.

### `playlist_items` — named, versioned scenario sets

A Delta Live Tables pipeline reads `event_instance_fact` and groups event windows into named collections. Each playlist is versioned, so a collection can evolve and historical versions stay queryable.

```
container_id | event_id              | start_ts | end_ts  | playlist_id  | playlist_version | created_at
1            | pedestrian_high_speed | 1100000  | 2640000 | hard_braking | 1                | 2026-05-20 14:30:00
1            | pedestrian_high_speed | 4400000  | 5100000 | hard_braking | 1                | 2026-05-20 14:30:00
1            | cyclist_left_cut_in   | 1800000  | 2200000 | cut_ins_v2   | 2                | 2026-05-22 09:15:00
```

**Why this is a separate table from `event_instance_fact`.** They look redundant at first glance — every row here references a window that already exists in `event_instance_fact`. The difference is between *raw output* and *curated, versioned artifact*. `event_instance_fact` is what the platform writes when it runs event detection. Every recording ingested adds rows; every new event definition adds rows. It is a moving target by design. `playlist_items` is the named, immutable snapshot you commit to. Pull `hard_braking` version 1 six months from now and you get the same windows, even though `event_instance_fact` has grown in between. A playlist can also combine multiple event types — cyclist cut-ins plus pedestrian-high-speed plus lane departures, all grouped as "high-risk near-misses" — where `event_instance_fact` groups by `event_id`, `playlist_items` groups by meaning. Playlists are the hand-off boundary too: they are what you share with labelling tools, training pipelines, and regulators, while `event_instance_fact` stays as internal pipeline state.

This is the today-available answer to the lineage challenge from the opening. The training set used for model version 3.7, the homologation evidence pack submitted to UNECE in Q1, the regression suite for the next firmware release — each is one `(playlist_id, playlist_version)` away. A regulator asking "which exact scenarios validated ADAS firmware 4.7.2?" cannot be answered with a SQL filter against a continuously growing fact table. They get a playlist id and version instead.

### `frame_embeddings` — per-frame vectors for similarity search

One row per downsampled camera frame. The demo runs at 5 Hz on the front camera only; production deployments raise the rate (8 to 10 Hz is typical) and add the surround cameras. This is a pipeline and compute decision, not a schema change.

The schema uses a single `embedding` column with an `embedding_type` discriminator. Image vectors (from CLIP or SigLIP) and text-description vectors (from BGE or a similar text embedding model) coexist on the same table, backed by two separate Mosaic AI Vector Search indexes.

```
container_id | channel_id | frame_ts | embedding_type | embedding              | model_version  | frame_description
1            | 20         | 1000000  | image          | [0.12, -0.34, 0.55, …] | clip-vit-l-14  | NULL
1            | 20         | 1000000  | text           | [0.41, 0.17, -0.22, …] | bge-large      | "pedestrian crossing in front of ego, daylight, urban intersection"
```

Scene-level similarity is computed at query time by aggregating the frame vectors within a window. We deliberately do not pre-aggregate at ingest, because that would tie the embedding pipeline to TSAL's event detection and rule out ad-hoc time windows.

### Per-sensor annotation tables — raw geometry for tool hand-off

Six additional tables hold the raw geometry that `object_tracks` deliberately drops. Each is keyed on `(container_id, frame_ts, object_id)` plus a `sensor_id`:


| Table                      | Holds                                                                        | Example columns                                      |
| -------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------- |
| `camera_object_detections` | 2D bounding boxes per camera (including thermal, distinguished by sensor_id) | x1, y1, x2, y2                                       |
| `lidar_object_detections`  | 3D cuboids in vehicle frame                                                  | cx, cy, cz, length, width, height, yaw_rad           |
| `radar_object_detections`  | Polar returns with Doppler                                                   | range_m, azimuth_rad, radial_velocity_ms             |
| `lane_markings`            | Lane polynomials per camera                                                  | c0, c1, c2, c3 (third-order polynomial coefficients) |
| `free_space`               | Drivable area polygon                                                        | boundary_pts: array of (x, y) structs                |
| `predicted_trajectories`   | Planner output per object                                                    | waypoints: array of (x, y, z, t) structs             |


The BYOD demo populates `camera_object_detections` and `lidar_object_detections` because notebook `03_per_event_detail.py` consumes them to build OpenLABEL exports. The other four are intentionally not populated by the demo. The schemas are in place, but ingest stays dormant until a specific consumer is scoped — a labelling tool, a training pipeline, a homologation evidence pack. Building these tables speculatively is expensive, and the schemas are tuned for hand-off rather than for general query.

## Event detection

Every event class writes into the same `event_instance_fact` silver table — `(container_id, event_id, event_instance_id, start_ts, end_ts)`. That table is the raw solver output: append-only, moving target, every window any predicate ever fired on. Downstream tooling — KPI rollups, OpenLABEL exports, OpenSCENARIO exports, training-data manifests, Lakeview dashboards — does not read `event_instance_fact` directly. It reads `playlist_items`, the gold layer of named, versioned, immutable curated sets built from `event_instance_fact` rows. The split is deliberate: solver output is what the platform produces; playlists are the committed hand-off boundary that V&V, labelling partners, and regulators consume. Composition happens at authoring time on the silver layer; consumption goes through playlists.

Today's surface is `BasicEvent` over scalar channels and `ContainerEvent` over recording metadata. `PerceptionEvent`, the helpers `perception_signal(...)` and `db.register_derived_channel(...)`, and the source-agnostic extension of `SequenceOfEvents` are planned (see "What's coming"). The walk-through below shows what each one looks like under the unified model.

### The event vocabulary


| Kind              | Construct                                      | Surface                                          | Semantics                                                                                                                                                        | Example use case                                                                                 |
| ----------------- | ---------------------------------------------- | ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Event class       | `BasicEvent`                                   | `channels`                                       | **Discrete match.** Boolean predicate over channel signals → one row per matched window in `event_instance_fact`                                                 | Speed > 100 kph AND steering-wheel angle > 30°.                                                  |
| Catalog op        | `db.register_derived_channel(event)`           | Any event class                                  | **Discrete → continuous.** Projects an event's match windows into the channels surface as a named, versioned signal; idempotent on identical `definition_hash`   | Publish `highway_traffic_jam` and `cyclist_present_left` as shared, governed signals.            |
| Surface extension | `derived_channels` (registered)                | Extends the channel surface for any `BasicEvent` | **Continuous signal.** The output of `db.register_derived_channel(...)`. Behaves like a recorded channel; referenced by name from any `BasicEvent` predicate     | Reference `cyclist_present_left` (a registered `PerceptionEvent`) inside any `BasicEvent`.       |
| Surface extension | `perception_signal(ot, predicate)` (ephemeral) | Extends the channel surface for any `BasicEvent` | **Continuous signal (ad-hoc).** Inline perception predicate computed at solve time; no registration, no lineage                                                  | Iterate on a candidate cyclist predicate inline before publishing it.                            |
| Event class       | `ContainerEvent`                               | `container_tags`                                 | **Discrete match.** Attribute match → one row per matching container in `event_instance_fact`                                                                    | Highway-daylight recordings from the Frankfurt fleet.                                            |
| Event class       | `PerceptionEvent`                              | `object_tracks`                                  | **Discrete match.** Row-level predicate over `object_tracks` (frame matches when at least one row matches) → one row per matched window in `event_instance_fact` | Cyclist on front-left within 8 m at LiDAR confidence > 0.7.                                      |
| Event class       | `PerceptionEvent` (with `track_scope=True`)    | `object_tracks`                                  | **Discrete match.** Same predicate; windowing per `(container_id, object_id)` with optional `min_duration_ms` debounce                                           | Cut-in — a vehicle whose `lane_offset` went from -1 to 0 and held the new lane for at least 1 s. |
| Event class       | `SequenceOfEvents`                             | Expressions over any registered source           | **Discrete match.** Ordered temporal pattern → one row per matching sequence in `event_instance_fact`                                                            | AEB followed within 2 s by lead-vehicle distance < 5 m.                                          |


**Events produce discrete matches; derived channels are continuous signals.** Events write one row per match into `event_instance_fact`. Derived channels project those matches into the channels surface as named, versioned signals that other events can reference by name. `db.register_derived_channel(event)` is the bridge — it promotes an event from discrete form to continuous form so the result composes into more predicates. `BasicEvent` is the compose-anything event class: its predicate accepts any windowed signal, recorded or derived. `PerceptionEvent` and `ContainerEvent` are atomic event classes scoped to one surface each; `SequenceOfEvents` adds the temporal axis on top of any of them.

For the full authoring walk-through — how to write each event class against your data, compound-predicate production / exploration patterns, temporal sequences, layered composition (events of events), and version pinning for regulatory reproducibility — see `[03_authoring_events.md](03_authoring_events.md)`.

## What the BYOD demo proves today

The reference implementation is `demos/byod/`. Every dataset plugs in as an adapter under `demos/byod/adapters/<name>/`. The four pipeline notebooks are byte-identical across adapters, and all per-dataset logic lives inside the adapter package. Adding a new dataset is one Python package — zero notebook edits.

The current adapters are NuScenes (the most complete reference), A2D2 (closer to real OEM signal naming), PandaSet (multi-LiDAR sensor configurations), and a synthetic MDF4 sample (for testing the binary log format path).

The notebooks are:


| Notebook                 | What it does                                                                                                                        |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| `00_download.py`         | Calls the adapter's `download()` function to fetch or validate source data                                                          |
| `01_ingest.py`           | Writes `container_tags`, `channel_tags`, the RLE-encoded `channels`, `perception_channels`, and `object_tracks`                     |
| `02_detect_events.py`    | Runs the `BasicEvent` predicates and writes detected windows to `event_instance_fact`, plus a versioned `playlist_items` collection |
| `03_per_event_detail.py` | For each event window, populates `lidar_object_detections` and `camera_object_detections`, then writes an OpenLABEL JSON document   |
| `04_visualize.py`        | Multi-sensor visualization per event window, with adapter-pluggable readers                                                         |


Running the demo end-to-end proves five things:

1. The lakehouse handles multi-modal ADAS data through a single ingest path.
2. TSAL produces stable event windows over scalar signals.
3. `object_tracks` supports scenario search without per-sensor SQL joins.
4. OpenLABEL exports connect to labelling tools without re-extracting frames.
5. The `source` column in `object_tracks` enables KPI workflows that compare hardware configurations without requiring external ground truth.

What's deliberately not in the demo today: frame embeddings and Vector Search (these require a GPU pipeline for the embedding model), text-based search (the description-quality validation step is the prerequisite), and on-demand frame extraction for the scene-cut workflow.

## Getting started

The fastest way to see the platform working is to run the BYOD demo against a public dataset.

1. Clone the impulse repository and install the `demos/byod` package.
2. Pick an adapter. NuScenes is the most complete reference. A2D2 is closer to real OEM signal naming. PandaSet covers multi-LiDAR sensor configurations.
3. Run notebooks `00` through `04` in order. Expect about thirty minutes from `git clone` to the first detected event window, once the source data is on disk.

To bring your own data, write a single adapter package under `demos/byod/adapters/<your-name>/`. The adapter protocol is documented in `demos/byod/README.md`, and the four existing adapters are reference implementations covering different ingest shapes. Once the adapter ingests, every notebook downstream of ingest works against your data without modification.

For the package-level layout and the module surface (`PerceptionDB`, `ObjectTracksConfig`, `geometry`, `openlabel`), see `src/mda_query_engine/perception/README.md`.

For lineage and reproducibility, the today-available record is `playlist_items` (versioned scenario sets) plus `frame_embeddings.model_version` (per-embedding reproducibility).

## What's coming

The demo is intentionally the minimum that proves the architecture works. Everything planned beyond it — `PerceptionEvent`, `derived_channels`, the source-agnostic `SequenceOfEvents` refactor, frame-summary materialisation, auto-rewrite to TSAL, scene-cut-on-demand frame extraction — extends the current shape without breaking what's already there. The full plan, sequencing rationale, and forward guarantees live in `[04_roadmap.md](04_roadmap.md)`.