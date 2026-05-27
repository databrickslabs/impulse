# Building a BYOD adapter

If you have your own data — MDF4 logs from an OEM fleet, a public research dataset, output from a specific perception pipeline — you bring it into the platform by writing an adapter. The adapter is one Python package under `demos/byod/adapters/<your-name>/`. Once it ingests, every downstream notebook (TSAL event detection, OpenLABEL export, sensor KPI, visualization) works against your data without modification.

This document walks the adapter contract, the four reference adapters that ship today, and the common challenges to expect when writing your own.

> Companion doc: [`01_how_it_works.md`](01_how_it_works.md) covers the canonical schema (`channels`, `channel_tags`, `perception_channels`, `object_tracks`, and the rest) that your adapter writes into. Read that first if you have not already.

## What you're committing to

An adapter is a translator between your source data and the canonical schema. It owns the per-dataset weirdness — file layouts, signal naming, coordinate frames, timestamp conventions — and produces rows that any downstream notebook can consume. Everything upstream of the canonical Delta tables is the adapter's responsibility; everything downstream is generic.

The contract is the `Adapter` Protocol in `demos/byod/lib/adapter.py`. About a dozen methods total. Some are trivial; some take real effort. The complexity depends on what your source data offers — a dataset with pre-aggregated ECU scalars is dramatically easier to adapt than one with raw per-object annotations.

A realistic adapter takes a working engineer about a week. The four existing adapters are reference implementations covering different scenarios — start from a copy of the one closest to your situation.

## The four reference adapters

| Adapter | Use as baseline when… |
|---|---|
| **NuScenes** (`demos/byod/adapters/nuscenes/`) | Your source data is per-object annotations (JSON, per-frame, with bbox / cuboid geometry) and you need to *synthesize* scalar channels from the annotations. The most complete reference; full pipeline including Phase 4 hand-off. |
| **A2D2** (`demos/byod/adapters/a2d2/`) | Your source data has real OEM-style CAN bus signals (`vehicle_speed`, `steering_angle_calculated`, etc.) that need name translation but no aggregation. Closer to real fleet data than NuScenes. |
| **PandaSet** (`demos/byod/adapters/pandaset/`) | You have multi-LiDAR setups and want per-physical-sensor `object_tracks` rows (spinning LiDAR + solid-state LiDAR analyzed separately). Cleanest license of the four (CC BY 4.0). |
| **mdf4_sample** (`demos/byod/adapters/mdf4_sample/`) | Your source data is in MDF4 / binary log format, or you are testing the binary-log ingest path. Sidecar directory layout for binary media that lives outside the MDF4 itself. |

### Common scenario → baseline mapping

| Your scenario | Start from |
|---|---|
| OEM fleet data: MDF4 with pre-aggregated ECU scalars, camera frames in sidecar files | mdf4_sample |
| Public research dataset with per-object annotations needing scalar synthesis | NuScenes |
| Customer data with OEM-internal signal names, multi-modal sensors, no scalar aggregation needed | A2D2 |
| Custom perception pipeline output with multiple physical LiDAR streams to compare | PandaSet |
| Pure CAN/ECU data, no perception layer | A2D2 (drop the camera/lidar methods) |
| Pre-aggregated scalars + perception annotations from a research dataset you do not own | NuScenes (closest pattern; replace the loader logic) |

## What the adapter Protocol asks of you

Twelve methods total, in three groups by complexity.

### Group A — trivial (under 20 lines each)

Metadata and routing:

- **`download(dataroot)`** — fetch or validate source data. For datasets requiring registration (NuScenes, A2D2), this raises `FileNotFoundError` with instructions and the user provides the data manually. For PandaSet, it can fetch from a known URL.
- **`scenes()`** — yield recording objects, each with `.container_id` and `.name`. Just list the things in your source data.
- **`openlabel_metadata()`** — return strings for the OpenLABEL JSON header: `annotator`, `exporter`, `stream_description_prefix`.
- **`visualize_format()`** — return hints for the visualization notebook: which reader to use for camera frames, which for LiDAR, what dtype the LiDAR points are stored in.

### Group B — moderate (50–200 lines)

Translation from your source format into the canonical schema:

- **`ingest_metadata(spark)`** — write `container_tags`, `container_metrics`, and `channel_tags` rows. The canonical-to-source signal mapping is a Python dict in your adapter's `ingest.py` (per ADR-P13). Allocate channel IDs in a numeric range distinct from other adapters' channels.
- **`scalar_source(spark)`** — return a DataFrame matching `CHANNELS_SCHEMA`. This is where you either pass through ECU-aggregated scalars (the OEM case) or synthesize them from per-object annotations (the NuScenes case). The platform does not care which path produced the value.
- **`perception_paths()`** — yield one `perception_channels` row per camera frame, LiDAR scan, or other binary blob. File paths point at the bytes in Unity Catalog Volumes.

### Group C — hard (200–500 lines)

Per-object geometry and per-sensor detail:

- **`map_to_object_tracks(scene, min_confidence)`** — per-frame row assembly for `object_tracks`. Coordinate frame conversion (global → ego), azimuth sectorization (continuous angle → sector enum), distance computation, source-string aggregation across sensors. The `lakevision.geometry` module has helpers for the math.
- **`map_to_lidar_detections(scene, event_windows)`** — produce `lidar_object_detections` rows for each TSAL event window. 3D cuboids in vehicle frame.
- **`map_to_camera_detections(scene, event_windows)`** — produce `camera_object_detections` rows. 2D bboxes projected from 3D or read from native annotations.

Group C is what makes the difference between "we ingested the data and can detect events" and "the full pipeline works including OpenLABEL export." If you do not need the Phase 4 hand-off path (labelling tools, training pipelines), you can skip Group C; notebook `03_per_event_detail.py` produces empty per-sensor tables but everything else still works.

## Walk-through of the work

Order of operations when writing a new adapter, with the milestones that confirm each step worked:

1. **Copy a baseline.** Pick from the scenario mapping above and copy `demos/byod/adapters/<baseline>/` to `demos/byod/adapters/<your-name>/`. Rename the `Adapter` class and update the registry entry in `__init__.py`.
2. **Implement `download()` and `scenes()`.** Smallest blocks; get them returning real data first.
3. **Translate scalars.** Decide which canonical channel names your dataset maps to and write the `SIGNAL_MAP` Python dict (see ADR-P13 for the pattern). Implement `scalar_source(spark)` to read your raw data and emit rows matching `CHANNELS_SCHEMA`.
4. **Index media files.** Implement `perception_paths()` to yield one row per frame per sensor.
5. **Run notebook `01_ingest.py`.** End-of-day-one milestone. `channels`, `channel_tags`, and `perception_channels` should populate.
6. **Run notebook `02_detect_events.py`.** TSAL should detect events from your scalar channels. This is the validation that your scalar synthesis is correct — if `02` finds events, your scalars are reaching the platform in the right shape.
7. **Implement `map_to_object_tracks`.** Per-frame row assembly. Use `lakevision.geometry` for coordinate transforms and azimuth sectorization. Validate by spot-checking a few rows.
8. **(Optional)** Implement `map_to_lidar_detections` and `map_to_camera_detections` if you need the Phase 4 hand-off.
9. **Run notebooks `03_per_event_detail.py` and `04_visualize.py`.** End-to-end smoke test. OpenLABEL JSON files should appear in your volume; the visualization notebook should overlay detections on camera frames.
10. **Write tests.** Synthetic-fixture unit tests under `tests/demos/byod/adapters/<your-name>/` mirroring the per-adapter conftest pattern from the existing adapters.

## Common challenges

These consistently take more time than expected when writing a new adapter.

**Coordinate frames.** Most datasets give you object positions in global frame (UTM, lat/long, or world coordinates). `object_tracks` is in ego frame: x forward, y left, z up, ego at origin. You need ego pose at every frame timestamp to do the conversion. NuScenes provides ego pose per frame; A2D2 has GPS and you derive heading from successive samples; PandaSet provides explicit ego pose. `lakevision.geometry` has the helper functions.

**Timestamp alignment.** Source data often uses absolute Unix epoch (nanoseconds since 1970). `channels.tstart` uses recording-relative microseconds. The conversion is straightforward but you need to pick a per-recording epoch (NuScenes uses the first sample timestamp, A2D2 uses the first GPS fix) and stick with it. Record the epoch in `container_tags` as `epoch_start_ns` so downstream consumers can convert back to wall-clock time.

**Channel ID allocation.** Each adapter writes channels with specific numeric IDs. Two adapters reusing the same ID for different signals breaks cross-adapter analysis. The existing adapters allocate from non-overlapping ranges (NuScenes 1000–1999, A2D2 2000–2999, PandaSet 3000–3999, mdf4_sample 4000–4999). Pick a range and document it in your adapter's `loader.py`.

**Detection-class enumerations.** `object_tracks.detection_class` is a string enum: `pedestrian`, `car`, `cyclist`, `truck`, `motorcycle`, `bus`. Map your dataset's classes to these. If your dataset has classes the existing enum does not cover (construction vehicles, animals, scooters), document the additions in your adapter and discuss before extending the enum platform-wide.

**Confidence scales.** `object_tracks.confidence` is a float in [0, 1]. If your data has a discrete confidence scale (NuScenes uses `visibility_token` values 1–4), document the mapping in your `loader.py` and apply it once at ingest.

**Sensor source string.** `object_tracks.source` is pipe-delimited (`lidar|camera`, `lidar|radar|camera`, etc.). It records which sensors observed the object — KPI workflows filter on this. For datasets without per-sensor coverage metadata, you may need to invent the rule (NuScenes uses LiDAR and radar point counts plus the camera visibility token).

**Idempotence.** Adapters should be re-runnable. The notebooks write with `mode='overwrite'` by default for ingest. If your source has multiple recordings, partition or filter so re-ingesting one recording does not affect others.

## Testing your adapter

Each existing adapter has tests under `tests/demos/byod/adapters/<name>/`. The pattern:

- `__init__.py` and `conftest.py` per adapter. The `__init__.py` markers prevent test-file basename collisions across adapters (without them, `test_object_tracks.py` in two adapters would collide during pytest collection).
- Synthetic fixtures for unit tests. NuScenes uses a minimal synthetic `Scene` object; PandaSet uses synthetic point-cloud arrays.
- Integration tests that go end-to-end against a small fixture. PandaSet exercises this; the `mdf4_sample` adapter has a four-layer round-trip test that verifies path channels round-trip byte-identical through MDF4.

The shared integration tests under `tests/mda_query_engine/perception/integration/` exercise every registered adapter through the BYOD pipeline. Once your adapter is registered (`lib.adapter.register(...)` in your `__init__.py`), those tests pick it up automatically.

## Where the adapter lives in the bigger picture

The adapter is a thin layer at the top of the data flow. Once it writes canonical rows to `channels`, `channel_tags`, `perception_channels`, and `object_tracks`, everything downstream is generic. The Impulse Core TSAL solver, the LakeVision OpenLABEL builder, the playlist DLT pipeline, the visualization notebook — none of these know which adapter produced the data. That generality is the entire reason the adapter is the only customer-specific code.

If your work stops at "we ingested the data and ran TSAL scenario search," you only need Groups A and B. Group C is additional work for the labelling and training hand-off path, and it is the place where most of the per-dataset complexity actually lives.
