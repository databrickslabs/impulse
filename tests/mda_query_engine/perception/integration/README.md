# Integration tests — synthetic data through demo pipelines

Local end-to-end tests for the LakeVision demos. Each test file feeds **synthetic source-shaped data** through the same library functions and SQL transforms used by the demo notebooks, then asserts the resulting tables hold what the demo claims they will.

These tests do **not** touch a real workspace. They run against a local Spark session (the `spark` fixture in `tests/conftest.py`) writing Delta tables to a temp directory. They are fast enough to run on every push.

## What "synthetic source-shaped" means

For each demo, the fixtures in `conftest.py` produce minimal data shaped like the real input:

- **nuscenes pipeline**: a few synthetic scenes with 2–3 sample frames each, fake LiDAR scans, and signal CSVs matching the nuScenes signal naming conventions used by `demos/nuscenes_e2e/lib/`.
- **a2d2 / BYOD pipeline**: a few synthetic frames + sensor records matching the A2D2 JSON/NPY layout described in `demos/byod/configs/a2d2.yaml`.

The fixtures are written once per demo, then imported by the test files. Synthetic data is the seam that lets these tests run without datasets in the gigabytes.

## What gets asserted

For each test file:

- Library helper outputs match the schema declared in `src/mda_query_engine/perception/schema/`.
- Tables produced by one notebook satisfy the input contract of the next notebook (e.g. `object_tracks` written by step 05 has the columns that step 09 reads).
- For BYOD specifically, the **same pipeline** invoked with two different configs (`nuscenes` and `a2d2`) produces equivalent table shapes — this is the regression net for "swapping configs requires zero notebook edits".

### BYOD per-notebook invariants

Each notebook also enforces `assert`-based **acceptance checks** inline (one per step). The integration test should cover the same invariants on synthetic fixtures so regressions surface before any workspace deploy:

| # | Notebook | Step | Output | Invariant |
|---|---|---|---|---|
| 01 | `01_ingest.py` | Step 2 — metadata | `container_tags`, `container_metrics`, `channel_tags` | All three non-empty; ≥1 distinct `container_id`. |
| 01 | `01_ingest.py` | Step 3 — scalars | `channels`, `channel_metrics` | `channels` non-empty; **`channel_metrics` row count = distinct `(container_id, channel_id)` in `channels`**. The TSAL solver inner-joins on `channel_metrics`, so a missing or partial index silently zeros downstream events. |
| 01 | `01_ingest.py` | Step 4 — perception | `perception_channels` | Non-empty; no NULL `file_path`; every `file_path` is a UC Volume path (ADR-P3). |
| 01 | `01_ingest.py` | Step 5 — object tracks | `object_tracks` | Non-empty; ≥1 distinct `detection_class`. |
| 02 | `02_detect_events.py` | TSAL + playlist | `event_instance_fact`, `playlist_items` | Both non-empty for the active `playlist_id`. |
| 03 | `03_per_event_detail.py` | Step 1 — LiDAR cuboids | `lidar_object_detections` | Non-empty; every container's detection span lies inside its playlist span (ADR-7 temporal-leak gate). |
| 03 | `03_per_event_detail.py` | Step 2 — camera bboxes | `camera_object_detections` | Same shape as the LiDAR invariant. |
| 03 | `03_per_event_detail.py` | Step 3 — OpenLABEL | `<volume_openlabel>/<playlist_id>/*.json` | JSON file count = playlist window count (one package per event window). |
| 04 | `04_visualize.py` | KPI half | `dist_stats`, gap counts | Distance stats non-empty; gap counts ≥ 0. |

When a workspace regression slips past the inline asserts, add a fixture that reproduces it and an assertion to the BYOD integration test so the local tier catches it next time.

## Naming

- `test_nuscenes_pipeline.py` — drives `demos/nuscenes_e2e/lib/` over synthetic nuscenes data.
- `test_a2d2_pipeline.py` — drives the same library over synthetic a2d2 data via the byod config.
- One file per demo. Cross-demo invariants belong in a shared `test_pipeline_invariants.py`.

## Running

```
cd reusable/repos/impulse
uv run --exact --all-extras pytest tests/mda_query_engine/perception/integration/ -v
```

Single file:

```
uv run --exact --all-extras pytest tests/mda_query_engine/perception/integration/test_nuscenes_pipeline.py -v
```

## Authoring a new integration test

1. Add a fixture in `conftest.py` that returns a populated temp catalog/schema (synthetic Bronze tables in place).
2. Write a test that calls the demo's library functions (`from demos.nuscenes_e2e.lib import ...`) against that fixture.
3. Assert on the output tables.

Keep fixtures small (single-digit row counts where possible). The test is checking *shape and wiring*, not statistical fidelity — large fixtures slow the suite without adding coverage.
