# How perception querying works

This package extends Impulse core with the ability to query `object_tracks`
— fused per-object sensor data — alongside scalar channels, using the same
TSAL authoring surface that `BasicEvent` already provides.

See [`03_authoring_events.md`](03_authoring_events.md) for the authoring
walk-through and [`04_roadmap.md`](04_roadmap.md) for what is planned next.

## The data model: object_tracks

The `object_tracks` table is a sensor-agnostic view of fused perception output.
One row represents one tracked object at one frame, regardless of how many
sensors contributed to the estimate.

```
container_id | frame_ts | object_id | detection_class | distance_m | lane_offset | relative_velocity_ms | azimuth     | confidence | source
1            | 1000000  | 100       | pedestrian      | 8.5        |  0          | -0.3                 | front       | 0.94       | lidar|camera
1            | 1000000  | 101       | pedestrian      | 12.1       | +1          | -0.1                 | front_left  | 0.85       | lidar|camera
1            | 1000000  | 102       | cyclist         | 18.7       | -1          | -3.4                 | front_left  | 0.78       | lidar|radar|camera
1            | 1100000  | 100       | pedestrian      | 8.2        |  0          | -0.3                 | front       | 0.94       | lidar|camera
1            | 1100000  | 102       | cyclist         | 15.3       | -1          | -3.4                 | front_left  | 0.81       | lidar|radar|camera
```

Key design choices:

- `azimuth` is a sector enum (`front`, `front_left`, `left`, `rear_left`, …)
  rather than a continuous angle — scenario predicates naturally say "came from
  the left," not "was at 267.3 degrees."
- `source` is a pipe-delimited provenance string (`lidar|radar|camera`). Filter
  on `source_contains("lidar")` to restrict to rows where LiDAR contributed.
- `relative_velocity_ms` is negative when the object is approaching ego.
- `lane_offset` is relative to ego's lane: −2, −1, 0, +1, +2.
- `frame_ts` is recording-relative microseconds, aligned to the same origin as
  `channels.tstart` so event windows from the two surfaces are directly
  comparable.

The full schema is `mda_query_engine.perception.schema.scenario.OBJECT_TRACKS`.

## Downsampling modes

A full-resolution `object_tracks` table at thousands of recordings can run
into hundreds of millions of rows. `ObjectTracksConfig` provides two modes:

- **Full-stride (default).** Rows are written at a fixed rate for the whole
  recording. The minimum supported stride is 2 Hz, which is the Nyquist floor
  for sub-second ADAS events like a cyclist cut-in. This mode is
  discovery-friendly — every frame is available for investigation without
  pre-defining scenarios.

- **TSAL-gated.** Rows are written only inside TSAL event windows, plus a
  configurable buffer around each window (500 ms before and after by default).
  At a typical 5 percent event-window coverage, this mode is roughly
  20 times more compact. Use it once you have a stable playlist and want the
  storage saving.

The trade-off with TSAL-gated mode: object continuity outside the buffer is
not stored. If you need to investigate what an object was doing several seconds
before a window opened, extend the buffer or switch to full-stride.

## The event vocabulary

Every event class writes into the same `event_instance_fact` silver table —
`(container_id, event_id, event_instance_id, start_ts, end_ts)`. This package
adds two new ways to populate it.

| Class                                  | Predicate surface    | Window granularity                             | Use case                                                           |
| -------------------------------------- | -------------------- | ---------------------------------------------- | ------------------------------------------------------------------ |
| `BasicEvent`                           | `channels` (scalar)  | per `container_id`                             | Speed above threshold, AEB active, detection count exceeds limit   |
| `PerceptionEvent`                      | `object_tracks`      | per `container_id` (frame fires when any row matches) | Cyclist on front-left, pedestrian within 8 m at high confidence    |
| `PerceptionEvent` with `track_scope=True` | `object_tracks`  | per `(container_id, object_id)`                | Per-object cut-in, per-object sustained track, per-object lane change |
| `SequenceOfEvents`                     | any expression source | per `container_id`                            | AEB intervention followed by lead-vehicle distance falling below 5 m |

`PerceptionEvent` without `track_scope` fires once per frame where at least
one `object_tracks` row satisfies the predicate, then collapses contiguous
frames into windows per `container_id`.

`PerceptionEvent` with `track_scope=True` runs a per-object inner loop: for
each `object_id` present in the container, the predicate is evaluated against
only that object's rows, producing one window set per `(container_id, object_id)`.
Each distinct window still writes one row to `event_instance_fact`; the
`object_id` is preserved in the `perception_event_instance_objects` side-car
(see below).

`min_duration_ms` is available on `PerceptionEvent` in both modes: any window
shorter than the threshold is dropped before write, suppressing single-frame
perception noise.

## How PerceptionSolver works

`PerceptionSolver` extends Impulse core's `KeyValueStoreSolver`. If a query
has no perception selectors it delegates to the parent unchanged. Otherwise it
cogroups `channels` and `object_tracks` on `container_id` and applies a single
Pandas UDF that receives both DataFrames for each container:

- A `PerceptionCache` is built over both surfaces simultaneously.
- For `track_scope=False` selections the cache evaluates the predicate across
  all objects and emits `[start_ts, end_ts]` pairs per container.
- For `track_scope=True` selections the UDF loops over each unique `object_id`,
  builds a single-object cache, evaluates the predicate, and emits
  `[start_ts, end_ts, object_id]` triples.

A single predicate may not mix `track_scope=True` and `track_scope=False`
selectors — `PerceptionEvent` raises at construction time if this is detected.

## The TSAL authoring surface

`ObjectTrackAccessor` is the proxy you use to build per-object predicates.
It knows the `OBJECT_TRACKS` schema and rejects unknown column names.

```python
ot = db.query.object_track   # ObjectTrackAccessor instance

# String columns — callable form
ot.detection_class("cyclist")    # eq on detection_class
ot.azimuth("front_left")

# Numeric columns — comparison operators
ot.distance_m < 8.0
ot.confidence >= 0.7

# Helpers for multi-value fields
ot.source_contains("lidar")      # substring on the pipe-delimited source string

# Per-object windowing mode
ot_ts = ot(track_scope=True)    # taints every selector it produces
```

Predicates compose with `&` and `|`. Full authoring walk-throughs are in
[`03_authoring_events.md`](03_authoring_events.md).

## Output: event_instance_fact and the perception side-car

`PerceptionEvent.determine_events` writes into the standard
`event_instance_fact` schema — `(container_id, event_id, event_instance_id,
start_ts, end_ts)` — identical to `BasicEvent` and `ContainerEvent`. All
downstream tooling (playlist curation, OpenLABEL export, KPI rollups) is
event-class-agnostic.

For `track_scope=True` events, the `object_id` associated with each window is
written to a separate side-car table: `perception_event_instance_objects`.

```
container_id | event_id | event_instance_id | object_id
1            | 4021     | 98712             | 102
1            | 4021     | 98713             | 107
```

Schema: `mda_query_engine.perception.schema.scenario.PERCEPTION_EVENT_INSTANCE_OBJECTS`.
Primary key: `(container_id, event_id, event_instance_id)` — at most one row
per `event_instance_fact` row. Perception-aware reads LEFT JOIN this table on
that triple to recover `object_id`.

## perception_channels — file-path index

One row per frame per sensor, holding the Unity Catalog Volume path to the
binary media file. Timestamps are recording-relative microseconds aligned to
the same origin as `channels` and `object_tracks`, so scene-cutting from an
event window to its frames is one filter:
`WHERE timestamp BETWEEN event.start_ts AND event.end_ts`.
Raw binary data is never stored in Delta columns.

## Authoring examples

Frame-level — cyclist approaching from the front-left:

```python
ot = db.query.object_track

cyclist_approaching = PerceptionEvent(
    name="cyclist_front_left_approaching",
    expr=(ot.detection_class("cyclist"))
         & (ot.azimuth("front_left"))
         & (ot.relative_velocity_ms < -3.0),
    desc="Cyclist approaching from the front-left at >3 m/s closing speed",
)
report.add_event(cyclist_approaching)
```

Per-object — one pedestrian tracked continuously for at least 3 seconds:

```python
ot_ts = ot(track_scope=True)

sustained_pedestrian = PerceptionEvent(
    name="pedestrian_tracked_3s",
    expr=(ot_ts.detection_class("pedestrian")) & (ot_ts.confidence >= 0.7),
    min_duration_ms=3000,
    desc="One pedestrian tracked at confidence >= 0.7 for at least 3 s",
)
report.add_event(sustained_pedestrian)
```

For compound and sequence patterns see [`03_authoring_events.md`](03_authoring_events.md).
