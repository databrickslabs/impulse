# Authoring events for perception data

This document is the authoring walk-through for the perception event
vocabulary. It assumes you have read [`01_how_it_works.md`](01_how_it_works.md)
and understand the data model and how `PerceptionSolver` evaluates predicates.
This doc shows how to write each kind of event against your data, how to
compose them, and how to arrange temporal sequences.

Every event class writes into the same `event_instance_fact` silver table —
`(container_id, event_id, event_instance_id, start_ts, end_ts)`. That table
is the raw solver output: append-only, every window any predicate ever fired
on. Downstream tooling reads from `event_instance_fact` and builds curated
sets for KPI rollups, exports, and training-data manifests.

## Contents

- [Basic events over scalar channels](#basic-events-over-scalar-channels)
- [Container-level predicates](#container-level-predicates)
- [Per-object perception predicates](#per-object-perception-predicates)
- [Per-object windowing with track_scope](#per-object-windowing-with-track_scope)
- [Compound predicates](#compound-predicates)
- [Temporal sequences](#temporal-sequences)
- [Worked example: multi-source compound query](#worked-example-multi-source-compound-query)

## Basic events over scalar channels

The simplest case. A Boolean predicate over one or more recorded channels:

```python
pedestrian_distance = db.channel("Pedestrian_Nearest_Distance_m")
vehicle_speed       = db.channel("Vehicle_Speed_kph")

pedestrian_high_speed = BasicEvent(
    name="pedestrian_high_speed_proximity",
    expr=(pedestrian_distance <= 15.0) & (vehicle_speed > 30.0),
    desc="Pedestrian within 15 m while travelling above 30 kph",
)
report.add_event(pedestrian_high_speed)
```

The TSAL solver compiles the predicate into a single scan over the
run-length-encoded `channels` table. One `BasicEvent`, one pass over the
relevant channels, intervals out into `event_instance_fact`.

## Container-level predicates

Container-level metadata — weather, region, road class, vehicle config —
is best expressed not by scanning channels but by filtering whole recordings:

```python
highway_recordings = ContainerEvent(
    name="highway_daylight_recordings",
    attributes={"road_class": "highway", "ambient_light": "daylight"},
    desc="Containers tagged as highway daylight recordings",
)
report.add_event(highway_recordings)
```

A `ContainerEvent` matches whole `container_id` rows; the output in
`event_instance_fact` is one row per matching container with `start_ts` and
`end_ts` set to the container bounds. Same schema as any other event, so
downstream tooling treats container events identically to time-windowed ones.

## Per-object perception predicates

Same authoring style as `BasicEvent`, but the surface is `object_tracks`:

```python
ot = db.query.object_track   # ObjectTrackAccessor

cyclist_left = PerceptionEvent(
    name="cyclist_left_approaching",
    expr=(ot.detection_class("cyclist"))
         & (ot.azimuth("front_left"))
         & (ot.relative_velocity_ms < -3.0),
    desc="Cyclist approaching from the front-left at >3 m/s closing speed",
)
report.add_event(cyclist_left)
```

The predicate is evaluated row-by-row against `object_tracks` (one row is one
detected object at one frame). A frame matches when at least one row passes;
windows form by collapsing contiguous matching frames per `container_id`.

String columns use the callable form — `ot.detection_class("cyclist")`. Numeric
columns use comparison operators — `ot.distance_m < 8.0`. Predicates compose
with `&` (AND) and `|` (OR).

### Source filtering

The `source` column is a pipe-delimited provenance string (`lidar|radar|camera`).
Use `source_contains` to restrict to rows where a specific sensor contributed:

```python
lidar_only_cyclist = PerceptionEvent(
    name="cyclist_lidar_confirmed",
    expr=(ot.detection_class("cyclist"))
         & ot.source_contains("lidar")
         & (ot.confidence >= 0.8),
    desc="Cyclist detected with LiDAR at confidence >= 0.8",
)
```

### Multi-value azimuth filtering

Pass a list to `isin` on a string-column proxy when the predicate covers
several sectors:

```python
left_sectors = ["front_left", "left", "rear_left"]

pedestrian_on_left = PerceptionEvent(
    name="pedestrian_any_left_sector",
    expr=(ot.detection_class("pedestrian")) & ot.azimuth.isin(left_sectors),
)
```

## Per-object windowing with track_scope

Frame-level windowing fires whenever any object passes the predicate in that
frame. Per-object windowing — `track_scope=True` — runs a separate window
formation for each `(container_id, object_id)` pair. Use this for cut-ins,
lane changes, and any scenario that depends on the behaviour of one tracked
object across time.

```python
ot_ts = ot(track_scope=True)   # accessor that taints every selector with track_scope=True

sustained_pedestrian = PerceptionEvent(
    name="pedestrian_tracked_3s",
    expr=(ot_ts.detection_class("pedestrian")) & (ot_ts.confidence >= 0.7),
    min_duration_ms=3000,
    desc="One pedestrian object tracked continuously at confidence >= 0.7 "
         "for at least 3 seconds",
)
report.add_event(sustained_pedestrian)
```

With `track_scope=True`, windows form per `(container_id, object_id)`.
Each window writes one row to `event_instance_fact` (unchanged); the
associated `object_id` is recorded in `perception_event_instance_objects`
so perception-aware reads can recover which object triggered each instance.

`min_duration_ms` is the post-window debounce threshold — windows shorter than
the given number of milliseconds are dropped. The same threshold applies whether
or not `track_scope` is set.

A single predicate may not mix `track_scope=True` and `track_scope=False`
selectors. `PerceptionEvent` raises at construction time if you attempt this.
Write two separate events if you need both window shapes.

### Cut-in example

```python
ot_ts = ot(track_scope=True)

cut_in = PerceptionEvent(
    name="vehicle_cut_in",
    expr=(ot_ts.detection_class("car"))
         & (ot_ts.lane_offset == 0)
         & (ot_ts.relative_velocity_ms < 0.0),
    min_duration_ms=500,
    desc="A vehicle tracked in ego lane while approaching; window must hold for 500 ms",
)
report.add_event(cut_in)
```

## Compound predicates

Two patterns that frame-level or per-object scope alone cannot express:

- **Two different objects in the same frame.** "A cyclist and a pedestrian both
  visible on the left at the same time."
- **A perception condition together with a scalar-channel condition.** "A
  cyclist on the left while vehicle speed exceeds 30 kph."

The current supported pattern is to run the two events separately and join on
`event_instance_fact`, or to use `SequenceOfEvents` when the two conditions are
temporally ordered (including "simultaneous" via `max_step_duration_ms=0`).

For the two-objects-same-frame case, run each object predicate as a separate
`PerceptionEvent`, then join the resulting `event_instance_fact` rows on
`(container_id, start_ts, end_ts)` to find overlapping windows:

```python
cyclist_left = PerceptionEvent(
    name="cyclist_present_left",
    expr=(ot.detection_class("cyclist")) & ot.azimuth("front_left"),
)
pedestrian_left = PerceptionEvent(
    name="pedestrian_present_left",
    expr=(ot.detection_class("pedestrian")) & ot.azimuth("front_left"),
)
report.add_event(cyclist_left)
report.add_event(pedestrian_left)
# Post-solve: overlap join on event_instance_fact to find co-occurring windows.
```

## Temporal sequences

`SequenceOfEvents` evaluates an ordered pattern of expressions, each yielding
intervals. The classic use is state-transition scenarios — lane changes, AEB
intervention sequences, vehicle approach behaviours:

```python
left_to_ego_lane_change = SequenceOfEvents(
    name="left_to_ego_lane_change",
    expressions=[
        db.channel("Lane_Offset") == -1,
        db.channel("Lane_Offset") == 0,
    ],
    desc="Lane offset transitions from the left lane (−1) to the ego lane (0)",
)
report.add_event(left_to_ego_lane_change)
```

Each step is a `TimeSeriesExpression`. The sequence fires when the steps'
intervals appear in order on the same `container_id`.

Perception event intervals participate in `SequenceOfEvents` as expressions
— pass a `PerceptionEvent`'s expression directly as a step:

```python
aeb_then_clear = SequenceOfEvents(
    name="aeb_then_path_clear",
    expressions=[
        db.channel("AEB_Active") == True,
        (ot.detection_class("pedestrian")) & (ot.azimuth("front_center")),
    ],
    desc="AEB active followed by a pedestrian in the front-center zone clearing",
)
report.add_event(aeb_then_clear)
```

## Worked example: multi-source compound query

The scenario: find windows where a cyclist was detected with low confidence on
a production LiDAR sensor, followed within 2 seconds by a high-confidence
detection from a roof-rack reference LiDAR on the same test vehicle.

```python
ot = db.query.object_track

# 1. Scope to dual-LiDAR test vehicles.
gt_lidar_containers = ContainerEvent(
    name="gt_lidar_rig_containers",
    attributes={"vehicle_config": "dual_lidar"},
    desc="Recordings from a test vehicle equipped with a roof-rack reference LiDAR",
)

# 2. Low-confidence cyclist on the production in-car sensor.
cyclist_lowconf_prod = PerceptionEvent(
    name="cyclist_lowconf_prod",
    expr=(ot.detection_class("cyclist"))
         & ot.source_contains("lidar_inboard")
         & (ot.confidence < 0.5),
    container_filter=gt_lidar_containers,
    desc="Cyclist with low production-LiDAR confidence on dual-LiDAR containers",
)

# 3. High-confidence cyclist on the roof-rack reference sensor.
cyclist_highconf_gt = PerceptionEvent(
    name="cyclist_highconf_gt",
    expr=(ot.detection_class("cyclist"))
         & ot.source_contains("lidar_roof_rack")
         & (ot.confidence >= 0.8),
    container_filter=gt_lidar_containers,
    desc="Cyclist with high confidence on the roof-rack reference LiDAR",
)

# 4. Sequence: low-confidence on production followed by high-confidence on reference.
cyclist_disagreement = SequenceOfEvents(
    name="cyclist_prod_lowconf_then_gt_highconf",
    expressions=[
        cyclist_lowconf_prod.get_expression(),
        cyclist_highconf_gt.get_expression(),
    ],
    container_filter=gt_lidar_containers,
    max_step_duration_ms=2000,
    desc="Production LiDAR low-confidence cyclist followed within 2 s by "
         "roof-rack high-confidence detection — likely production sensor miss",
)

report.add_event(cyclist_lowconf_prod)
report.add_event(cyclist_highconf_gt)
report.add_event(cyclist_disagreement)
report.run()
```

**What each construct does.**

- `ContainerEvent` scopes the entire query to containers tagged
  `vehicle_config = "dual_lidar"`. The same instance is reused as
  `container_filter=` on every downstream event — one filter, declared once,
  pushed into every source scan.
- Each `PerceptionEvent` produces windows independently into
  `event_instance_fact`.
- `SequenceOfEvents` adds the temporal constraint — the two windows must appear
  in order within 2 seconds of each other on the same `container_id`.
- All three share the same `event_instance_fact` output schema, so playlist
  curation, KPI dashboards, and OpenLABEL exports work identically across them.
