# Authoring events for your data

This document is the authoring walk-through for the LakeVision event vocabulary. It assumes you have read [`01_how_it_works.md`](01_how_it_works.md) and understand what the event vocabulary contains. This doc shows how to write each kind of event against your data, how to compose them, and how to keep the lineage clean enough for safety-case work.

Every event class writes into the same `event_instance_fact` silver table — `(container_id, event_id, event_instance_id, start_ts, end_ts)`. That table is the raw solver output: append-only, moving target, every window any predicate ever fired on. Downstream tooling — KPI rollups, OpenLABEL exports, OpenSCENARIO exports, training-data manifests, dashboards — reads `playlist_items` (the gold layer of named, versioned, immutable curated sets built from `event_instance_fact` rows), not `event_instance_fact` directly. Composition happens at authoring time on the silver layer; consumption goes through playlists.

Today's surface is `BasicEvent` over scalar channels and `ContainerEvent` over recording metadata. `PerceptionEvent`, the helpers `perception_signal(...)` and `db.register_derived_channel(...)`, and the source-agnostic extension of `SequenceOfEvents` are planned. The walk-throughs below show what each one looks like under the unified model.

## Contents

- [Basic events over scalar channels](#basic-events-over-scalar-channels)
- [Container-level predicates](#container-level-predicates)
- [Per-object perception predicates](#per-object-perception-predicates)
- [Compound predicates: production and exploration](#compound-predicates-production-and-exploration)
- [Temporal sequences](#temporal-sequences)
- [Layered composition: events of events](#layered-composition-events-of-events)
- [Worked example: a multi-class compound query](#worked-example-a-multi-class-compound-query)
- [Version pinning for reproducibility](#version-pinning-for-reproducibility)

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

The TSAL solver compiles the predicate into a single scan over the run-length-encoded `channels` table. One `BasicEvent`, one pass over the relevant channels, intervals out into `event_instance_fact`.

## Container-level predicates

Container-level metadata — weather, region, road class, vehicle config, campaign tag — is best expressed not by scanning channels but by filtering whole recordings:

```python
highway_recordings = ContainerEvent(
    name="highway_daylight_recordings",
    attributes={"road_class": "highway", "ambient_light": "daylight"},
    desc="Containers tagged as highway daylight recordings",
)
report.add_event(highway_recordings)
```

A `ContainerEvent` matches whole `container_id` rows; the output in `event_instance_fact` is one row per matching container with `start_ts` and `end_ts` set to the container bounds. Same schema as any other event, so playlists, KPI groupings, and exports treat container events identically to time-windowed ones.

## Per-object perception predicates

Same authoring elegance as `BasicEvent`, surface is `object_tracks`:

```python
ot = db.perception.tracks

cyclist_left = PerceptionEvent(
    name="cyclist_left_approaching",
    expr=(ot.detection_class == "cyclist")
         & (ot.azimuth == "front_left")
         & (ot.relative_velocity_ms < -3.0),
    desc="Cyclist approaching from front-left at >3 m/s closing speed",
)
report.add_event(cyclist_left)
```

The predicate is evaluated row-by-row against `object_tracks` (one row is one detected object at one frame). A frame matches when at least one row passes; windows form by collapsing contiguous matching frames per `container_id`. Per-object windowing — for cut-ins, lane changes, sustained-track behaviour — is one flag away:

```python
sustained_pedestrian_track = PerceptionEvent(
    name="pedestrian_tracked_3s",
    expr=(ot.detection_class == "pedestrian") & (ot.confidence >= 0.7),
    track_scope=True,
    min_duration_ms=3000,
    desc="One pedestrian object tracked continuously for at least 3 seconds",
)
```

With `track_scope=True`, windows form per `(container_id, object_id)` instead of per `container_id`. `min_duration_ms` filters out tracks shorter than the threshold, suppressing single-frame perception noise.

## Compound predicates: production and exploration

Two patterns the row-level scope of `BasicEvent` and `PerceptionEvent` alone cannot express:

- **Two different objects in the same frame.** "A cyclist and a pedestrian both visible on the left at the same time."
- **An object together with a scalar-channel condition.** "A cyclist on the left while vehicle speed exceeds 30 kph."

Both collapse to a single `BasicEvent` whose subexpressions are windowed signals from any of three sources: recorded channels, registered derived channels, or ad-hoc `perception_signal(...)` calls. The difference between production and exploration is whether the perception side is registered.

**Production: register first, then compose.**

```python
ot   = db.perception.tracks
left = ["front_left", "side_left"]

# 1. Author each component as a named PerceptionEvent.
cyclist_present_left = PerceptionEvent(
    name="cyclist_present_left",
    expr=(ot.detection_class == "cyclist") & ot.azimuth.isin(left),
)
pedestrian_present_left = PerceptionEvent(
    name="pedestrian_present_left",
    expr=(ot.detection_class == "pedestrian") & ot.azimuth.isin(left),
)

# 2. Promote each to a derived scalar channel. One catalog row per event,
#    with a (name, version, definition_hash) lineage triple. Re-registering
#    an identical definition returns the existing handle without churn.
db.register_derived_channel(cyclist_present_left)
db.register_derived_channel(pedestrian_present_left)

# 3. Compose. db.channel(...) does not distinguish recorded from registered;
#    the channel-source registry resolves the name.
both_left = BasicEvent(
    name="cyclist_and_pedestrian_left",
    expr=db.channel("cyclist_present_left") & db.channel("pedestrian_present_left"),
)
risky_cyclist = BasicEvent(
    name="risky_cyclist_high_speed",
    expr=db.channel("cyclist_present_left") & (db.channel("Vehicle_Speed_kph") > 30.0),
)
```

The same registered signals power any number of downstream events — six months later, pulling `cyclist_present_left` version 1 returns exactly the intervals that matched the v1 definition. Reproducibility for safety case evidence holds even after v2 supersedes it as `active`.

**Exploration: skip registration, compute inline.**

```python
both_left_explore = BasicEvent(
    name="cyclist_and_pedestrian_left",
    expr=(
        perception_signal(ot, (ot.detection_class == "cyclist")    & ot.azimuth.isin(left))
        & perception_signal(ot, (ot.detection_class == "pedestrian") & ot.azimuth.isin(left))
    ),
)
```

`perception_signal(...)` computes the per-frame existence signal at solve time without writing to `derived_channels`. Use it while you are still tuning the predicate — confidence thresholds, azimuth ranges, whether the scenario shows up at all — without polluting the catalog with throwaway labels.

**Promotion is mechanical.** When an exploratory predicate proves useful, lift each `perception_signal(...)` call out into a named `PerceptionEvent`, call `db.register_derived_channel(...)` on it, and switch the inline call to `db.channel("name")`. Same windows, same matches; now with a name, a version, and a definition hash attached.

## Temporal sequences

`SequenceOfEvents` evaluates an ordered pattern of expressions, each yielding intervals. The classic use is state-transition scenarios — lane changes, AEB intervention sequences, vehicle approach behaviours:

```python
left_to_ego_lane_change = SequenceOfEvents(
    name="left_to_ego_lane_change",
    expressions=[
        db.channel("Lane_Offset") == -1,
        db.channel("Lane_Offset") == 0,
    ],
    desc="Lane offset transitions from the left lane (-1) to the ego lane (0)",
)
report.add_event(left_to_ego_lane_change)
```

Each step is a `TimeSeriesExpression`. The sequence fires when the steps' intervals appear in order on the same `container_id`. Under the planned source-agnostic refactor, the same algorithm runs against expressions from any registered source — including registered derived channels and per-object perception predicates — without a new event class:

```python
# After the source-agnostic refactor lands, the same algorithm composes
# perception inputs and channel inputs in one declarative sequence.
aeb_then_pedestrian = SequenceOfEvents(
    name="aeb_then_pedestrian_clear",
    expressions=[
        db.channel("AEB_Active") == True,
        db.channel("pedestrian_present_front_center") == 0,
    ],
    desc="AEB intervention followed by the front-center pedestrian leaving the scene",
)
```

The second expression references a registered derived channel sourced from a `PerceptionEvent`. From the solver's perspective it is just another interval-valued signal.

## Layered composition: events of events

Once an event is registered as a derived scalar channel, it is indistinguishable from any other channel as far as downstream events are concerned. Labels stack:

```python
# Layer 1 — physical-condition primitives, each a registered BasicEvent.
vehicle_stopped = BasicEvent(
    name="vehicle_stopped",
    expr=db.channel("Vehicle_Speed_kph") < 2.0,
)
traffic_light_red = BasicEvent(
    name="traffic_light_red",
    expr=db.channel("Traffic_Light_Ahead") == "red",
)
db.register_derived_channel(vehicle_stopped)
db.register_derived_channel(traffic_light_red)

# Layer 2 — a behavioural label composed from Layer 1 primitives.
stopped_at_red_light = BasicEvent(
    name="stopped_at_red_light",
    expr=db.channel("vehicle_stopped") & db.channel("traffic_light_red"),
)
db.register_derived_channel(stopped_at_red_light)

# Layer 3 — a perception input joins the stack, and a temporal sequence
# composes Layer 2 with the perception signal.
pedestrian_in_front = PerceptionEvent(
    name="pedestrian_in_front",
    expr=(ot.detection_class == "pedestrian") & (ot.azimuth == "front_center"),
)
db.register_derived_channel(pedestrian_in_front)

red_light_pedestrian_cross = SequenceOfEvents(
    name="red_light_pedestrian_cross",
    expressions=[
        db.channel("stopped_at_red_light"),
        db.channel("pedestrian_in_front"),
    ],
    desc="Vehicle stopped at a red light followed by a pedestrian crossing in front",
)
report.add_event(red_light_pedestrian_cross)
```

Each layer is a `(name, version, definition_hash)`-traceable entry in `derived_channels_definitions`. The dependency tree is recoverable from those rows — if `vehicle_stopped` ever rolls to v2 with a stricter threshold, every event referencing it surfaces in the lineage view. The path from "highway traffic jam" to a 50-row scenario taxonomy is the same shape repeated: one Layer 1 primitive per physical condition, Layer 2 behaviours composing primitives, Layer 3+ scenarios composing behaviours.

## Worked example: a multi-class compound query

The use case is real: the LiDAR training team owns the production in-car LiDAR. On test vehicles equipped with a higher-fidelity roof-rack LiDAR rig, they treat the roof-rack as ground truth. They want every window where the production sensor and the ground-truth roof-rack disagree on detection count by more than 5%, restricted to recordings from dual-LiDAR vehicles, plus a richer pattern (low-confidence cyclist on the production sensor followed shortly by a disagreement spike). Output should land in a named, versioned playlist they can pull on demand.

One query exercises every event class plus the registration, computed-channel, and playlist machinery:

```python
ot = db.perception.tracks

# 1. ContainerEvent — restrict downstream events to dual-LiDAR test vehicles.
gt_lidar_containers = ContainerEvent(
    name="gt_lidar_rig_containers",
    attributes={"vehicle_config": "dual_lidar"},
    desc="Recordings from a test vehicle with a roof-rack ground-truth LiDAR rig "
         "alongside the production in-car LiDAR",
)

# 2. PerceptionEvents — per-sensor presence, registered as derived channels so
#    other events can reference them by name.
prod_lidar_present = PerceptionEvent(
    name="lidar_prod_present",
    expr=ot.source.contains("lidar_inboard"),
    container_filter=gt_lidar_containers,
)
gt_lidar_present = PerceptionEvent(
    name="lidar_gt_present",
    expr=ot.source.contains("lidar_roof_rack"),
    container_filter=gt_lidar_containers,
)
db.register_derived_channel(prod_lidar_present)
db.register_derived_channel(gt_lidar_present)

# 3. Computed DerivedChannel — per-frame disagreement percentage. Not an event
#    projection (the value is scalar arithmetic, not boolean), so it goes through
#    the computed path: same registration call, different spec object.
lidar_disagreement_pct = DerivedChannel(
    name="lidar_count_disagreement_pct",
    sources=[ot],
    container_filter=gt_lidar_containers,
    compute_sql="""
        SELECT
            container_id,
            frame_ts AS ts,
            ABS(n_prod - n_gt) / GREATEST(n_prod, n_gt, 1) AS value
        FROM (
            SELECT
                container_id,
                frame_ts,
                COUNT(CASE WHEN source LIKE '%lidar_inboard%'   THEN 1 END) AS n_prod,
                COUNT(CASE WHEN source LIKE '%lidar_roof_rack%' THEN 1 END) AS n_gt
            FROM {source}
            GROUP BY container_id, frame_ts
        )
    """,
)
db.register_derived_channel(lidar_disagreement_pct)

# 4. BasicEvent — disagreement window > 5%, scoped to dual-LiDAR containers,
#    with a 1 s minimum duration to suppress single-frame blips.
lidar_count_mismatch = BasicEvent(
    name="lidar_count_mismatch_5pct",
    expr=db.channel("lidar_count_disagreement_pct") > 0.05,
    container_filter=gt_lidar_containers,
    min_duration_ms=1000,
    desc="Production LiDAR and roof-rack ground truth disagree on detection count by >5%",
)

# 5. SequenceOfEvents — richer pattern: low-confidence cyclist on the production
#    sensor followed within 2 s by a disagreement spike. The training team most
#    wants frames where the production LiDAR likely failed to confirm a cyclist
#    the roof-rack caught cleanly.
cyclist_lowconf_prod = PerceptionEvent(
    name="cyclist_lowconf_prod",
    expr=(ot.detection_class == "cyclist")
         & ot.source.contains("lidar_inboard")
         & (ot.confidence < 0.5),
    container_filter=gt_lidar_containers,
)
db.register_derived_channel(cyclist_lowconf_prod)

cyclist_then_disagreement = SequenceOfEvents(
    name="cyclist_then_lidar_disagreement",
    expressions=[
        db.channel("cyclist_lowconf_prod"),
        db.channel("lidar_count_disagreement_pct") > 0.05,
    ],
    container_filter=gt_lidar_containers,
    max_step_duration_ms=2000,
    desc="Low-confidence cyclist on production LiDAR followed within 2 s by "
         "detection-count disagreement against roof-rack ground truth",
)

# 6. Run.
report.add_event(lidar_count_mismatch)
report.add_event(cyclist_then_disagreement)
report.run()

# 7. Curate the matched windows from both events into a named, versioned playlist.
#    The training team pulls `lidar_training_disagreement_v1` whenever they want
#    the current cut, without re-running the events.
events_df = db.event_instance_fact(spark).filter(
    F.col("event_name").isin([
        "lidar_count_mismatch_5pct",
        "cyclist_then_lidar_disagreement",
    ])
)
event_fact_to_playlist_items(
    events_df,
    playlist_id="lidar_training_disagreement_v1",
    playlist_version=1,
).write.mode("append").saveAsTable(cfg.t_playlist_items)
```

**What each construct is doing.**

- **`ContainerEvent`** scopes the entire query to containers tagged `vehicle_config = "dual_lidar"`. The same `gt_lidar_containers` instance is reused as `container_filter=` on every downstream event — one filter, declared once, pushed into every source scan.
- **`PerceptionEvent`** (registered) carries the per-sensor presence predicates and the low-confidence cyclist predicate. Naming and registering each one via `db.register_derived_channel(...)` turns them into named signals that the disagreement event and the sequence event reference by name.
- **`DerivedChannel`** carries the per-frame disagreement percentage — the only piece that is not an event projection because the value is scalar arithmetic. Goes through the same registration call as events but with a `compute_sql=` (or `compute_fn=`) spec. Same `(name, version, definition_hash)` lineage triple, same idempotence-on-hash-match guarantee, same `db.channel(...)` lookup.
- **`BasicEvent`** composes the registered disagreement signal with the `container_filter` and a `min_duration_ms` debounce to produce the simple "counts disagree" event-instance rows.
- **`SequenceOfEvents`** fires the richer "low-confidence cyclist → disagreement within 2 s" temporal pattern; same `event_instance_fact` output schema as the other events.
- **`playlist_items`** is the gold-layer hand-off — one named, versioned playlist spanning both flavors of disagreement detection. The training team pulls v1 whenever they need the current cut; if the definition tightens, they get v2 without losing v1.

**The lineage trace.** Pull `lidar_training_disagreement_v1` six months later and the chain is recoverable from the catalog: playlist rows reference `event_instance_fact` rows by `event_id`; `event_instance_fact` carries the event definition hash from `event_dimension`; the events reference `derived_channels` by name; `derived_channels_definitions` carries each `(name, version, definition_hash)` plus the compute spec or event-projection origin; and the `container_filter` is part of every parent event's definition hash so the dual-LiDAR scoping is auditable end-to-end. No prose required — the meaning is in the catalog.

## Version pinning for reproducibility

`db.channel("name")` resolves to the currently `active` version of a derived channel. For audit and replay, pin a specific version:

```python
risky_cyclist_v1 = BasicEvent(
    name="risky_cyclist_v1_audit",
    expr=db.channel("cyclist_present_left", version=1)
         & (db.channel("Vehicle_Speed_kph") > 30.0),
    desc="Audit re-run against v1 of the cyclist-present definition",
)
```

The event resolves against the exact `derived_channels` rows that matched the v1 definition, even after v2 has been promoted to `active`. Combined with the immutability of `event_instance_fact` rows, this is the property regulatory homologation and safety-case workflows depend on — the meaning of every label at every point in the lineage is recoverable from the catalog, not inferred from prose.
