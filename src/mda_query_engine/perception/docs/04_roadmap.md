# LakeVision roadmap

This document covers what is planned beyond the shipped BYOD demo, in what order, and why. The demo is intentionally the minimum that proves the architecture works; everything below extends it without breaking what is already there. We sequence and prioritize by customer demand.

The supported authoring surface today and the authoring walk-throughs live in [`01_how_it_works.md`](01_how_it_works.md) and [`03_authoring_events.md`](03_authoring_events.md). This document focuses on what comes next.

## Contents

- [PerceptionEvent and derived scalar channels are the centerpiece](#perceptionevent-and-derived-scalar-channels-are-the-centerpiece)
- [The solver behaves like a query optimizer](#the-solver-behaves-like-a-query-optimizer)
- [Why derived_channels is the labelling infrastructure ADAS engineers actually want](#why-derived_channels-is-the-labelling-infrastructure-adas-engineers-actually-want)
- [The full sequence of planned capabilities](#the-full-sequence-of-planned-capabilities)
- [Sequencing rationale](#sequencing-rationale)
- [Forward guarantees](#forward-guarantees)

## PerceptionEvent and derived scalar channels are the centerpiece

The two most important planned capabilities are `PerceptionEvent` and `derived_channels`. Together they unlock perception predicates expressed with the same elegance as `BasicEvent`, and they close the lineage story from the opening of `01_how_it_works.md`.

**What `PerceptionEvent` enables that `BasicEvent` alone doesn't.** Some predicates only make sense at the per-object level: per-sensor confidence filters (`source = 'lidar' AND confidence >= 0.9`), lane offset relative to a specific tracked object, transitions of a single object across frames (cut-ins, lane changes, follow-through behaviour). These cannot be reduced to scalar channels without pre-computing every predicate variant at ingest time. `PerceptionEvent` lets you author the predicate at query time, no pre-computation required.

**When to stick with `BasicEvent`.** If the question you are asking is fundamentally about scalars — does vehicle speed exceed 30 kph, has pedestrian count gone above 2, is the ADAS state intervening — author a `BasicEvent` directly. There is no need to involve the perception surface for a scalar-shaped question. `PerceptionEvent` earns its keep when the predicate references attributes that only exist at the per-object level, or when you want the system to pick the cheapest execution path automatically as the derived channel catalog evolves.

## The solver behaves like a query optimizer

Authoring a `PerceptionEvent` is one thing; how it executes is another. The solver examines the predicate tree and picks the cheapest available path, in the same way Spark's Catalyst rewrites a SQL query to take advantage of indexes and predicate pushdown:

- If every atom in the predicate has a matching `derived_scalar_channel` and the predicate does not reference object identity, rewrite to a `BasicEvent` and ride the existing TSAL batch-solve. Zero perception-surface scan cost.
- Else, if `object_tracks_frame_summary` (a denormalized one-row-per-frame table on the roadmap) is available and the predicate is frame-level, route there. Roughly N times cheaper than `object_tracks` at fleet scale, where N is the average number of objects per frame.
- Else, scan `object_tracks` directly.

You write the same `PerceptionEvent` either way. Routing is internal. As derived channel registrations land in your catalog, the same code gets cheaper with no changes on your side.

## Why `derived_channels` is the labelling infrastructure ADAS engineers actually want

Engineers describe what their system does with labels: "highway traffic jam," "stopped at a red light," "cut-in from the left lane," "school zone approach." A label is a human-readable name for a condition that holds over a time window. State machines, rule engines, and trigger logic — the building blocks of any ADAS validation stack — all output labels: a signal that is active when the conditions match and inactive otherwise.

In practice these labels live as code. Python functions in V&V notebooks, SQL views in dashboards, business rules inside scenario-mining tools, hand-tuned thresholds in MATLAB scripts. Each team ends up with its own version of "highway traffic jam." Definitions drift: someone tightens a confidence threshold, someone widens a distance band, someone uses `lane_offset` where another uses `azimuth`. Six months later nobody can reproduce "the set of recordings where highway traffic jam was active" because the meaning of the label has shifted in three different places. The "set of all detected cut-ins" your safety case is built on becomes a forensic question, not a query.

`derived_channels` turns these labels into governed, versioned data assets. It lands in Impulse Core (the labelling problem is universal across time-series engineering, not perception-specific). Each label becomes a named scalar signal alongside `channels`. A definitions table records the predicate, the confidence thresholds, and a hash of the definition, so any change to the meaning of a label creates a new version automatically rather than silently overwriting v1:

```
name                  | version | predicate_str                                                            | active
highway_traffic_jam   | 1       | road_type=='highway' & vehicle_speed_kph<20 & lead_vehicle_distance_m<30 | false
highway_traffic_jam   | 2       | road_type=='highway' & vehicle_speed_kph<15 & lead_vehicle_distance_m<40 | true
stopped_at_red_light  | 1       | traffic_light_ahead=='red' & vehicle_speed_kph<2                         | true
cyclist_front_left    | 1       | detection_class=='cyclist' & azimuth=='front_left'                       | true
```

Pull `highway_traffic_jam` version 1 six months later and you get exactly the windows that matched v1's definition, even after v2 supersedes it as the active version. This is the property regulatory homologation requires. It is also what turns scenario labels from a maintenance liability into a queryable, comparable asset across teams and programs — V&V, perception, planning, and ML can all reference the same label by name without each team forking a slightly different definition.

A label that depends on per-object data — like `cyclist_front_left` above, computed from `object_tracks` — is registered once via `db.register_derived_channel(...)` and then participates in TSAL like any other scalar channel. Compound predicates that today require two events and a SQL join collapse into a single `BasicEvent` over the registered labels — both the channels-plus-perception case ("cyclist on the left while vehicle speed exceeds 30 kph") and the multi-object same-frame case ("a cyclist and a pedestrian both visible on the left at the same time"). The exploration/production split for those compound predicates is described in [`03_authoring_events.md`](03_authoring_events.md).

## The full sequence of planned capabilities

| Capability | What we build | What you can search for | What stays the same |
|---|---|---|---|
| Native perception predicates with the same elegance as `BasicEvent` | `PerceptionEvent` class (LakeVision) plus `db.register_derived_channel` helper plus `derived_channels` (Impulse Core) | "An animal crossed the road from the left at high relative speed." "A cyclist appeared on the front-left with low LiDAR confidence." Per-object predicates authored at query time, no pre-computation required. | `event_instance_fact` is unchanged; all downstream tooling (playlists, OpenLABEL, KPI) keeps working |
| Per-object scenarios (cut-ins, lane changes, follow-through) | `track_scope=True` on `PerceptionEvent`, plus a nullable `object_id` column on `event_instance_fact` | "A vehicle cut into your lane within 2 seconds." "An object held station on the front-right for at least 3 seconds." Windows form per object, with a debounce for short detections. | Same predicate grammar; one extra flag |
| Roughly N times cheaper frame-level perception queries at fleet scale (N is the average number of objects per frame, typically 8–15) | `object_tracks_frame_summary` materialized table plus solver routing | The same `PerceptionEvent` queries you already wrote, running roughly N times faster on frame-level predicates. No syntactic change. | The user writes the same `PerceptionEvent`; routing is internal |
| Compound predicates without authoring `db.register_derived_channel` calls | Auto-rewrite a `PerceptionEvent` to TSAL when a matching derived scalar channel already exists | "A cyclist was on the left while vehicle speed exceeded 30 kph." Authored once as a compound predicate; the solver rewrites to TSAL automatically when a matching derived scalar is registered. | Manual registration keeps working as the fallback |
| Temporal sequences per object | `SequenceOfEvents` extended to be source-agnostic, so it can run over `object_tracks` outputs as well as `event_instance_fact` events | "A pedestrian moved from the front-right to the front-center to the front-left within 4 seconds." Per-object state transitions over time, not just per-frame presence. | One event class, not a new one — the existing sequencing algorithm absorbs the perception-source case |
| Ad-hoc compound predicates without registering a `derived_scalar_channel` (exploration mode) | `perception_signal(...)` helper plus an ephemeral source kind in the `ChannelSource` registry | "An animal crossed the road from the left at high relative speed while a car was coming the other way in the opposite lane." "A cyclist and a pedestrian were both visible on the left at the same time." Authored inline in a `BasicEvent`, no `derived_channels` row registered. Promote to a registered derived channel when the predicate stabilises. | `BasicEvent` stays the single compound-event class; the production path (`db.register_derived_channel` + `BasicEvent`) keeps working unchanged |
| Scene-cut-on-demand frame extraction | `frame_index` table plus a Lakeflow Job triggered from `playlist_items` inserts | "The camera frames inside every cut-in we just detected" — without extracting frames for everything else upfront. The opt-in `extract_all_frames` campaign flag covers the rare "all frames" path. | Today: ingest does not extract; `perception_channels` indexes whatever was extracted out of band |

## Sequencing rationale

`PerceptionEvent` ships first because it unlocks the customer-facing story and lays the registry pattern that every later capability reuses. Per-object windowing comes next because cut-in and lane-change scenarios come up most often after pedestrian proximity. The frame-summary materialized table is the cost-saving lever once dashboard refresh frequency starts mattering. The auto-rewriter makes the registration step transparent. `perception_signal(...)` lands alongside `db.register_derived_channel` so the exploration and production paths arrive together. Per-object sequences are a roadmap item rather than initial scope — the trigger is a customer needing temporal predicates per object.

## Forward guarantees

One guarantee holds across every roadmap item. Every future event type writes into the same `event_instance_fact`. Every derived scalar channel reads through the same `ChannelSource` interface on `MeasurementDB`. Every registered derived channel is versioned with `(name, version, definition_hash)` so historical events stay reproducible. The investments customers make against today's demo — BYOD adapters, TSAL queries, playlist definitions, OpenLABEL pipelines — carry forward without modification as each new capability lands.
