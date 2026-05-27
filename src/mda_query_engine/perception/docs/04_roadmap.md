# Roadmap

This document covers what is planned beyond the capabilities shipped in this
package. Everything shipped is described in [`01_how_it_works.md`](01_how_it_works.md)
and [`03_authoring_events.md`](03_authoring_events.md). This document focuses
on what comes next.

## What is already shipped

- `PerceptionEvent` — frame-level and per-object (`track_scope=True`) windowing
  over `object_tracks`.
- `PerceptionSolver` — cogroup-based solver delivering `object_tracks` to the
  per-container UDF alongside scalar channels.
- `ObjectTrackAccessor` — TSAL predicate authoring surface with full schema
  enforcement.
- `PerceptionCache` and `PerceptionSelector` — the evaluation layer that runs
  inside the UDF.
- `SequenceOfEvents` with perception expressions as steps.
- `perception_event_instance_objects` side-car for recovering `object_id` from
  track-scoped event instances.
- `PerceptionDB` + `PerceptionDBConfig` — accessor for `object_tracks`,
  `perception_channels`, `perception_event_instance_objects`.
- Schema definitions: `OBJECT_TRACKS`, `PERCEPTION_EVENT_INSTANCE_OBJECTS`,
  `PERCEPTION_CHANNELS`.
- `ObjectTracksConfig` downsampling modes: full-stride and TSAL-gated.

## Planned capabilities

### Named versioned signals (derived channels)

Today, a `PerceptionEvent` produces windows directly. There is no way to give
those windows a name that persists in the catalog and makes them referenceable
from other events by name.

The planned capability is a governance layer that promotes a `PerceptionEvent`
into a named, versioned scalar signal — letting any downstream `BasicEvent`
reference it with `db.channel("cyclist_present_left")`. Defining the same
signal with a different predicate creates a new version automatically rather
than overwriting the old one, so historical event instances stay reproducible
against their original definition.

This is a generic Impulse core capability (labelling and lineage are not
perception-specific), and it lands in Impulse core rather than this package.

### object_tracks_frame_summary materialized table

At fleet scale, `object_tracks` can be expensive to scan when the predicate is
frame-level: scanning one row per object per frame to answer a frame-level
question reads N times more data than necessary, where N is the average number
of objects per frame.

The planned `object_tracks_frame_summary` is a denormalized one-row-per-frame
materialized view over `object_tracks` — pre-aggregating the attributes most
commonly used in frame-level predicates. Queries that do not require per-object
identity can route to this table and run roughly N times faster at fleet scale.
The authoring surface (`PerceptionEvent`, `ObjectTrackAccessor`) is unchanged;
routing is internal to the solver.

### Auto-rewrite PerceptionEvent to BasicEvent

Once `object_tracks_frame_summary` and the derived channel catalog are both
in place, the solver can automatically rewrite a `PerceptionEvent` to a
`BasicEvent` when a matching derived channel already exists. The user authors
the same `PerceptionEvent` expression and the solver picks the cheapest
available execution path — TSAL scan over derived channels is typically
cheaper than scanning `object_tracks`. This is the perception-specific instance
of the Catalyst-style query optimization pattern used by the rest of the solver.

### Source-agnostic per-object temporal sequences

`SequenceOfEvents` currently evaluates temporal patterns over
`event_instance_fact` intervals. The planned extension makes it possible to
express per-object temporal sequences — for example, "a pedestrian moved from
the front-right sector to the front-center sector to the front-left sector
within 4 seconds" — without pre-materializing the per-object windows into a
new event class. The existing sequencing algorithm is extended to accept
per-object interval streams as a source.

### Scene-cut-on-demand frame extraction

Today, frames are either extracted at ingest time or not at all. The planned
capability is a lightweight frame-index table and a triggered extraction job
that runs against a set of event windows, extracting only the frames inside
those windows. This avoids the cost of extracting frames for the entire dataset
when only a small fraction will be used.

## Sequencing rationale

Named versioned signals come first because they close the lineage story and
unlock compound multi-event compositions without post-solve joins. The
frame-summary table is the cost lever for the accounts where dashboard refresh
frequency has become a pain point. The auto-rewriter makes the registration
step transparent over time. Per-object sequences are triggered by demand for
temporal per-object predicates. Scene-cut-on-demand lands as frame extraction
becomes a regular part of labelling workflows.

## Forward guarantees

Every future event type writes into the same `event_instance_fact`. Every
future derived channel reads through the same channel-source interface.
Predicates authored today against `ObjectTrackAccessor` carry forward without
modification as each new capability lands.
