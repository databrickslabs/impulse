# Perception — Object-Track Querying for Impulse

This package extends Impulse core with object-track-based scenario search.
It adds a TSAL authoring surface over the `object_tracks` table, a solver
that cogroups scalar channels with per-object data, and the event and schema
types needed to find, window, and record perception-level scenarios.

> **How it works:** [`docs/01_how_it_works.md`](docs/01_how_it_works.md) —
> the data model, the event vocabulary, and how `PerceptionEvent` and
> `PerceptionSolver` work together. Start here.
>
> **Authoring events:** [`docs/03_authoring_events.md`](docs/03_authoring_events.md) —
> how to write `PerceptionEvent` predicates against your data, compose them
> with `BasicEvent` scalar conditions, and use `SequenceOfEvents` with
> perception inputs.
>
> **Roadmap:** [`docs/04_roadmap.md`](docs/04_roadmap.md) — what is planned
> beyond the shipped capabilities and why.

## Quick start

```python
from mda_query_engine.perception.perception_db import PerceptionDB, PerceptionDBConfig
from mda_query_engine.perception.events.perception_event import PerceptionEvent

cfg = PerceptionDBConfig.for_unity_catalog("my_catalog")
db  = PerceptionDB(cfg)

ot = db.query.object_track   # ObjectTrackAccessor proxy

cyclist_approaching = PerceptionEvent(
    name="cyclist_front_left_approaching",
    expr=(ot.detection_class("cyclist"))
         & (ot.azimuth("front_left"))
         & (ot.relative_velocity_ms < -3.0),
    desc="Cyclist approaching from the front-left at >3 m/s closing speed",
)
report.add_event(cyclist_approaching)
report.run()
```

## Modules

| Module                 | Provides                                                                                                    |
| ---------------------- | ----------------------------------------------------------------------------------------------------------- |
| `perception_db`        | `PerceptionDB` + `PerceptionDBConfig` — accessor for `object_tracks`, `perception_channels`, `perception_event_instance_objects` |
| `object_tracks_config` | `ObjectTracksConfig` — controls full-stride vs TSAL-gated downsampling at ingest                            |
| `schema/`              | Spark `StructType` definitions: `OBJECT_TRACKS`, `PERCEPTION_EVENT_INSTANCE_OBJECTS`, `PERCEPTION_CHANNELS` |
| `tsal/`                | `ObjectTrackAccessor`, `PerceptionSelector`, `PerceptionCache` — predicate authoring surface                |
| `events/`              | `PerceptionEvent` — event class over `object_tracks`, including `track_scope=True` per-object windowing     |
| `query/`               | `PerceptionSolver` — cogroup-based solver that delivers `object_tracks` to the per-container UDF            |

## Design principles

- **TSAL finds the event.** Do not run scenario search over raw geometry
  tables. Author predicates over `object_tracks` using `ObjectTrackAccessor`
  and let the solver evaluate them at query time.
- **Binary media is never stored in Delta columns.** Raw frames and point
  clouds live in Unity Catalog Volumes; `perception_channels` holds file-path
  pointers and is the join key for frame-level work.
- **Ingest at native fidelity; downsample at materialisation time per workload.**
  `ObjectTracksConfig` offers full-stride (default) and TSAL-gated modes.
  Choose based on whether you need discovery flexibility or storage efficiency.
## Relationship to Impulse core

```
Impulse core (src/mda_query_engine, src/mda_reporting)
  └── provides: channels, channel_tags, container_tags, TSAL, event_instance_fact

Perception (src/mda_query_engine/perception/)
  └── requires: Impulse core (container_id, TSAL events, event_instance_fact)
  └── adds: object_tracks, perception_channels,
             perception_event_instance_objects,
             ObjectTrackAccessor, PerceptionEvent, PerceptionSolver
```

`PerceptionDB` sits alongside `MeasurementDB` — it does not replace it.
For recordings that also have scalar channels, the two accessors share the
same `container_id` namespace and the same `event_instance_fact` output.
