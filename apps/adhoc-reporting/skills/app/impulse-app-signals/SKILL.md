---
name: impulse-app-signals
description: >
  Find channels, add them as signals, derive new signals and define events. Use when the user describes
  signals in words (English or German), asks for a derived signal such as an average, a difference or
  distance from speed, or wants to analyze only when a condition holds or at specific instants.
---

# Signals and events

## Finding channels

1. Translate the user's words into search terms. `references/automotive_domains.md` maps English and
   German engineering terms to typical channel names, units and ranges, for example Drehzahl to `nmot`,
   `eng_spd` or `Engine RPM`.
2. Call `search_channels` with one or more comma-separated terms, for example `rpm, drehzahl, nmot`.
   Each match has its identity (`channel`), `unit`, the number of `recordings` that contain it, and `min`
   and `max` over those recordings.
3. Prefer channels present in all recordings, with a matching unit and a plausible range. When several
   channels fit equally, ask the user which one to use and list at most five.
4. Call `add_channel_signals` with the channel names as returned. Give short signal names such as
   `eng_rpm` or `veh_spd`; otherwise they are derived from the channel name.

Never add a channel that `search_channels` did not return. Channels are added as `db.query.channel(...)`
sample signals; the app does not support `poi_channel()` (points-in-time channels such as DTC codes).

## Derived signals

`add_derived_signal` takes an expression over existing signal names that evaluates to a signal:

| Goal | Expression |
|---|---|
| Average of two signals | `(amb_temp + intake_temp) / 2` |
| Difference | `intake_temp - amb_temp` |
| Unit conversion | `veh_spd / 3.6` |
| Power from torque and speed | `eng_trq * eng_rpm * 2 * 3.14159 / 60 / 1000` |
| Cumulative distance in km from speed in km/h | `veh_spd.resample(1e6).cumtrapz() / 3600 / 1e6` |
| Smoothing | `eng_rpm.rolling_average(5e6)` |
| Signal only inside a condition | `eng_rpm.where(veh_spd > 0)` |

Allowed: numbers, signal names, `+ - * / %`, comparisons, `&` and `|`, parentheses, and the methods
`resample`, `cumtrapz`, `rolling_average`, `sparse`, `diff`, `where`, `rising_edges`, `falling_edges`,
`intervals_between_falling_edges`; on conditions `filter`, `debounce`, `expand`, `expand_left`,
`expand_right`, `shrink`, `shrink_left`, `shrink_right`, `merge_overlaps`, `merge_intervals`,
`start_points`, `end_points`; on points `expand`, `expand_left`, `expand_right`.
Not allowed: `and`, `or`, `not`, chained comparisons such as `a < x < b` (write `(x > a) & (x < b)`), a
minus sign in front of a signal (write `0 - x`), strings, functions other than these methods. Result types and their rules: `impulse-tsal`.

Times use the data's time unit, which the system prompt states. The examples here assume microseconds,
the default; scale them for other units. `resample(1e6)` is one sample per second, `filter(2e6)` drops
windows shorter than 2 s, `expand(10e6)` widens by 10 s on each side.

## Events

`add_event` with:

- kind `condition`: an expression that gives time windows. Examples: `eng_rpm > 3000`,
  `(eng_rpm > 2000) & (eng_rpm < 5000)`, `(veh_spd > 0) & (eng_rpm < 900)`, `(cool_temp > 100).filter(5e6)`,
  `(distance_km % 10).intervals_between_falling_edges()` for every 10 km.
- kind `points`: an expression that gives instants. Examples: `veh_spd.rising_edges()`,
  `(eng_rpm > 4000).start_points()`, `(distance_km % 10).falling_edges()` for each 10 km mark.

Use a condition event to scope histograms, heatmaps and statistics; use a points event only for values at
points. Aggregations without an event cover the whole series of each recording. Event semantics: `impulse-events`.

Check the result of each add: the `issues` list names expression errors, such as a points expression
used for a condition event. Fix and call the tool again with the same name to replace the definition.
