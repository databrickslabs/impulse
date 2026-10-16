---
name: impulse-app-aggregations
description: >
  Choose and configure aggregations: duration and distance histograms, heatmaps, statistics and values
  at points, with sensible bins and scopes. Use when the user asks for a distribution, load spectrum,
  operating point map, histogram, heatmap, statistics, or values at certain moments.
---

# Aggregations

## Choosing the aggregation

| Question | Aggregation |
|---|---|
| How long was the engine in each speed range? | `add_histogram`, weight `duration` |
| How far was driven in each speed range? | `add_histogram`, weight `distance`, with a cumulative distance signal |
| Where are the operating points of RPM against torque or speed? | `add_heatmap` |
| What were min, max, mean, median, first or last values per recording or per event? | `add_statistics` |
| What were the values at each gear change, edge or milestone? | `add_point_values` with a points event |

Mean and median are weighted by duration. Statistics are computed per event instance, or per recording
when the event is empty; results list these values as Impulse writes them.

## Scope

`event` is empty for the whole series (one value per recording) or the name of a condition event, for example a histogram of
RPM only while `rpm_band` holds. `add_point_values` needs a points event. Define events first
(`impulse-app-signals`).

## Bins

Give `bins` as explicit edges, or `start`, `stop` and `step`. Without either, the app fits about 20 bins
to the channel's min and max from `search_channels`; derived signals need explicit bins.

Rules of thumb:

- Cover the observed range from `search_channels`, rounded outward to the step.
- Use 10 to 30 bins with a round step: 1, 2, 2.5 or 5 times a power of ten.
- Start at 0 for quantities that cannot be negative; make signed ranges symmetric around 0.
- Typical steps: engine speed 250 or 500 rpm, vehicle speed 10 km/h, temperatures 5 or 10 °C, pedal and
  throttle 10 %, torque 25 or 50 Nm, lateral acceleration 1 m/s², state of charge 10 %.
- Integer states such as gear: one bin per state, edges at half values, for example `[0.5, 1.5, 2.5, ...]`.
- Heatmaps: 8 to 20 bins per axis; a typical pair is engine speed on x and torque or vehicle speed on y.

Typical ranges per signal family are in `impulse-app-signals`: `read_skill_reference` with file
`references/automotive_domains.md`.

## Names and replacing

Name aggregations after signal and purpose, such as `rpm_hist`, `rpm_vs_speed`, `temp_stats`. Calling an
add tool with an existing name replaces that aggregation. `remove_aggregation` deletes one.

Framework semantics, output tables and parameters: `impulse-aggregations`.
