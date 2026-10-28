---
name: impulse-app-run
description: >
  Validate, preview, run and monitor a report, explain failures and read its results. Use when the user
  asks whether the report is complete, wants to see the code, run the report, check a run, understand an
  error, or interpret histograms and statistics after a run.
---

# Validate, run and read results

## Validate and preview

`validate_report` checks the whole report against the data and the framework rules and returns issues per
step. Fix them with the tools of that step; `ready` true means the report can run. `preview_code` returns
the generated Impulse notebook; show the relevant part only when the user asks for code.

## Run

Call `run_report` only when the user asks. It first checks access:

- The user needs SELECT on every source table and CREATE TABLE in the output schema. Missing privileges are
  listed under `preflight.user`; the user asks a data owner for them.
- Report jobs run as the app's service principal. Missing privileges are listed under `preflight.app`, and
  `preflight.grants` holds the GRANT statements an admin runs once. Show them unchanged.

When it starts, the run takes one to three minutes on serverless compute. Tell the user the app shows
progress on the Review & run step; call `get_run_status` when the user asks how it is going.
Each run recomputes the selected recordings and overwrites the report's gold tables (Impulse full mode).

## Failures

`get_run_status` returns `error_message` and the end of `trace` for a failed run. Common causes:

| Message contains | Cause | Fix |
|---|---|---|
| `requires an expression that evaluates to` | An event or aggregation got the wrong kind of expression | Fix the event kind or expression |
| `TABLE_OR_VIEW_NOT_FOUND` | A table in the data model does not exist | Fix the data or the data model |
| `INSUFFICIENT_PERMISSIONS`, `PERMISSION_DENIED` | The service principal lost access | Show the grants from a new `run_report` attempt |
| `measurement_dimensions` | A configured dimension column is missing in `container_metrics` | Fix the data model |
| A run that succeeds with empty results | No recording matched, or the channel is missing in the selected recordings | Check units under test, window and `search_channels` coverage |

Framework behavior during a run: `impulse-reporting`.

## Results

`get_results` summarizes the gold tables of the latest run:

- `events`: instances, recordings and total time in seconds per condition event.
- Histograms: `[lower, upper, value]` per bin; duration values are seconds, distance values use the
  distance signal's unit.
- Heatmaps: the total and the top cells as `[x_lower, x_upper, y_lower, y_upper, seconds]`.
- Statistics and values at points: `columns` and `rows`, one row per recording or event instance (with
  `instance`, numbered in time order per recording) as in the gold table; `event` is null for the whole series.

Explain results in the user's terms, for example "the engine spent 89 % of the band time between 2000 and
2500 rpm". Results reflect the last successful run; after changing the report, run it again.
`run_sql` can query the gold tables directly; names follow `<catalog>.<schema>.<prefix>_<table>`, for
example `..._histogram_fact`, joined to `..._histogram_dimension` on `visual_id` (`impulse-data-model`).
