---
name: impulse-app
description: >
  Entry point for operating the Impulse app. Load it at the start of any report task: the report
  lifecycle, which tool does what, the supported framework subset, naming rules and guardrails.
---

# Impulse app: operating guide

The app turns a report spec into one Impulse notebook (framework version pinned by the app), runs it as a
serverless job and reads the gold tables back. The user edits the same report in the UI. Every tool reads
the latest saved report and saves its change at once, so the UI shows each change immediately.

## Lifecycle

| Step | Tools | Skill |
|---|---|---|
| 1. Data | `set_data`, `list_catalogs`, `list_schemas` | `impulse-app-data` |
| 2. Units under test | `list_uuts`, `set_uuts` | `impulse-app-data` |
| 3. Signals and events | `search_channels`, `add_channel_signals`, `add_derived_signal`, `add_event`, `remove_signal`, `remove_event` | `impulse-app-signals` |
| 4. Aggregations | `add_histogram`, `add_heatmap`, `add_statistics`, `add_point_values`, `remove_aggregation` | `impulse-app-aggregations` |
| 5. Check | `validate_report`, `preview_code` | `impulse-app-run` |
| 6. Run and results | `run_report`, `get_run_status`, `get_results` | `impulse-app-run` |

Other tools: `list_reports`, `create_report`, `open_report`, `get_report`, `goto_step`, `run_sql`,
`load_skill`, `read_skill_reference`.

Follow the steps in order. Each step needs the previous one: channels are listed for the selected
units under test, and aggregations refer to signals and events by name.

## Supported subset

| Need | Use |
|---|---|
| Time windows where a condition holds | `add_event` with kind `condition` (framework `BasicEvent`) |
| Instants, such as rising edges | `add_event` with kind `points` (framework `PointsInTimeEvent`) |
| The whole recording or series | leave the aggregation's event empty (the framework's whole-series scope) |
| Time spent per value range | `add_histogram` with weight `duration` |
| Distance travelled per value range | `add_histogram` with weight `distance` and a cumulative distance signal |
| Time spent per combination of two signals | `add_heatmap` |
| min, max, mean, median, start, end per recording or event instance | `add_statistics` |
| Signal values at points in time | `add_point_values` with a `points` event |

Not supported: sequences of events, calculated channels, custom statistics, points-in-time (POI)
channels, alias channels, incremental runs and schedules. Decline them and offer the closest supported
alternative, for example two condition events instead of a sequence, or a derived signal instead of a
calculated channel.

## Names

- Report, event and aggregation names: lowercase letters, digits and underscores, starting with a letter.
- Signal names follow the same rule and are used in expressions. These are reserved: `report`, `db`,
  `page`, `config`, `sink`, `table`, `tables`, `spark`, `dbutils`, `display`, Python keywords, and names
  starting with `ev_`.
- Choose short, meaningful names such as `eng_rpm`, `veh_spd`, `rpm_band`, `rpm_hist`.

## Guardrails

- Use names only from tool results: catalogs, schemas, units under test and channels are never guessed.
- Ask one short question when several channels could match the user's words; otherwise act.
- After a batch of changes call `validate_report`; fix every issue you caused before you reply.
- When a tool returns an `error`, read it, fix the cause and retry once. If it fails again, tell the user
  the exact message.
- Call `run_report` only when the user asks to run. A run starts a serverless job.
- `run_sql` is read-only and returns at most 50 rows; use it to inspect data, never to change it.
- Keep replies short: what changed, what to check, what comes next.

## Framework semantics

For the meaning of framework objects load the framework skills: `impulse-tsal` for expressions and
result types, `impulse-events` for events, `impulse-aggregations` for aggregations, `impulse-config` and
`impulse-data-model` for tables and configuration, `impulse-reporting` for the run lifecycle.
