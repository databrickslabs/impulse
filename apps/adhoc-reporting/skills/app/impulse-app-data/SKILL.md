---
name: impulse-app-data
description: >
  Choose the silver data, the output location, the units under test and the time window of a report. Use
  when the user names a catalog or schema, asks which data is available, picks vehicles, machines, test
  benches or test drives, or restricts the report to a period. Also explains the shared data model for
  non-standard table layouts.
---

# Data, units under test and time window

## Source and output

1. If the user does not name a schema, call `list_catalogs` and `list_schemas`, and suggest schemas whose
   names suggest measurement data. Ask which one to use.
2. Call `set_data` with `catalog` and `schema`. The output defaults to the same schema with the report
   name as table prefix; pass `output_catalog`, `output_schema` or `table_prefix` to change it.
3. Read the result:
   - `tables`: the Impulse tables found. `container_metrics`, `channel_metrics` and `channels` are required.
   - `data_type`: `RAW` (one row per sample) or `RLE` (one row per stable interval). Detected from the
     `channels` columns.
   - `uuts_by`: `column` (a unit under test column on `container_metrics`) or `tag` (rows in `container_tags`).
   - `issues`: anything missing. A non-empty list blocks the later steps.
4. Changing the source clears the unit under test selection and the time window.

The output schema must exist. The user needs CREATE TABLE there, and the app's service principal is
checked before each run (see `impulse-app-run`).

## When the schema is not Impulse-ready

The app reads tables through one shared data model, edited by an admin on the app's Data model page
(`/Workspace/Shared/Impulse/config.yaml`). It covers table names, the unit under test key and source
(`uut.key`, `uut.source`), the time columns used for the window, the channel identity tags and the
framework's `solver_config` column mappings and filters. When `issues` mention missing tables or columns,
tell the user which names were expected and point them to the Data model page. Framework semantics of the
mapping: `impulse-config` and `impulse-data-model`.

## Units under test

Units under test are the objects the recordings come from, such as vehicles, machines or test benches.
Users often call them by those names.

1. Call `list_uuts`. It returns the `total` number of units under test, how many are `selected`, the
   `window` and up to 50 units under test, each with `containers` (recordings), `first` and `last` recording
   time. `query` finds ids in large fleets; `more` counts the matches not shown. A `total` of 0 means
   `uut.key` names no column or tag with ids; point the user to the Data model page.
2. Call `set_uuts` with `all` true when the user says all or does not restrict the units under test,
   otherwise with ids exactly as listed.
3. The time window `start` and `stop` use the timestamp format of `first` and `last`, such as
   `2026-01-09 00:00:00`. A recording is included when it starts at or after `start` and stops at or before
   `stop`. One window applies to all selected units under test. Without a window all recordings are included.
4. A window outside every unit under test's first and last time selects nothing; check the dates against
   `list_uuts` before setting them.

Channels are listed only for the selected units under test and window, so select them before searching
channels.
