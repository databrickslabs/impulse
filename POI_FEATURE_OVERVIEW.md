# POI support in impulse — feature overview

High-level summary of the POI (point-of-interest) capability: how the POI table
is laid out, the report-config knob that preselects `poi_type`s, and the ways POI
can be queried — as a **container metric filter**, as a **filter/gate on
channels**, and as its **own signal** (a numeric or categorical
`PointsInTimeSeries`).

POI is a *wide* occurrence log: **one row per occurrence in time**, N rows per
container. Everything below is built on that single table.

---

## 1. POI layout

### Schema (test fixture `tests/unit/data/poi_integration_csv/poi.csv`)

| column          | type            | meaning                                              |
|-----------------|-----------------|------------------------------------------------------|
| `container_id`  | string/int      | the recording session this occurrence belongs to     |
| `poi_type`      | string          | the kind of POI (`charging_error`, `defect`, `dtc`, …)|
| `timestamp_abs` | timestamp       | when the occurrence happened (the instant)            |
| `value`         | **string (VARCHAR)** | the value at that instant — see note below       |
| `network`       | string          | context column (e.g. bus network `FD3`/`INFO`/`CAN`)  |
| `occurrences`   | int             | source occurrence count (NOT used; POI counts rows)   |

> **Value-type note:** the POI `value` column is a single **VARCHAR** — every
> value is stored as a string (numeric ones are string-encoded, e.g. `"108.0"`;
> categorical ones are codes, e.g. `"P0420"`). How a POI *channel* interprets it
> is chosen per selection via `dtype` (§4): `dtype="double"` (default) parses the
> string to a number → `PointsInTimeSeries`; `dtype="string"` keeps it categorical
> → `PointsInTimeSeriesString`. Numeric parse of a non-numeric value fails loudly
> (no silent NaN). The container filter (§3) always treats values as strings.

### Sample rows

```csv
container_id,poi_type,timestamp_abs,value,network,occurrences
1,charging_error,1970-01-01T00:00:05.000+00:00,0.0,FD3,1
1,charging_error,1970-01-01T00:00:15.000+00:00,1.0,FD3,1
1,charging_error,1970-01-01T00:00:25.000+00:00,0.0,FD3,1
1,defect,1970-01-01T00:00:12.000+00:00,108.0,FD3,1
1,defect,1970-01-01T00:00:15.000+00:00,999.0,CAN,1
2,charging_error,1970-01-01T00:00:25.000+00:00,1.0,FD3,1
2,defect,1970-01-01T00:00:08.000+00:00,200.0,INFO,1
3,charging_error,1970-01-01T00:00:07.000+00:00,1.0,FD3,1
1,dtc,1970-01-01T00:00:15.000+00:00,P0420,FD3,1
2,dtc,1970-01-01T00:00:25.000+00:00,P0171,FD3,1
```

Reading the sample: container **1** has three `charging_error` instants (ok@5,
err@15, ok@25), two `defect`s (@12 FD3, @15 CAN) and a categorical `dtc` `P0420`
@15; container **3** has only a single `charging_error` (err@7) and **no channel
data at all** — the "POI-only container" case that the container-scope wiring
keeps alive. (The numeric `charging_error`/`defect` values are string-encoded;
`dtc` codes like `P0420` are genuinely categorical.)

---

## 2. Report config: preselecting `poi_type`s

Two settings turn POI on, at two levels:

**a) Point the DB at the POI table** (on the measurement-DB config). In debug
mode this is inferred from a table named `poi`:

```python
cfg.poi_table = "poi"          # enables POI reads; None = POI disabled
```

**b) Preselect the `poi_type`s** for the container filter, via `PoiConfig` on the
`SolverConfig` (`solver_config.py`):

```python
from impulse_query_engine.analyze.query.solvers.solver_config import (
    SolverConfig, PoiConfig,
)

config = SolverConfig(
    poi=PoiConfig(
        poi_types=["defect", "charging_error"],  # ← the preselection
        # column names (defaults shown) — after column_name_mapping:
        ts_column="timestamp_abs",
        value_column="value",
        poi_type_column="poi_type",
        # column_name_mapping={...}  # if the physical POI table uses other names
    )
)
```

`PoiConfig` extends `TableConfig`, so it also carries `column_name_mapping`
(physical → internal names) and `filters`, exactly like every other table.

**What `poi_types` controls:** it is the list of types the **container filter**
rolls up. For each entry `<t>`, the rollup emits a **pair of columns** at
container grain:

- `poi_<t>_values` — `array<string>`, the distinct value set for that type
- `poi_<t>_count` — `long`, the **`COUNT(*)`** of that type's rows (occurrences
  are counted from rows, not read from the `occurrences` column)

So `poi_types=["defect"]` produces `poi_defect_values` + `poi_defect_count`.
Containers with none of a configured type get an **empty set + count 0** (never
null). `poi_types=[]` produces no container-filter columns.

> The `poi_channel` *signal* (§4) does **not** require the type to be in
> `poi_types` — that list is only for the container-filter rollup. A
> `poi_channel("charging_error")` reads its type directly regardless.

---

## 3. POI as a container metric filter

Once `poi_types` is set, the rollup columns behave like any other container
metric, so they compose with the normal `q.metric(...)` DSL in `.where(...)`.
They run in the **container-metrics filter stage** (report-wide, prunes the
container set).

### Syntax

```python
q = db.query

# threshold on the occurrence count
q.where(q.metric("poi_defect_count") >= 5)

# membership in the value set (dual-backend array_contains)
q.where(q.metric("poi_defect_values").contains("108"))

# composes with ordinary metrics
q.where(
    (q.metric("poi_defect_count") >= 1)
    & (q.metric("duration_ms") > 30)
)
```

- `poi_<type>_count` → scalar `long`; use `>= / > / == …`.
- `poi_<type>_values` → `array<string>`; use `.contains(value)`.
- These filter **which containers survive** — a report-wide gate, evaluated at
  container grain. No `poi_channel` selection is needed to use them.

---

## 4. POI as a channel filter, and as its own signal

Here POI is read as a **channel**: one `poi_type` → a `PointsInTimeSeries` (a
value at each instant). This is a *selection*, scoped to the surviving-container
set, so it emits even for containers with no measured channel (and honors
incremental batches).

Create the selector with `q.poi_channel(...)`, optionally choosing how the value
is interpreted (see §4e):

```python
charging = q.poi_channel(channel_name="charging_error")                # numeric (default)
dtc      = q.poi_channel(channel_name="dtc", dtype="string")           # categorical
```

### 4a. As its own signal (select POI directly)

```python
# numeric POI series: array<array<double>> of [ts, value] pairs
q.select(charging.alias("charging"))
# container 1 → [[5,0],[15,1],[25,0]], container 3 (POI-only) → [[7,1]]

# project to just the instants matching a value → PointsInTime (array<double>)
q.select((charging == 1.0).alias("error_points"))
# container 1 → [15.0]
```

### 4b. As a filter/gate on a measured channel

`channel.where(poi_condition)` samples the channel **at the POI instants** that
satisfy the condition:

```python
rpm = q.channel(channel_name="Engine RPM")
charging = q.poi_channel(channel_name="charging_error")

# sample RPM at every charging_error == err instant
q.select(rpm.where(charging == 1.0).alias("rpm_at_error"))
# container 1: err@15 → RPM 3000 → [[15.0, 3000.0]]
```

Aliased channels work the same way — the alias resolves, then POI gates it:

```python
engine_speed = q.channel_with_alias(channel_alias="engine_speed")
q.select(engine_speed.where(charging == 1.0).alias("engine_speed_at_error"))
```

### 4c. As a gate on a *virtual* (derived) signal

The gated thing can be a derived signal (a `TimeSeriesOp` built from arithmetic
on channels), not just a persisted channel — the POI selector is found by
recursing into the op tree:

```python
rpm   = q.channel(channel_name="Engine RPM")
speed = q.channel(channel_name="Vehicle Speed Sensor")
charging = q.poi_channel(channel_name="charging_error")

virtual = rpm * speed                       # derived signal, never persisted
q.select(virtual.where(charging == 1.0).alias("power_at_error"))
# container 1: err@15 → 3000 * 60 = [[15.0, 180000.0]]
```

### 4d. In aggregations

POI instants can drive aggregations that sample or summarize channels:

```python
# sample RPM at every charging_error instant
PointValueAggregator(
    input_expressions=[rpm],
    event_expression=charging.to_points_in_time(),
)

# stats over a POI-gated channel
StatsAggregator(
    input_expressions=[rpm.where(charging == 1.0)],
    statistics=["min", "max", "mean"],
)
```

### 4e. Value interpretation: numeric vs categorical (`dtype`)

The source `value` is always a VARCHAR (§1). A POI channel's `dtype` decides how
that string is interpreted into a series:

```python
# numeric (default): value parsed to double → PointsInTimeSeries
soc = q.poi_channel("charging_error")                 # dtype="double" implicit

# categorical: value kept as a string → PointsInTimeSeriesString
dtc = q.poi_channel("dtc", dtype="string")
```

- **`dtype="double"` (default)** → `PointsInTimeSeries`, values are `float64`.
  Supports the full numeric surface (`==/>/<`, arithmetic, `mean`/`min`/`max`,
  aggregations). A value that can't be parsed as a number **raises** (no silent
  NaN).
- **`dtype="string"`** → `PointsInTimeSeriesString`, values are strings.
  Supports **equality only** — `== "P0420"` / `!= "P0420"` return a
  `PointsInTime` of matching instants (usable as a gate, exactly like numeric).
  Ordering, arithmetic and numeric reductions **raise `TypeError`** — "the mean
  of defect codes" is undefined.

```python
# categorical series: struct<tstarts: array<double>, values: array<string>>
q.select(dtc.alias("dtc"))
# container 1 → tstarts=[15.0], values=["P0420"]

# gate a measured channel by a categorical code (value type irrelevant to the gate)
q.select(rpm.where(dtc == "P0420").alias("rpm_at_p0420"))
# container 1 → [[15.0, 3000.0]]
```

**Aggregator guard:** passing a `dtype="string"` channel as a value-carrying
*input* to `StatsAggregator` / `PointValueAggregator` raises a clear `TypeError`
at `.solve()` (plan time, before any Spark job). A string channel is only valid
as a *gate/event* (`channel.where(poi == "CODE")` or `poi.to_points_in_time()`),
not as something to average. Numeric aggregation over categories is undefined.

**Internal detail:** POI values ride an internal `poi_value_str` frame column
(the numeric `value` column is null for POI rows), and `dtype` is applied at
blob-load time. This is invisible to the query API — it only matters if you read
the solve frame directly.

### Solver requirement

POI channels require `solver=DefaultSolver(spark, config=...)`. The default
`BlobSolver` does not implement the POI channel path (limitation #7).

---

## 5. Where the filtering happens: `_run_filter_pipeline`

`.solve()` runs one shared metadata-filter chain,
`QueryBuilder._run_filter_pipeline`, before the per-container solve step. The two
POI features enter at **different points** in that flow — this is the part worth
internalizing.

```
QueryBuilder.solve(spark, DefaultSolver(config))
│
├── _run_filter_pipeline  ──────────────────────────────────────────────┐
│     stage 1  filter_container_tags     → tags_df      (per container)   │
│     stage 2  filter_container_metrics  → metrics_df   (per container)   │
│               └── POI CONTAINER FILTER runs here (§3): the rollup is    │
│                   left-joined on, then q.metric("poi_..") in .where()   │
│     stage 3  filter_channel_tags       → EAV mode: match channels; wide mode: pass-through
│     stage 4  filter_channel_metrics    → channel_metrics_df (per channel; the prune lands here in wide mode)
│     stage 5  (alias resolution, optional)
│     returns  (channel_metrics_df, metrics_df)   ◄── 2-tuple
│                          │              │
├── solver.solve(..., container_scope_df = metrics_df) ◄──────────────────┘
│     _build_solve_frame:
│        join channels ← channel_metrics_df   (inner join on (cid, chid))
│        POI CHANNEL runs here (§4): read poi, PoiTransformer.to_channel_rows
│           scoped to container_scope_df (= metrics_df), unioned in
│        grouped-map UDF per container → PoiChannelSelector.build → series
```

**Two different insertion points:**

- **POI container filter (§3)** runs *inside* the pipeline, at **stage 2
  (`filter_container_metrics`)**. The rollup columns are left-joined onto the
  container-metrics frame, so `q.metric("poi_defect_count") >= 5` is evaluated by
  the same `.where()` as every other metric. It **prunes the container set** —
  it is a genuine filter, at container grain, report-wide.

- **POI channel (§4)** is **not** filtered in the pipeline at all. POI channel
  selectors deliberately return `[]` from the normal selector walk
  (`get_selectors()`), so they never enter stages 3–4 (which resolve against the
  `channel_tags` / `channel_metrics` tables POI isn't in). Instead they are
  collected by a separate walker and read+unioned in `_build_solve_frame` during
  the solve step. A POI channel is a **selection**, not a filter — it doesn't
  prune anything.

### Why the pipeline returns a 2-tuple — the container scope

The critical transition in the pipeline is between stage 2 and the channel
stages:

- After **stage 2**, `metrics_df` holds the **FULL surviving container set**
  (tags ∩ container-metrics), **including containers that have no channels**.
- The **channel stages** match selectors against channels and reshape to
  per-channel rows. A container with **no matching channel produces no rows** →
  its `container_id` **disappears** from `channel_metrics_df`. *Which* stage does
  the drop depends on the layout: in **EAV mode** (`channel_tags_table` set) it's
  stage 3 (`filter_channel_tags`); in **wide mode** (`channel_tags_table` None —
  what the POI fixtures use) stage 3 is a pass-through and the drop lands in
  stage 4 (`filter_channel_metrics`). Either way, a POI-only query has **no
  direct selectors** (POI opts out of `get_selectors()`), so the channel-match
  frame comes back **empty**.

So `channel_metrics_df` alone is the *pruned* set (containers-with-a-channel).
If the POI channel read were scoped to that, then:

- a **POI-only report** (only a `poi_channel` is selected) → no measured channel
  → `channel_metrics_df` empty → **every** container drops → no POI at all;
- a **POI-only container** (POI but no selected measured channel, e.g. container
  3 in the fixture) → not in `channel_metrics_df` → its POI is dropped.

That is exactly the "POI must survive even if no channel is selected"
requirement, and it is why `_run_filter_pipeline` returns **both** frames:

```python
return channel_metrics_df, metrics_df   # match frame + FULL container scope
```

`solve` threads `metrics_df` in as **`container_scope_df`**, and
`_build_solve_frame` scopes the POI read to it (not to the pruned
`channel_metrics_df`). The POI rows are still **unioned after** the channels
inner-join (POI's synthetic negative `channel_id` has no row in the physical
`channels` table, so a join before the union would drop them) — only the
*container scope* moved upstream to the full set. Net effect: **a POI channel
resolves for every container that survived the container-grain filters, whether
or not it has a measured channel.**

This same scope is what makes POI honor **incremental processing**: a
`pre_filtered_containers_df` batch flows into `metrics_df`, so the POI read is
scoped to exactly the batch's containers (no out-of-batch POI leaks in).

`solve_calculated_channels` also calls `_run_filter_pipeline`, so it unpacks and
**discards** the scope (`channel_metrics_df, _ = ...`) — calculated channels
never carry POI, but the tuple must still be unpacked.

---

## Container-filter vs channel: the key distinction

| aspect        | container metric filter (§3)              | POI channel (§4)                                   |
|---------------|-------------------------------------------|----------------------------------------------------|
| grain         | rolled up to 1 row / container            | 1 point / occurrence (numeric or string `PointsInTimeSeries`) |
| role          | **filters** which containers survive       | **selected** signal / gate; does not prune          |
| config gate   | needs `poi_types` (rollup columns)         | reads its type directly; `poi_types` not required   |
| stage         | container-metrics filter (report-wide)     | solve step (union into per-container frame)         |
| syntax        | `q.metric("poi_<t>_count") >= N` / `.contains(...)` | `q.poi_channel(name)` then select/`where`   |
| scope         | tag ∩ metric container set                 | surviving-container set (POI-only containers emit)  |

Because a POI channel is scoped to the *container* set (not the channel-match
set), it is filtered by container tag/metric filters only — a channel `where`
filter does not prune a POI selection. See `POI_KNOWN_LIMITATIONS.md` #6.

---

## Where this is exercised

- Container filter: `tests/impulse_query_engine/unit/analyze/query/solvers/`
  `default_solver_poi_container_filter_test.py`, and the rollup unit test
  `poi_container_transformer_test.py`.
- POI channel — signal / gating / incremental / virtual signal / `.having` /
  coincident (`&`) / **categorical `dtype="string"`** / **aggregator guards** /
  **EAV-mode channel matching**:
  `tests/impulse_query_engine/integration/default_solver_poi_test.py` (uses the
  `poi_integration_db` wide-mode fixture and the `poi_integration_eav_db`
  EAV-mode fixture from `tests/conftest.py`).
