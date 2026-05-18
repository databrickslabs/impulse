---
sidebar_position: 2
title: Getting Started
---

# Getting Started

Clone the Impulse repository into your Databricks workspace, open the
`demos/getting_started.ipynb` notebook, fill in three widgets, and run
a one-event, one-histogram report end-to-end in about five minutes.

For a deeper walkthrough that uses three events, both 1D and 2D histograms,
and multiple statistics aggregators, continue with the
[Tutorial](tutorial/demo.md) after finishing this page.

---

## Prerequisites

- A **Databricks workspace** with **serverless compute** enabled.
- Permission to create a schema in a catalog you can write to (or an
  existing catalog/schema with write access).

This demo uses pre-shaped data; no ingestion work is required.
For your own data, see [Ingestion](./data_model/ingestion.md) — Impulse's
default solvers expect a specific
[silver-layer shape](./data_model/index.md), and matching that shape during
ingest is the simplest path.

---

## Step 1 — Clone the Impulse repository into a Git Folder

In the Databricks workspace UI:

1. **Workspace** → **Create** → **Git folder**.
2. **Git repository URL:** `https://github.com/databrickslabs/impulse`
3. **Git folder name:** `impulse` (or whatever you prefer).

Databricks pulls the source, the demo data, and the demo notebooks into
your workspace at `/Workspace/Users/<you>/impulse`. See the
[Databricks Git folders docs](https://docs.databricks.com/aws/en/repos/)
if you have not used the feature before.

---

## Step 2 — Open the Getting Started notebook

In the Git folder you just cloned, open
`demos/getting_started.ipynb`. Attach it to a **Serverless** cluster
running **Environment Version 2 or higher** (Python 3.12+) — the
default Environment Version 1 ships Python 3.10 and the notebook's
first import will fail with `ImportError: cannot import name 'Self'
from 'typing'`.

---

## Step 3 — Configure the run

Run cell 1 (widget declaration). After it executes, three text widgets
appear at the top of the notebook:

- **Catalog** — any Unity Catalog you have `CREATE SCHEMA` and
  `CREATE TABLE` permissions on.
- **Schema** — created if it does not already exist (e.g.
  `impulse_demo`).
- **Table Prefix** — applied to every silver and gold table the
  notebook creates so they are easy to identify and clean up later
  (e.g. `demo`).

Fill all three in before running the remaining cells. The notebook
auto-detects its own location in the workspace, so there is no path
to edit.

---

## Step 4 — Run cells 2–4

Below is what each remaining cell does and what to expect.

### Cell 2: load the demo data as silver-layer Delta tables

Reads the widget values, derives the repo path from the notebook's own
location, adds the source tree to `sys.path`, creates the schema, and
turns each of the five demo CSVs into a Delta table named
`<CATALOG>.<SCHEMA>.<PREFIX>_<csv_name>`. Together those five tables
form the silver-layer input to Impulse — see
[Data Model](./data_model/index.md) for the schema Impulse expects.

You only need to run this cell once per workspace. It prints
`Loaded 5 silver-layer tables under <CATALOG>.<SCHEMA>.<PREFIX>_*` on
success.

### Cell 3: define and run the report

Imports `Report`, `Page`, `BasicEvent`, and `HistogramDuration` from the
framework, builds a small config, and:

- Selects the **Engine RPM** channel from the demo data
  (Seat Leon recordings).
- Defines an event covering every interval where RPM > 2000.
- Computes a duration-weighted histogram of RPM over those intervals,
  binned in 500-rpm steps from 0 to 5000.
- Persists the result to a gold-layer star schema using the prefix
  you entered in the Table Prefix widget.

The `data_type: "RAW"` setting tells the solver that the `channels`
table stores raw `(timestamp, value)` samples rather than pre-encoded
`[tstart, tend)` intervals — see
[Silver Layer Schema](./data_model/silver_layer_schema.md) for the
difference between the two formats.

### Cell 4: visualize the output

Reads the persisted gold-layer histogram fact table, sums durations
across containers per bin, and hands the result to `display()`. The
cell ships with a pre-configured bar chart — open the **Histogram** tab
on the cell output (x = `bin_name`, y = `duration_us`).

You should see ten bins. The four bins below 2000 rpm are at zero (the
event filtered them out) and the bulk of the duration concentrates in
the 2000-2500 rpm bin.

---

## Where to next

- **[Tutorial — Reporting walkthrough](tutorial/demo.md)** — the same workflow
  with multiple events, 1D and 2D histograms, and statistics
  aggregators.
- **[TSAL reference](./references/tsal.md)** — the full DSL for
  selecting channels, building virtual signals, and expressing event
  conditions.
- **[Data Model](./data_model/index.md)** — the silver-layer schema
  Impulse expects, and how to land your own measurement data into it.
- **[Configuration reference](./config/configuration.md)** — every
  config field including container filters, solver options, and
  incremental processing.
