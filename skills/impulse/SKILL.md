---
name: impulse
description: >
  Entry point for the Impulse framework — the Databricks Labs library for analyzing large-scale
  time-series measurement data (automotive testing, industrial IoT sensor recordings) on Spark and
  Delta. Use when the user mentions "Impulse", "TSAL", measurement/sensor/telemetry channels, test
  drives, or wants to build histograms, event windows, or statistics over time-series recordings and
  isn't sure where to start. Explains the core concepts (container, channel, event, aggregation),
  the three usage modes (reporting, ad-hoc analysis, ML), how to set up `spark` + `WorkspaceClient`,
  and routes to the right sibling skill.
---

# Impulse — overview and routing

Impulse analyzes petabyte-scale time-series measurement data on Databricks without requiring Spark
expertise. You express signals, events, and aggregations in a Python DSL called **TSAL**; a **query
engine** compiles them to Spark and runs them per recording; **aggregations** produce duration- and
distance-weighted histograms and event-scoped statistics. It sits between a governed silver layer and
a gold-layer star schema in Unity Catalog.

Impulse is a **pure library** — there is no CLI and no bundle. Everything runs inside a Databricks
notebook or job.

## Core vocabulary

| Term            | Meaning                                                                                       |
|-----------------|-----------------------------------------------------------------------------------------------|
| **Container**   | One measurement recording — e.g. one test drive, one bench run. Identified by `container_id`. |
| **Channel**     | One sensor signal within a container — e.g. "Engine RPM". Selected by metadata tags, not columns. |
| **Event**       | A time window of interest, defined by a TSAL condition or spanning the whole recording.       |
| **Aggregation** | A computation over channel data within event windows — histogram, 2D histogram, or statistics. |
| **Silver layer**| The input Delta tables Impulse reads (`container_metrics`, `channel_metrics`, `channels`, …).  |
| **Gold layer**  | The output star schema Impulse writes (fact + dimension tables) for dashboards and apps.       |

## The three usage modes

Pick the mode that matches the user's goal, then open the skill for it.

| Goal                                                                    | Mode            | Skill                  |
|-------------------------------------------------------------------------|-----------------|------------------------|
| "Persist results to gold tables for a dashboard / scheduled job"        | **Reporting**   | `impulse-reporting`    |
| "Explore signals in a notebook, get a DataFrame back, no writes"        | **Ad-hoc**      | `impulse-analyze`      |
| "Turn recordings into a feature matrix for MLflow / AutoML"             | **ML**          | `impulse-ml`           |

All three build on the same foundation. Whichever mode, you will almost always also need:

- **`impulse-tsal`** — how to select channels and build the signal expressions every mode consumes.
- **`impulse-config`** — the config dict that points Impulse at your tables.
- **`impulse-data-model`** — the shape of the input tables and output star schema.
- **`impulse-events`** and **`impulse-aggregations`** — the building blocks of reporting and ML.

## Setup (every mode)

Impulse needs an active `spark` session (present in Databricks notebooks) and a Databricks SDK
`WorkspaceClient`. Install the library first:

```python
# Wheel install (Serverless / DBR ML). The local-dev extra adds pydantic, scipy, etc.
%pip install databricks-impulse[local-dev]
dbutils.library.restartPython()
```

Or, in a Databricks **Git folder** clone of the repo, add its source tree to `sys.path` (what the
demo notebooks do):

```python
import sys, os
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))  # REPO_ROOT = your cloned impulse folder
```

Then construct the workspace client:

```python
from databricks.sdk import WorkspaceClient
ws = WorkspaceClient()   # authenticates from the notebook's Databricks context
```

**Requirement:** Python 3.12+, PySpark 4.0, Delta Lake 4.0. On Databricks Serverless, use
**Environment Version 2 or higher** — Version 1 ships Python 3.10 and Impulse's first import fails
with `ImportError: cannot import name 'Self' from 'typing'`.

## Imports come from full module paths

Impulse's `__init__.py` files do not re-export symbols, so import from the full path every time:

```python
from impulse_reporting.core.report import Report
from impulse_reporting.core.page import Page
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.aggregations.histogram import HistogramDuration
```

The two top-level packages are `impulse_query_engine` (TSAL + query engine) and `impulse_reporting`
(the reporting orchestration layer). Each sibling skill lists the exact import path for the symbols
it covers.

## Minimal end-to-end example (reporting mode)

```python
from databricks.sdk import WorkspaceClient
from impulse_reporting.core.report import Report
from impulse_reporting.core.page import Page
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.aggregations.histogram import HistogramDuration

ws = WorkspaceClient()
config = {
    "source": {
        "container_metrics_table": "my_catalog.silver.container_metrics",
        "channel_metrics_table": "my_catalog.silver.channel_metrics",
        "channels_uri": "my_catalog.silver.channels",
        "channel_tags_table": "my_catalog.silver.channel_tags",
    },
    "unity_sink": {"catalog": "my_catalog", "schema": "gold", "table_prefix": "my_report"},
    "query_engine": {"solver": "DefaultSolver", "data_type": "RLE"},
}

report = Report(name="my_report", spark=spark, workspace_client=ws, config=config)

eng_rpm = report.get_db().query.channel(channel_name="Engine RPM")      # see impulse-tsal
high_rpm = BasicEvent(name="high_rpm", expr=eng_rpm > 2000)             # see impulse-events
report.add_event(high_rpm)

page = Page(page_number=1)                                             # see impulse-aggregations
page.add_aggregation(HistogramDuration(
    name="rpm_hist", base_expr=eng_rpm, bins=[0, 2000, 4000, 6000, 8000], event=high_rpm,
))
report.add_page(page)

report.determine_report()   # compute
report.persist_results()    # write the gold star schema
```

## Where to go next

- Building signal expressions → **`impulse-tsal`**
- Defining event windows → **`impulse-events`**
- Histograms / statistics → **`impulse-aggregations`**
- The full reporting lifecycle and incremental runs → **`impulse-reporting`**
- Notebook exploration without writes → **`impulse-analyze`**
- ML feature matrices → **`impulse-ml`**
- Config fields (filters, solver, sinkless) → **`impulse-config`**
- Input/output table shapes and ingestion → **`impulse-data-model`**
