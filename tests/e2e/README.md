# End-to-end tests

Runs Impulse reports on Databricks serverless compute and compares the results with the expected files in `expected/`.

## Job

The job is defined in `resources/e2e_job.yml`. Each task is a notebook in this folder. After `setup`, the tests run in parallel in one temporary schema `e2e_<run_id>`, each with its own table prefix (the task name).

| Task | Notebook | Checks |
|---|---|---|
| `setup` | `setup_schema` | Creates the schema, drops this job's schemas older than one day, checks the source data against `expected/containers.csv` |
| `rpm_histogram` | `e2e_rpm_histogram` | Baseline report (event `Engine RPM > 2000`, duration histogram of `Engine RPM`): a compute-only run writes nothing and gives the expected histogram and events; two persisted runs update the gold tables in place (same Delta table, new version); the histogram adds up to the event durations |
| `incremental` | `e2e_incremental` | Baseline report on a silver copy that receives the containers in two loads, run incrementally in batches of 15; the second load leaves the first load's rows untouched and commits one MERGE per batch; must equal `rpm_histogram` |
| `incremental_updates` | `e2e_incremental_updates` | A rerun without changes rewrites no fact table; a re-ingested container (half its samples) is recomputed, its histogram matching its new events, while the other containers' rows stay untouched; changed histogram bins recompute only the histogram, which still adds up to the event durations; the result equals a full run |
| `pipeline` | `e2e_pipeline` | Container event, per-container statistics, an RPM x speed heatmap and a calculated channel; the calculated channel's samples must give the same statistics as the StatsAggregator |
| `container_filter` | `e2e_container_filter` | Baseline report on (`experiment_1` or `experiment_2`) and longer than 30 minutes; must equal those containers' `rpm_histogram` rows, with the containers selected independently in SQL |
| `events` | `e2e_events` | Points-in-time, sequence and 5-minute window events with point values and window statistics; point events have zero length; windows tile each container without gaps or overlaps; each slow_down has one value per channel, with speed at most 100 |
| `teardown` | `teardown` | Drops the schema; runs only if all tests passed, otherwise it ends as upstream failed, which fails the job run |

Shared code (report config, baseline report, comparison) is in `e2e_utils.py`. When a test fails, its schema is kept for debugging.

## Running

```bash
make test-e2e
```

This deploys the bundle (`databricks.yml`) and runs the job. It needs the Databricks CLI and workspace authentication.

CI runs it via `.github/workflows/e2e.yml` on every push to a PR into main, nightly and on demand. Each PR deploys its own copy of the bundle and job (bundle variable `suffix`, e.g. `-pr-123`), which is removed when the PR is closed. Fork PRs are skipped; mirror them with `make fork-sync PR=<n>`.

## Input data

The reports read the silver tables in `<source_catalog>.<source_schema>` (bundle variables in `databricks.yml`) and write to temporary schemas in `<catalog>`. They were created from MF4 files with the [Impulse MF4 ingest solution accelerator](https://github.com/databricks-industry-solutions/impulse-mf4-ingest).

`expected/containers.csv` lists the MF4 file (`experiment_id` folder and `filename`) behind each container id. Use it to rebuild the silver layer from scratch with the accelerator. If the rebuild assigns different container ids, refresh the expected files.

## Expected results

`expected/<test>/<table>.csv` holds the expected gold rows of a test (without `_` bookkeeping columns). `incremental` and `container_filter` reuse `expected/rpm_histogram/`; `incremental_updates` compares with full reference runs instead of expected files.

To refresh them after an intended change, run the job. On a mismatch, the test uploads its actual rows as CSV to `<bundle root>/actual/<test>/` and logs the path. Review them, then copy them into `expected/`.

## Adding a test

Add a notebook `e2e_<name>.py` that builds a report with `config(p)` and `new_report(...)` from `e2e_utils`, runs it, and calls `check(p, table, gold(p, table))`. Add a task `<name>` to the job and to the `depends_on` of `teardown`. The first run fails and uploads the actual rows; review them and save them as `expected/<name>/`.
