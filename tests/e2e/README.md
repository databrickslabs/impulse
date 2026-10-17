# End-to-end tests

Runs Impulse reports on Databricks serverless compute and compares the results with the expected files in `expected/`.

## Job

The job is defined in `resources/e2e_job.yml`. Each task is a notebook in this folder. After `setup`, the tests run in parallel in one temporary schema `e2e_<run_id>`, each with its own table prefix (the task name).

| Task | Notebook | Checks |
|---|---|---|
| `setup` | `setup_schema` | Creates the schema, drops this job's schemas older than one day, checks the source data against `expected/containers.csv` |
| `rpm_histogram` | `e2e_rpm_histogram` | Baseline report (event `Engine RPM > 2000`, duration histogram of `Engine RPM`), run twice; the rerun must update the gold tables in place |
| `incremental` | `e2e_incremental` | Baseline report on a silver copy that receives the containers in two loads, run incrementally in batches of 15; must equal `rpm_histogram` |
| `pipeline` | `e2e_pipeline` | Container event, per-container statistics and an RPM x speed heatmap |
| `container_filter` | `e2e_container_filter` | Baseline report filtered to `experiment_1`; must equal those containers' `rpm_histogram` rows |
| `teardown` | `teardown` | Drops the schema; runs only if all tests passed |
| `fail_on_error` | `fail_on_error` | Fails the job run if `setup` or a test failed |

Shared code (report config, baseline report, comparison) is in `e2e_utils.py`. When a test fails, its schema is kept for debugging.

## Running

```bash
make test-e2e
```

This deploys the bundle (`databricks.yml`) and runs the job. It needs the Databricks CLI and workspace authentication. CI runs it nightly via `.github/workflows/e2e.yml`.

## Input data

The reports read the silver tables in `<catalog>.<source_schema>` (bundle variables in `databricks.yml`). They were created from MF4 files with the [Impulse MF4 ingest solution accelerator](https://github.com/databricks-industry-solutions/impulse-mf4-ingest).

`expected/containers.csv` lists the MF4 file (`experiment_id` folder and `filename`) behind each container id. Use it to rebuild the silver layer from scratch with the accelerator. If the rebuild assigns different container ids, refresh the expected files.

## Expected results

`expected/<test>/<table>.csv` holds the expected gold rows of a test (without `_` bookkeeping columns). `incremental` and `container_filter` reuse `expected/rpm_histogram/`.

To refresh them after an intended change, run the job. On a mismatch, the test uploads its actual rows as CSV to `<bundle root>/actual/<test>/` and logs the path. Review them, then copy them into `expected/`.

## Adding a test

Add a notebook `e2e_<name>.py` that builds a report with `config(p)` and `new_report(...)` from `e2e_utils`, runs it, and calls `check(p, table, gold(p, table))`. Add a task `<name>` to the job and to the `depends_on` of `teardown` and `fail_on_error`. The first run fails and uploads the actual rows; review them and save them as `expected/<name>/`.
