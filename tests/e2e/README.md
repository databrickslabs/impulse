# End-to-end tests

Runs an Impulse report on Databricks serverless compute and compares the results with the expected files in `expected/`.

## Job

The job is defined in `resources/e2e_job.yml`. Each task is a notebook in this folder:

1. `setup_schema`: creates a temporary schema `e2e_<run_id>`.
2. `run_report`: runs the report (event `Engine RPM > 2000`, duration histogram of `Engine RPM`) and writes the gold tables to the temporary schema.
3. `run_report_rerun`: runs the same report again. The gold tables must be updated in place.
4. `validate`: compares the source containers and the gold tables with `expected/`.
5. `teardown`: drops the temporary schema. Always runs.
6. `fail_on_error`: fails the job run if any earlier task failed.

## Running

```bash
make test-e2e
```

This deploys the bundle (`databricks.yml`) and runs the job. It needs the Databricks CLI and workspace authentication. CI runs it nightly via `.github/workflows/e2e.yml`.

## Input data

The report reads the silver tables in `<catalog>.<source_schema>` (bundle variables in `databricks.yml`). They were created from MF4 files with the [Impulse MF4 ingest solution accelerator](https://github.com/databricks-industry-solutions/impulse-mf4-ingest).

`expected/containers.csv` lists the MF4 file (`experiment_id` folder and `filename`) behind each container id. Use it to rebuild the silver layer from scratch with the accelerator. If the rebuild assigns different container ids, refresh the expected files.

## Expected results

| File | Content |
|---|---|
| `containers.csv` | Container id to MF4 file mapping of the source data |
| `histogram_fact.csv` | Gold histogram rows |
| `event_instance_fact.csv` | Gold event instance rows |

To refresh them after an intended change, run the job. On a mismatch, `validate` uploads the actual rows as CSV to `<bundle root>/actual/` and logs the path. Review them, then copy them into `expected/`.
