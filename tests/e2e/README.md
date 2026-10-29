# End-to-end tests

Runs Impulse reports on Databricks serverless compute and compares the results with the expected files in `expected/`. A separate job measures performance; see [Performance benchmark](#performance-benchmark).

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

## Performance benchmark

Measures the runtime and memory of one report on a large dataset, so that changes and releases can be compared with a baseline. The job is defined in `resources/benchmark_job.yml`, and its notebooks are in `benchmark/`.

The report (`benchmark/benchmark_report.py`) runs on the [Renumics fleet telemetry](#re-creating-the-benchmark-dataset): 100 trips, 680 signals, 5.08 B samples. Channel batching is Impulse's default, so each trip's ~50 channels are solved together. It uses:
- six events: the trip, driving cycles (merged intervals), charging and recuperation (battery thresholds), slow-downs (points in time) and 5-minute windows;
- two calculated channels;
- 1D and 2D histograms, statistics per event and point values;
- statistics of 40 signals resampled to 1 Hz. These are what Spark pushdown can speed up.

| Task | Notebook | Does |
|---|---|---|
| `setup` | `bench_setup` | Creates the results table `<catalog>.<bench_results_schema>.results`, or adds new metric columns to it, and the run's schema `bench_<run_id>`. Drops this job's schemas older than one day, fills in the DBUs of earlier runs, and checks that the source data is the full dataset |
| `run` | `run_benchmark` | Scenario `full`: the report into the empty schema. Scenario `incremental_noop`: an incremental rerun with nothing to update. Appends one row per scenario to the results table |
| `compare` | `bench_compare` | Compares the run with the median of the last 5 runs on `baseline_ref` that passed the gate (same benchmark version, compute and no overrides). Returns the table as markdown and stores the verdict in `gate_passed`, so a run with a regression never becomes part of a baseline |
| `gate` | `bench_gate` | Fails if a scenario failed, or if the `full` scenario's `wall_s`, `executor_run_time_s` or `peak_executor_python_rss_bytes` is more than `max_regression` (default 10 %) above the baseline. Identical runs differ by less than 1 % |
| `teardown` | `bench_teardown` | Drops the run's schema, also after a failure |

### Cluster

All tasks run on one job cluster of fixed size, so that runs are comparable:
- Databricks Runtime 17.3 LTS;
- a driver and 8 workers of the memory-optimized `r6id.2xlarge` (8 cores, 64 GB each);
- no autoscaling, no Photon;
- on-demand nodes only, so no spot instance is lost during a measurement.

The bundle variables `bench_spark_version`, `bench_node_type` and `bench_num_workers` set it. The cluster spec is stored with each run (`compute`), and runs only compare with runs of the same spec, so a new spec starts a new baseline. The cluster start (~4 min) isn't measured. The `full` scenario takes ~23 min and the whole job ~31 min. The job's principal needs the `allow-cluster-create` entitlement.

The notebooks install the wheel the bundle built (task parameter `wheel_dir`) with `%pip`, rather than as a cluster library: in the benchmark workspace, a cluster library keeps the notebooks from starting ("Hive metastore initialization failed").

### Metrics

Per scenario:

| Metric | Source |
|---|---|
| `wall_s` | Wall time of `report.run()` |
| `num_stages`, `executor_run_time_s`, `executor_cpu_time_s`, `jvm_gc_time_s`, `input_bytes`, `output_bytes`, `shuffle_read_bytes`, `shuffle_write_bytes`, `memory_spilled_bytes`, `disk_spilled_bytes`, `num_tasks`, `failed_tasks` | Spark UI REST API of the driver, summed over the stages the scenario ran. `executor_run_time_s` is the time spent in tasks on all executors, which depends less on the cluster size than `wall_s` |
| `peak_executor_python_rss_bytes` | Largest memory of the Python processes on one executor, i.e. the pandas UDF workers that run Impulse's expressions (Spark's process-tree metrics, `spark.executor.processTreeMetrics.enabled`) |
| `peak_executor_jvm_rss_bytes`, `peak_executor_jvm_heap_bytes`, `peak_executor_onheap_execution_bytes` | The executors' JVM memory peaks, same source |
| `peak_driver_jvm_heap_bytes` | The driver's JVM heap peak (over the cluster's lifetime) |
| `peak_driver_python_rss_bytes` | Peak memory of the notebook's Python process, where Impulse plans and collects (psutil, every second) |
| `dbus` | `system.billing.usage`. It arrives hours later, so a later run's `setup` fills it in (needs `SELECT` on the system table) |
| `fingerprint` | Row count and sums of the numeric columns of each gold table, without ids and hashes, which differ per run. If it differs from the baseline's, `compare` warns: a faster run with other results isn't comparable |

### Running

```bash
make benchmark
```

This deploys the bundle and runs the job with the default parameters (`git_ref=manual`). CI runs `.github/workflows/benchmark.yml` nightly on main. Once the workflow is on main, you can also start it on any branch or tag (Actions > benchmark > Run workflow), optionally with `config_overrides` and `baseline_ref`. The comparison table is written to the workflow's summary. Each ref deploys its own copy of the bundle (suffix `-benchmark-<ref>`), which is removed after the run; the results table is shared. Runs of different refs run in parallel, each on its own cluster. For one ref, GitHub keeps one pending run, so a newer run replaces a pending one.

`config_overrides` is JSON merged into the report config per section, e.g. `{"query_engine": {"enable_pushdown": true}}`. A run with overrides compares with the baseline runs, which have none.

Bump `BENCHMARK_VERSION` in `benchmark_report.py` whenever the report or its config changes. Runs only compare with runs of the same version, so the next runs on main start a new baseline.

To query the results:

```sql
SELECT ts, git_ref, git_sha, impulse_version, scenario, status, wall_s, executor_run_time_s, peak_executor_python_rss_bytes, dbus
FROM impulse_testing_catalog.impulse_benchmark.results ORDER BY ts DESC
```

## Re-creating the benchmark dataset

The benchmark reads `<source_catalog>.<bench_source_schema>` (`impulse_testing_catalog.impulse_renumics` by default). The silver tables there come from the [Synthetic Vehicle Fleet Telemetry](https://huggingface.co/datasets/renumics/synthetic-vehicle-fleet-telemetry) dataset on Hugging Face (CC BY-SA 4.0). It is published by [Renumics](https://renumics.com/), based on joint work between Volkswagen AG Group Innovation, IPG Automotive and Renumics; see `NOTICE.md` for the citation. The bundle in `benchmark/ingest/` builds them: it downloads ~52 GB and takes about 35 minutes on serverless.

1. **Prerequisites:**
   - Databricks CLI authentication to the workspace, e.g. a profile (`-p <profile>`) or `DATABRICKS_HOST` with a token. The bundle has no workspace URL.
   - Serverless compute with internet access to `huggingface.co`.
   - Privileges to create a schema and a volume in the catalog, or both created beforehand.
2. **Deploy the bundle.** The variables `catalog`, `schema` and `raw_volume` (defaults `impulse_testing_catalog`, `impulse_renumics`, `raw_data`) choose where the data goes:

   ```bash
   cd tests/e2e/benchmark/ingest
   databricks bundle deploy -t data
   ```

3. **Download the dataset** into `/Volumes/<catalog>/<schema>/<raw_volume>/synthetic-vehicle-fleet-telemetry`. This takes about 5 minutes. The schema and volume are created if missing. The Hugging Face revision is pinned. Files already copied are skipped, so rerun the job if it fails.

   ```bash
   databricks bundle run -t data Renumics_HF_Download
   ```

4. **Ingest the trips** in five batches of 20. Each run takes about 6 minutes. The first run registers all 100 trips in the `status` table, and each run ingests the next batch. A failed batch is rolled back and retried by the next run. `--params reset=True` starts over.

   ```bash
   for i in 1 2 3 4 5; do databricks bundle run -t data Renumics_Ingest --params max_containers_per_run=20; done
   ```

5. **Verify** that all trips are in. The query should return `100, 100, 5076151061`:

   ```sql
   SELECT (SELECT count(*) FROM impulse_testing_catalog.impulse_renumics.status WHERE status = 'succeeded'),
          (SELECT count(DISTINCT container_id) FROM impulse_testing_catalog.impulse_renumics.channel_metrics),
          (SELECT count(*) FROM impulse_testing_catalog.impulse_renumics.channels)
   ```

   The benchmark's `setup` task also checks the number of containers and samples, and a checksum of every container's id, channel count and sample count (constants in `bench_setup.py`). This keeps data derived from the dataset out of the repository. If a rebuild assigns other container ids or counts, `setup` fails and prints the actual rows. Review them, update the constants, and bump `BENCHMARK_VERSION`, since the old baseline no longer applies.
6. **Grant the CI service principal read access:**

   ```sql
   GRANT USE SCHEMA, SELECT ON SCHEMA impulse_testing_catalog.impulse_renumics TO `<service principal application id>`
   ```

Quirks of the data:
- `PT.BattHV.Temp` is labelled `degC` but logged in Kelvin.
- `Vhcl.v` has spikes above 2000 m/s.
- The SOC is 0 while the BMS initialises.
- Only the 63 electric and hybrid trips have HV battery signals.

The benchmark report handles these in its signal definitions; see `benchmark/ingest/README.md` for the table layout and license.
