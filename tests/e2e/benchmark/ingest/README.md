# Benchmark data: Renumics fleet telemetry → Impulse silver layer

A bundle with two serverless jobs. Together they create the silver tables read by the performance benchmark (see [`../../README.md`](../../README.md#re-creating-the-benchmark-dataset)):

| Job | Does |
|---|---|
| `Renumics_HF_Download` | Copies the [Synthetic Vehicle Fleet Telemetry](https://huggingface.co/datasets/renumics/synthetic-vehicle-fleet-telemetry) dataset from Hugging Face into a UC volume. The revision is pinned (`017ec8f02e0753d8b092cf9229e1372d92a7b635`). Files already copied are skipped, so a failed download can be rerun. |
| `Renumics_Ingest` | Ingests the next `max_containers_per_run` trips into the silver tables. Repeat it until all 100 trips are in. |

How the dataset maps to the silver tables:
- **One trip is one container.** The container id is the trip's `session_id`.
- **`channels`** holds RLE intervals `[tstart, tend)` in **epoch microseconds**. Every trip starts at `2026-03-20 12:00:00` UTC.
- **`channel_metrics` / `channel_tags`, `container_metrics` / `container_tags`** are built from the `channels` table and the dataset's `dim_*` tables.
- **`status`** tracks the trips: `unprocessed`, `in_progress`, `succeeded`, `failed`.

How the ingest behaves on reruns and failures:
- Each write replaces the batch's containers, so reruns are idempotent.
- A failed batch is rolled back from all five tables and retried by the next run.
- `--params reset=True` drops `status` and the silver tables. It keeps the schema and the volume.

Workspace host and auth come from the environment or a CLI profile (`-p`). The bundle variables `catalog`, `schema` and `raw_volume` choose where the data goes. The defaults match the benchmark job's `source_catalog` / `bench_source_schema`.

Based on the [`impulse-mf4-ingest`](https://github.com/databricks-industry-solutions/impulse-mf4-ingest) solution accelerator.

## Dataset and license

The code is under the Databricks License of this repository. The dataset is [Synthetic Vehicle Fleet Telemetry](https://huggingface.co/datasets/renumics/synthetic-vehicle-fleet-telemetry) on Hugging Face, published by [Renumics](https://renumics.com/) under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/). It is based on joint work between Volkswagen AG Group Innovation, IPG Automotive and Renumics. See `NOTICE.md` for the citation.
