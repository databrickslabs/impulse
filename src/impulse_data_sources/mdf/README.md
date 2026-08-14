# impulse_data_sources.mdf

> ⚠️ **Experimental** — see [KNOWN_LIMITATIONS.md](KNOWN_LIMITATIONS.md) for spec
> coverage gaps and backlog items.

Convert ASAM **MDF4** measurement files to **Delta Lake** tables with PySpark /
Databricks. The reader parses MDF4 binary blocks directly (no `asammdf` at
runtime), so conversion parallelises across Spark workers, each reading only the
bytes for its partition.

Every output row is identified by `file_uri` — the source file path.

## 📖 Documentation

**User-facing docs live in the published Impulse documentation** — that is the
source of truth for setup, the three data-source formats, options, schemas, and the
silver-layer worked example:

- **MDF4 data source:** <https://databrickslabs.github.io/impulse/docs/data_sources/mdf4>
- **Known limitations:** <https://databrickslabs.github.io/impulse/docs/data_sources/mdf4#known-limitations>

Source for those pages: [`docs/impulse/docs/data_sources/`](../../../docs/impulse/docs/data_sources/).

This README covers the package internals for contributors.

## Quick usage

```python
from databricks.sdk import WorkspaceClient
from impulse_data_sources.mdf import register_mdf_datasources

register_mdf_datasources(spark, WorkspaceClient())

signals = spark.read.format("mdf_signals").option("path", "/Volumes/.../mdf").load()
# discovers every *.mf4 under /Volumes/.../mdf, including subdirectories
```

Three read formats — `mdf_signals`, `mdf_metadata`, `mdf_masters` — plus a
high-level `MDFToDeltaConverter` that writes Delta tables directly. See the
[documentation](https://databrickslabs.github.io/impulse/docs/data_sources/mdf4)
for options, schemas, run-length encoding, and the silver-layer example.

## Acknowledgments

The MDF data sources and low-level reader were implemented with reference to
[asammdf](https://github.com/danielhrisca/asammdf) by [Daniel Hrisca](https://github.com/danielhrisca).
`asammdf` is not a runtime dependency of this package; it is listed under test
dependencies only (see below) for synthesising small MDF4 fixtures.

## Module layout

Package path: `src/impulse_data_sources/mdf/`

| module           | responsibility                                                                            |
| ---------------- | ----------------------------------------------------------------------------------------- |
| `mdf4_reader.py` | parse MDF4 structure (HD/DG/CG/CN) → `ChannelInfo`; header datetime                       |
| `mdf_blocks.py`  | low-level data-block I/O (`##DT`/`##DZ`/`##DL`/`##HL`, sub-blocks)                        |
| `mdf_decode.py`  | raw-bytes → values/timestamps (data types, CC conversion, invalidation)                   |
| `arrow_emit.py`  | build Arrow batches (per-group, stripe, master) + run-length encoding                     |
| `udf_helpers.py` | re-export shim over the three modules above (stable import surface)                       |
| `bin_packer.py`  | partition planning (`plan_partitions`, `plan_stripes_for_file`, `plan_master_partitions`) |
| `converter.py`   | `MDFToDeltaConverter` orchestration + Delta writes                                        |
| `datasources.py` | the three Spark data sources                                                              |
| `schemas.py`     | shared Spark schemas (`SIGNALS_SCHEMA`, `METADATA_SCHEMA`)                                |

## Dependencies

- Runtime: `numpy`, `pyarrow` (`pyspark` is provided by the Databricks runtime).
- Tests: `pytest`, `asammdf` (dev/test dependency only — synthesises small MDF4 files on the fly).
