# MDF data sources — quickstart

> 📖 **The quickstart now lives in the published Impulse documentation:**
> <https://databrickslabs.github.io/impulse/docs/data_sources/mdf4>
>
> That page is the source of truth — setup, the three formats (`mdf_signals`,
> `mdf_metadata`, `mdf_masters`), options, schemas, run-length encoding, and the
> worked example that writes the Impulse silver layer.
> Page source: [`docs/impulse/docs/data_sources/mdf4.md`](../../../docs/impulse/docs/data_sources/mdf4.md).

Minimal example:

```python
from databricks.sdk import WorkspaceClient
from impulse_data_sources.mdf import register_mdf_datasources

register_mdf_datasources(spark, WorkspaceClient())

signals = (
    spark.read.format("mdf_signals")
    .option("path", "/Volumes/catalog/schema/mdf_data")
    .load()
)
signals.select("file_uri", "channel_id", "time", "value").show(5)
```
