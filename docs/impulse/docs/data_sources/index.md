---
title: Data Sources
---

# Data Sources

A **data source** is a pluggable reader that turns a raw measurement file format
into Spark DataFrames — the first step of getting your data into Impulse. Data
sources live in the standalone `impulse_data_sources` package and are built on
PySpark's [`DataSource`](https://spark.apache.org/docs/latest/api/python/user_guide/sql/python_data_source.html)
API, so a registered format reads like any other Spark source:

```python
df = spark.read.format("mdf_signals").option("path", "/Volumes/.../mdf").load()
```

## Why data sources

Decoding a proprietary measurement format is the expensive, format-specific part
of ingestion. By packaging that decode behind a Spark `DataSource`, Impulse:

- **parallelises decoding across Spark workers** — each task reads only the bytes
  for its partition, so throughput scales with the cluster rather than with a
  single driver;
- **keeps the format concern separate** from the query engine and the silver-layer
  schema — a data source produces plain DataFrames you can inspect, transform, and
  write however you like;
- **feeds the silver layer directly** — the decoded samples map onto the
  [silver-layer `channels` shape](../data_model/ingestion.md), so a data source is
  a natural front end for [ingestion](../data_model/ingestion.md).

## Available data sources

| Format | Package | Status |
| ------ | ------- | ------ |
| [ASAM MDF4](mdf4.md) — `.mf4` measurement files | `impulse_data_sources.mdf` | Experimental |

MDF4 is the **first** data source. The `impulse_data_sources` subsystem is designed
to grow to additional measurement formats over time.

Start with the [MDF4 data source](mdf4.md).
