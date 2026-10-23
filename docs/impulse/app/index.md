---
sidebar_position: 1
title: Overview
slug: /
---

# Impulse App

The Impulse App is a Databricks App for building Impulse reports without writing code. Users pick
the data, the units under test (vehicles, machines or test benches), signals, events and
aggregations step by step, or describe the report to an assistant. The app generates one Impulse
notebook, runs it as a serverless job and charts the gold tables it writes.

The app is optional. It lives in [`app/`](https://github.com/databrickslabs/impulse/tree/main/app)
in the Impulse repository and is installed separately from the `databricks-impulse` package.

## What it does

- **Guided steps:** data, units under test, signals and events, aggregations, review and run.
- **Assistant:** a chat window that builds and runs reports with the same tools as the UI.
- **The user's own permissions:** every catalog, SQL, workspace and model call is made as the
  signed-in user.
- **Settings:** each user picks the SQL warehouse for the app's queries and the assistant's chat
  model.
- **Runs:** report jobs run as the app's service principal, after a pre-flight check of the user's
  own access.
- **Results:** event counts, histograms, heatmaps and statistics, read from the report's gold
  tables.
- **One pinned framework version:** the app pins the Impulse release its report jobs install, and
  the assistant and the tests use the same version.

## Supported framework subset

| Report element | Framework class |
|---|---|
| Events where a condition holds | `BasicEvent` |
| Events at points in time, such as rising edges | `PointsInTimeEvent` |
| Aggregations over the whole recording | no event |
| Duration and distance histograms | `HistogramDuration`, `HistogramDistance` |
| Heatmaps (2D duration histograms) | `Histogram2DDuration` |
| Statistics: min, max, mean, median, start, end | `StatsAggregator` |
| Values at points in time | `PointValueAggregator` |

Sequences of events, calculated channels, custom statistics, points-in-time channels and
incremental runs aren't supported in the app; use the framework directly for those.

## Reporting mode

Every run uses Impulse's reporting mode and writes events and aggregations to gold tables in Unity
Catalog, as described in [Report](/docs/references/report); the framework's ad-hoc analysis through
the [query engine](/docs/references/query_engine) isn't part of the app.

## Next steps

- [Requirements](requirements.md): workspace features, data and permissions.
- [Install](install.md): build, deploy and grant access.
- [Architecture](architecture.md): components, identities and where data is stored.
