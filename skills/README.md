# Impulse AI Assistant Skills

Impulse provides agent skills that can be used with [Databricks Genie Code](https://docs.databricks.com/aws/en/genie-code/skills) or any tool that follows the [Agent Skills](https://agentskills.io/) open standard. Skills teach AI assistants how to use Impulse — the Databricks Labs library for analyzing large-scale time-series measurement data on Spark and Delta.

## Skills

Each skill is a folder with a `SKILL.md` file that documents usage patterns. Start with [`impulse`](./impulse/SKILL.md), which explains the core concepts and routes to the right skill for the task.

| Skill                                                    | What it covers                                                                                                    |
|----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| [`impulse`](./impulse/SKILL.md)                          | Entry point. Core concepts (container, channel, event, aggregation), the three usage modes, setup, and a decision tree to the other skills. |
| [`impulse-tsal`](./impulse-tsal/SKILL.md)                | The Time Series Analytics Language DSL — selecting channels, deriving virtual signals, and the four result types (`SampleSeries`, `Intervals`, `PointsInTime`, `PointsInTimeSeries`). |
| [`impulse-data-model`](./impulse-data-model/SKILL.md)    | The silver-layer input tables Impulse reads, the gold-layer star schema it writes, landing your own data, and adapting an existing layout via column mappings. |
| [`impulse-config`](./impulse-config/SKILL.md)            | The `ImpulseConfig` schema — source tables, sink, container filters, solver options, incremental processing, and sinkless mode. |
| [`impulse-events`](./impulse-events/SKILL.md)            | Defining event windows: `BasicEvent`, `ContainerEvent`, `SequenceOfEvents`, `PointsInTimeEvent`.                   |
| [`impulse-aggregations`](./impulse-aggregations/SKILL.md)| Computing results over channels: 1D/2D histograms (duration/distance/custom-weight), `StatsAggregator`, `PointValueAggregator`, and pages. |
| [`impulse-reporting`](./impulse-reporting/SKILL.md)      | The batch pipeline that persists events and aggregations to the gold-layer star schema with `Report` / `Page`, plus incremental runs. |
| [`impulse-analyze`](./impulse-analyze/SKILL.md)          | Ad-hoc analysis — evaluating TSAL directly through the query engine and returning Spark or pandas DataFrames, no gold-layer write. |
| [`impulse-ml`](./impulse-ml/SKILL.md)                    | Extracting event-scoped statistics as a flat feature matrix for MLflow / AutoML.                                  |

## Install

Impulse runs inside a Databricks notebook or job with an active `spark` session and a Databricks SDK `WorkspaceClient`. Install it one of two ways:

- **Wheel** — `%pip install databricks-impulse[local-dev]` (the `local-dev` extra pulls in `pydantic`, `scipy`, and the other libraries that are otherwise assumed pre-installed on Databricks Serverless / DBR ML).
- **Git folder** — clone `https://github.com/databrickslabs/impulse` into a Databricks Git folder and add the repo's `src/` directory to `sys.path`, as the demo notebooks do.

Impulse requires Python 3.12 (Serverless Environment Version 2+), PySpark 4.0, and Delta Lake 4.0. See [`impulse`](./impulse/SKILL.md) for the full setup snippet.

## Scope and guardrails

These skills are scoped to Impulse's public API — the classes and config a user imports and calls. They do not document internal solver stages or private helpers. Every example is self-contained and uses only the framework's public primitives.
