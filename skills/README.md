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

## Install the skills

These skills are **not** discovered from this `skills/` folder directly — an agent only reads them from its configured skills directory. Copy the skill folders into that directory once; do it again whenever you change a skill (the copy is a snapshot, not a live link back to this repo).

### Databricks Genie Code

Genie Code discovers skills from a `.assistant/skills/` directory. Choose one:

- **User-level** (just you): `/Workspace/Users/<your-email>/.assistant/skills`
- **Workspace-wide** (everyone in the workspace): `/Workspace/.assistant/skills`

Import this folder with the Databricks CLI, run from the repo root:

```bash
# user-level
databricks workspace import-dir skills /Workspace/Users/<your-email>/.assistant/skills

# or workspace-wide
databricks workspace import-dir skills /Workspace/.assistant/skills
```

Genie picks the skills up the next time you use it. Confirm with a prompt like *"List the Impulse skills you can use."*

### Claude Code

Copy the skill folders to `~/.claude/skills/` on your machine, keeping each `SKILL.md` at the top level of its folder (not nested deeper):

```bash
cp -R skills/impulse* ~/.claude/skills/
```

Claude Code also reads the `.claude-plugin/plugin.json` in this folder, so the family can alternatively be installed as a plugin. Genie Code ignores that file and reads each `SKILL.md` directly.

### Other Agent Skills tools

Place the skill folders in the tool's configured skills directory, keep `SKILL.md` at each folder's root, and restart the tool if it caches skills on startup.

## Installing the Impulse library itself

The skills teach an agent to *use* Impulse; they do not install it. Inside a Databricks notebook or job (with an active `spark` session and a Databricks SDK `WorkspaceClient`), install the library one of two ways:

- **Wheel** — `%pip install databricks-impulse[local-dev]` (the `local-dev` extra pulls in `pydantic`, `scipy`, and the other libraries otherwise assumed pre-installed on Databricks Serverless / DBR ML).
- **Git folder** — clone this repository into a Databricks Git folder and add its `src/` directory to `sys.path`, as the demo notebooks do.

Impulse requires Python 3.12 (Serverless Environment Version 2+), PySpark 4.0, and Delta Lake 4.0. See [`impulse`](./impulse/SKILL.md) for the full setup snippet.

## Scope and guardrails

These skills are scoped to Impulse's public API — the classes and config a user imports and calls. They do not document internal solver stages or private helpers. Every example is self-contained and uses only the framework's public primitives.
