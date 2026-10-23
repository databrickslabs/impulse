# Impulse App

A Databricks App for building [Impulse](https://databrickslabs.github.io/impulse) reports on measurement data from vehicles, machines and other units under test. Users pick data, units under test, signals, events and aggregations step by step or describe the report to an assistant; the app generates one Impulse notebook, runs it as a serverless job and charts the gold tables it writes.

The app is optional and not part of the `databricks-impulse` package. The [Impulse App documentation](https://databrickslabs.github.io/impulse/app/) describes what it does, its [requirements](https://databrickslabs.github.io/impulse/app/requirements), the [installation](https://databrickslabs.github.io/impulse/app/install) and the [architecture](https://databrickslabs.github.io/impulse/app/architecture).

## Code layout

```
React + Vite (frontend/)        report steps, results charts, chat window
FastAPI (app.py, server/)       REST API and assistant, all calls as the user
  datamodel.py, catalog.py      table discovery, units under test and channels through the SQL warehouse and UC
  spec.py, tsal.py, codegen.py  report spec, expression checks, notebook generation
  store.py, preferences.py      reports and per-user settings as workspace files in /Users/<user>/Impulse/
  runs.py, results.py           pre-flight, serverless jobs as the app service principal, gold queries
  agent/                        tools, prompt, loop, skills loader
skills/impulse/                 Impulse framework skills, copied from the pinned tag
skills/app/                     skills that teach the assistant to operate the app
```

## Development

Run the commands from this folder. `make test` needs no workspace. The other targets read a gitignored `.env`: copy `.env.example` and set your Databricks CLI profile, a SQL warehouse id, and the catalog and schema for the Impulse demo data.

| Command | Does |
|---|---|
| `make test` | Unit tests, including the skills lint, and contract tests that build every generated notebook against the pinned framework wheel |
| `make test FRAMEWORK=..` | Same, but the contract tests build against the framework source in this repo |
| `make check-skills` | Checks that `skills/impulse/` matches the root `skills/` at the pinned tag |
| `make seed` | Loads the Impulse demo silver tables into the demo catalog and schema |
| `make live` | Data access tests against the workspace |
| `make smoke` | Runs the fixture report on serverless without writing gold tables |
| `make smoke-persist` | Same, but writes the fixture report's gold tables, which `make live` reads |
| `make eval` | Runs the assistant scenarios against FMAPI |
| `make deploy` | Builds the frontend and deploys the app (commit the app first) |
| `make logs` | Shows the app logs |
| `make lock` | Updates `uv.lock` and `requirements.txt`, keeping both lockfiles on the public registries |

## Updating Impulse

```
uv run --no-project --python 3.12 python scripts/sync_framework.py vX.Y.Z
make test smoke eval
```

The script updates `impulse.lock`, replaces `skills/impulse/` from the tag with a checksum manifest and lists the upstream commits since the previous pin. Pin released versions only; bump the pin after each framework release.

Don't edit `skills/impulse/` by hand: edit the framework skills in the root `skills/` folder of this repo, and they reach the app with the next pin. `make check-skills`, which CI runs, fails if the copy differs from `skills/` at the pinned tag.
