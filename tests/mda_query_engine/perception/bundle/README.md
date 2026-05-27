# Bundle validation tests

Smoke tests that every Databricks Asset Bundle under `bundles/` parses, resolves its variables, and passes `databricks bundle validate`. They run before workspace deployment as a cheap pre-flight — a malformed bundle should fail here, not after the deploy round-trip.

These tests shell out to the `databricks` CLI. Skip automatically if the CLI is not on PATH (so local devs without the CLI installed still get a clean suite).

## Running

```
cd reusable/repos/impulse
uv run --exact --all-extras pytest tests/mda_query_engine/perception/bundle/ -v
```

## What's covered

- `databricks bundle validate` exits 0 for every bundle directory.
- Each bundle's required variables have defaults or are documented.

## What's NOT covered (intentionally)

- Resource creation in a workspace — that's `/deploy-e2e`.
- Job logic correctness — that's the pipeline integration tests under `../integration/`.
