#!/usr/bin/env bash
# Syncs impulse_reporting/impulse_query_engine from this repo's src/ (so the
# app's own process can import them -- Databricks Apps only deploy the
# source-code-path directory itself, not sibling paths, so these can't just
# be sys.path-referenced at runtime) and builds a wheel from the same source
# into ./wheels/ (so the app can also ship this code to remote serverless
# workers -- TSAL compiles to Python UDFs that run there, not in the app's
# own process; see server/main.py's get_spark()).
#
# Run this once before `databricks sync` + `databricks apps deploy`, and
# again whenever the repo's src/ changes -- it always overwrites the local
# copies, so they can't silently drift from the canonical source. Assumes
# this directory lives two levels under the repo root (e.g.
# demos/agent_mcp_app/) -- adjust REPO_ROOT if you've moved it.
set -euo pipefail
cd "$(dirname "$0")"
REPO_ROOT="../.."

echo "Syncing impulse_reporting/impulse_query_engine from $REPO_ROOT/src..."
rm -rf impulse_reporting impulse_query_engine
cp -r "$REPO_ROOT/src/impulse_reporting" .
cp -r "$REPO_ROOT/src/impulse_query_engine" .

echo "Building wheel..."
rm -f wheels/databricks_impulse-*.whl
mkdir -p wheels
python3 -m pip install -q build
python3 -m build --wheel --outdir wheels "$REPO_ROOT"
echo "Built: $(ls wheels/databricks_impulse-*.whl)"
