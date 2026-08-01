"""Fixtures for the on-Databricks end-to-end suite.

These tests run **inside a serverless Databricks job** (see ``databricks.yml`` /
``resources/e2e_job.yml``), where a Spark session and workspace auth are already
ambient. We reuse ``databricks-labs-pytester`` for its ephemeral-schema / volume
fixtures (``make_schema``, ``make_volume``, ``make_random``, the watchdog
``RemoveAfter`` tagging), but its stock ``ws`` / ``spark`` / ``sql_backend``
fixtures assume the *opposite* environment — a runner *outside* the workspace
using ``DATABRICKS_HOST`` env vars, Databricks Connect, and a SQL warehouse. So
we override those three to bind to the in-job session/auth instead; every other
pytester fixture is inherited from the plugin and picks up these overrides.
"""

import os

import pytest

# NOTE: no top-level import of databricks-sdk / databricks-labs-* here. The root
# tests/conftest.py sets ``collect_ignore_glob`` to skip tests/e2e when
# ``databricks-labs-pytester`` is absent (local / default CI), but pytest still
# *imports* this conftest during collection. Keeping the workspace imports
# inside the fixture bodies means importing this module is harmless when those
# packages aren't installed; the fixtures only run in-job where they are.

# Target UC catalog for the ephemeral schemas the tests create. Matches the
# bundle variable / the catalog the CI service principal was granted on.
E2E_CATALOG = os.environ.get("IMPULSE_E2E_CATALOG", "impulse_tests")


@pytest.fixture
def ws():
    """Ambient workspace client.

    Overrides pytester's ``ws`` (which reads ``DATABRICKS_HOST`` from the env).
    Inside a job, a no-arg ``WorkspaceClient()`` picks up the job's ambient
    credentials via the SDK's unified auth.
    """
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


@pytest.fixture
def spark():
    """Ambient in-job Spark session.

    Overrides pytester's ``spark`` (which builds a Databricks Connect session).
    """
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


@pytest.fixture
def sql_backend(spark):
    """SQL backend bound to the ambient session.

    Overrides pytester's ``sql_backend`` (a ``StatementExecutionBackend`` that
    needs ``DATABRICKS_WAREHOUSE_ID``). ``RuntimeBackend`` runs statements
    through ``SparkSession.builder.getOrCreate()`` — the in-job session — so
    ``make_schema`` and friends work without a warehouse.
    """
    from databricks.labs.lsql.backends import RuntimeBackend

    return RuntimeBackend()


@pytest.fixture
def e2e_schema(make_schema):
    """A randomly-named ephemeral schema under the e2e catalog.

    Auto-dropped (``DROP SCHEMA ... CASCADE``) after the test and tagged with a
    ``RemoveAfter`` watchdog property, so a crashed run can't leak resources in
    the shared ``impulse_tests`` catalog. Returns the ``SchemaInfo`` — use
    ``.full_name`` (``impulse_tests.<random>``) to qualify tables.
    """
    return make_schema(catalog_name=E2E_CATALOG)


@pytest.fixture
def reporting_demo_dir() -> str:
    """Filesystem path to the reporting demo CSVs.

    The bundle syncs the whole repo into the workspace, so ``demos/data/reporting``
    sits at a stable path relative to this file. Overridable via
    ``IMPULSE_E2E_DATA_DIR`` if the layout differs.
    """
    override = os.environ.get("IMPULSE_E2E_DATA_DIR")
    if override:
        return override
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = here[: here.find("tests")]
    return os.path.join(repo_root, "demos", "data", "reporting")
