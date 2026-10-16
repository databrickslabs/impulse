"""Fixtures for the e2e suite, which runs inside a serverless Databricks job.

Overrides pytester's ``ws``, ``spark`` and ``sql_backend`` fixtures to use the
job's own session and credentials; the other pytester fixtures (e.g.
``make_schema``) are reused as-is.
"""

import os

import pytest

# Workspace packages are imported inside the fixtures: pytest imports this file
# even where they are not installed and tests/e2e is skipped.


@pytest.fixture
def ws():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


@pytest.fixture
def spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


@pytest.fixture
def sql_backend(spark):
    from databricks.labs.lsql.backends import RuntimeBackend

    return RuntimeBackend()


@pytest.fixture
def e2e_schema(make_schema):
    """Ephemeral schema for the test's output, dropped after the test."""
    return make_schema(catalog_name=os.environ["IMPULSE_E2E_CATALOG"])


@pytest.fixture
def source_schema() -> str:
    """Fully qualified schema with the silver test data (read-only)."""
    return f"{os.environ['IMPULSE_E2E_CATALOG']}.{os.environ['IMPULSE_E2E_SOURCE_SCHEMA']}"
