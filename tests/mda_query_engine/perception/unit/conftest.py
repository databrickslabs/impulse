"""Local conftest for LakeVision pure-Python unit tests.

Overrides the session-autouse Spark fixtures inherited from tests/conftest.py
with no-op module-scoped fixtures so these tests run without a JVM.

Using module scope (not session) avoids contaminating the root session-scoped
spark fixture when this directory is collected alongside Spark-dependent tests
in other directories (e.g. tests/mda_query_engine/unit/).

When LakeVision adds Spark-dependent integration tests later, place them under
tests/mda_query_engine/perception/integration/ which will inherit the real Spark fixtures.
"""

import pytest


@pytest.fixture(scope="module")
def spark():
    yield None


@pytest.fixture(scope="module", autouse=True)
def setup_basic_db():
    yield


@pytest.fixture(scope="function", autouse=True)
def cleanup_gold():
    yield


@pytest.fixture(scope="module", autouse=True)
def cleanup_schemas():
    yield
