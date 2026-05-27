"""Local conftest for LakeVision pure-Python schema tests.

Overrides the session-autouse Spark fixtures inherited from tests/conftest.py
with no-op fixtures so schema/config tests run without a JVM.

When LakeVision adds Spark-dependent integration tests later, place them under
tests/mda_query_engine/perception/integration/ which will inherit the real Spark fixtures.
"""

import pytest


@pytest.fixture(scope="session")
def spark():
    yield None


@pytest.fixture(scope="session", autouse=True)
def setup_basic_db():
    yield


@pytest.fixture(scope="function", autouse=True)
def cleanup_gold():
    yield


@pytest.fixture(scope="session", autouse=True)
def cleanup_schemas():
    yield
