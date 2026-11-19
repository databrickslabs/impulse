import os

import pytest
from databricks.sdk import WorkspaceClient

from server.user import User


@pytest.fixture(scope="session")
def live_user() -> User:
    if not os.environ.get("DATABRICKS_CONFIG_PROFILE"):
        pytest.skip("live tests need the workspace settings from .env.example")
    client = WorkspaceClient()
    return User(client.current_user.me().user_name, client)
