from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from fastapi import Header, HTTPException

from server.settings import settings


class User:
    def __init__(self, email: str, client: WorkspaceClient, obo: bool = False):
        self.email = email
        self.client = client
        self.obo = obo


def current_user(
    x_forwarded_email: str = Header(default=""),
    x_forwarded_access_token: str = Header(default=""),
) -> User:
    if not x_forwarded_email or not x_forwarded_access_token:
        raise HTTPException(401, "User authorization required")
    client = WorkspaceClient(config=Config(host=settings.host, token=x_forwarded_access_token, auth_type="pat"))
    return User(x_forwarded_email, client, obo=True)
