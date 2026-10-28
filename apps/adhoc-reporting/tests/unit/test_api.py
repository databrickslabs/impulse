from types import SimpleNamespace

import pytest
from databricks.sdk.errors import NotFound
from fastapi.testclient import TestClient

from app import app
from server import runs
from server.user import User, current_user

client = TestClient(app)

NAME_ROUTES = [
    ("GET", ""),
    ("PUT", ""),
    ("DELETE", ""),
    ("POST", "/check"),
    ("POST", "/preflight"),
    ("POST", "/runs"),
    ("GET", "/runs"),
    ("GET", "/runs/1"),
    ("POST", "/runs/1/cancel"),
    ("GET", "/results"),
]


class Workspace:
    def download(self, path):
        raise NotFound(path)

    def get_status(self, path):
        raise NotFound(path)


def no_service_principal():
    raise AssertionError("the request reached the app's service principal")


@pytest.fixture
def signed_in(monkeypatch):
    monkeypatch.setattr(runs, "runner", no_service_principal)
    user = User("someone@example.com", SimpleNamespace(workspace=Workspace()), obo=True)
    app.dependency_overrides[current_user] = lambda: user
    yield
    app.dependency_overrides.clear()


def test_health():
    response = client.get("/api/meta/health")
    assert response.status_code == 200
    assert response.json()["impulse_version"]


def test_user_endpoints_require_forwarded_identity():
    assert client.get("/api/reports").status_code == 401
    assert client.get("/api/data/catalogs").status_code == 401


@pytest.mark.parametrize("name", ["%2E%2E", "%2E"])
@pytest.mark.parametrize(("method", "route"), NAME_ROUTES)
def test_report_routes_reject_names_that_are_paths(signed_in, method, route, name):
    response = client.request(method, f"/api/reports/{name}{route}")
    assert response.status_code == 422
    assert ["path", "name"] in [error["loc"] for error in response.json()["detail"]]


def test_report_routes_accept_report_names(signed_in):
    response = client.get("/api/reports/oil_temp_2")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "not_found"
