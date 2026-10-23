import io
from types import SimpleNamespace

import pytest
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.serving import EndpointState, EndpointStateReady
from databricks.sdk.service.sql import State
from fastapi.testclient import TestClient

from app import app
from server import preferences
from server.agent import loop
from server.errors import ApiError
from server.preferences import Preferences
from server.settings import settings
from server.user import User, current_user

EMAIL = "someone@example.com"
READY = EndpointState(ready=EndpointStateReady.READY)


class Workspace:
    def __init__(self):
        self.files: dict[str, bytes] = {}

    def download(self, path):
        if path not in self.files:
            raise NotFound("missing")
        return io.BytesIO(self.files[path])

    def upload(self, path, content, format=None, overwrite=False):
        self.files[path] = content.read()

    def mkdirs(self, path):
        pass


class Warehouses:
    def __init__(self, items, fail=False):
        self.items = items
        self.fail = fail

    def get(self, id):
        if not any(w.id == id for w in self.items):
            raise NotFound(f"Warehouse {id} does not exist")
        return next(w for w in self.items if w.id == id)

    def list(self):
        if self.fail:
            raise PermissionDenied("Invalid scope")
        return iter(self.items)


class Endpoints:
    def __init__(self, items, fail=False):
        self.items = items
        self.fail = fail

    def get(self, name):
        if not any(e.name == name for e in self.items):
            raise NotFound(f"Endpoint {name} does not exist")
        return next(e for e in self.items if e.name == name)

    def list(self):
        if self.fail:
            raise PermissionDenied("Invalid scope")
        return iter(self.items)


def warehouse(id, name, state=State.RUNNING):
    return SimpleNamespace(id=id, name=name, state=state, cluster_size="Small")


def endpoint(name, task="llm/v1/chat", ready=True):
    return SimpleNamespace(name=name, task=task, state=READY if ready else EndpointState(ready=EndpointStateReady.NOT_READY))


def client(warehouses=None, endpoints=None, fail_warehouses=False, fail_endpoints=False):
    return SimpleNamespace(
        workspace=Workspace(),
        warehouses=Warehouses(warehouses or [], fail_warehouses),
        serving_endpoints=Endpoints(endpoints or [], fail_endpoints),
    )


@pytest.fixture(autouse=True)
def clean():
    preferences._cache.clear()
    loop._models.clear()
    yield
    app.dependency_overrides.clear()


@pytest.fixture
def frozen(monkeypatch):
    def replace(**changes):
        monkeypatch.setattr("server.preferences.settings", SimpleNamespace(**{**vars(settings), **changes}))
        monkeypatch.setattr("server.routes.settings.settings", SimpleNamespace(**{**vars(settings), **changes}))

    return replace


def api(user: User) -> TestClient:
    app.dependency_overrides[current_user] = lambda: user
    return TestClient(app)


def test_defaults_without_a_saved_file(frozen):
    frozen(warehouse_id="app-warehouse")
    user = User(EMAIL, client(), obo=True)
    assert preferences.get(user) == Preferences()
    assert preferences.warehouse_id(user) == "app-warehouse"


def test_saved_preferences_are_read_back(frozen):
    frozen(warehouse_id="app-warehouse")
    user = User(EMAIL, client(), obo=True)
    preferences.save(user, Preferences(warehouse_id="mine", serving_endpoint="databricks-gpt-5"))
    preferences._cache.clear()
    assert preferences.get(user) == Preferences(warehouse_id="mine", serving_endpoint="databricks-gpt-5")
    assert preferences.warehouse_id(user) == "mine"
    assert user.client.workspace.files[preferences.path(EMAIL)].startswith(b"{")


def test_unreadable_file_falls_back_to_defaults():
    user = User(EMAIL, client(), obo=True)
    user.client.workspace.files[preferences.path(EMAIL)] = b"not json"
    assert preferences.get(user) == Preferences()


def test_app_principal_ignores_preferences(frozen):
    frozen(warehouse_id="app-warehouse")
    user = User("app-id", None)
    assert preferences.get(user) == Preferences()
    assert preferences.warehouse_id(user) == "app-warehouse"


def test_pick_model_prefers_the_users_choice():
    user = User(EMAIL, client(endpoints=[endpoint("databricks-claude-sonnet-5-5")]), obo=True)
    assert loop.pick_model(user) == "databricks-claude-sonnet-5-5"
    preferences.save(user, Preferences(serving_endpoint="my-model"))
    assert loop.pick_model(user) == "my-model"


def test_pick_model_without_any_endpoint():
    user = User(EMAIL, client(), obo=True)
    assert loop.default_model(user) is None
    with pytest.raises(ApiError, match="No chat model"):
        loop.pick_model(user)


def test_get_and_put_settings(frozen):
    frozen(warehouse_id="app-warehouse")
    user = User(EMAIL, client(warehouses=[warehouse("w1", "Shared")], endpoints=[endpoint("databricks-gpt-5")]), obo=True)
    http = api(user)
    assert http.get("/api/settings").json() == {
        "warehouse_id": "",
        "serving_endpoint": "",
        "default_warehouse_id": "app-warehouse",
        "default_model": "databricks-gpt-5",
    }
    saved = http.put("/api/settings", json={"warehouse_id": "w1", "serving_endpoint": "databricks-gpt-5"})
    assert saved.status_code == 200
    assert saved.json()["warehouse_id"] == "w1"
    assert preferences.warehouse_id(user) == "w1"


@pytest.mark.parametrize(
    ("body", "code"),
    [
        ({"warehouse_id": "nope", "serving_endpoint": ""}, "invalid_warehouse"),
        ({"warehouse_id": "", "serving_endpoint": "nope"}, "invalid_model"),
    ],
)
def test_put_rejects_unavailable_choices(frozen, body, code):
    frozen(warehouse_id="app-warehouse")
    user = User(EMAIL, client(warehouses=[warehouse("w1", "Shared")]), obo=True)
    response = api(user).put("/api/settings", json=body)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == code
    assert preferences.get(user) == Preferences()


def test_options_lists_chat_models_and_warehouses():
    endpoints = [
        endpoint("zeta-chat"),
        endpoint("databricks-bge-large-en", task="llm/v1/embeddings"),
        endpoint("databricks-claude-sonnet-4-5", ready=False),
        endpoint("databricks-claude-sonnet-5-5"),
    ]
    warehouses = [warehouse("w2", "beta", State.STOPPED), warehouse("w1", "Alpha"), warehouse("w3", "gone", State.DELETED)]
    body = api(User(EMAIL, client(warehouses=warehouses, endpoints=endpoints), obo=True)).get("/api/settings/options").json()
    assert [w["id"] for w in body["warehouses"]] == ["w1", "w2"]
    assert body["warehouses"][1] == {"id": "w2", "name": "beta", "state": "STOPPED", "size": "Small"}
    assert body["models"] == [
        {"name": "databricks-claude-sonnet-5-5", "ready": True},
        {"name": "databricks-claude-sonnet-4-5", "ready": False},
        {"name": "zeta-chat", "ready": True},
    ]
    assert body["warehouses_error"] is None and body["models_error"] is None


def test_options_survive_one_failing_list():
    user = User(EMAIL, client(warehouses=[warehouse("w1", "Alpha")], endpoints=[endpoint("x")], fail_endpoints=True), obo=True)
    body = api(user).get("/api/settings/options").json()
    assert [w["id"] for w in body["warehouses"]] == ["w1"]
    assert body["models"] == [] and "Invalid scope" in body["models_error"]


def test_settings_require_forwarded_identity():
    assert TestClient(app).get("/api/settings").status_code == 401
