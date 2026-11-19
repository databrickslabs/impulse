from concurrent.futures import ThreadPoolExecutor

from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.serving import EndpointStateReady
from databricks.sdk.service.sql import State
from fastapi import APIRouter, Depends

from server import preferences
from server.agent.loop import PREFERRED, default_model
from server.errors import ApiError
from server.preferences import Preferences
from server.settings import settings
from server.user import User, current_user

router = APIRouter(prefix="/api/settings")

CHAT_TASK = "llm/v1/chat"


def _message(e: DatabricksError) -> str:
    return str(e).split(" Config:")[0]


def _state(user: User) -> dict:
    return {**preferences.get(user).model_dump(), "default_warehouse_id": settings.warehouse_id, "default_model": default_model(user)}


@router.get("")
def get_settings(user: User = Depends(current_user)) -> dict:
    return _state(user)


@router.put("")
def put_settings(body: Preferences, user: User = Depends(current_user)) -> dict:
    if body.warehouse_id:
        try:
            user.client.warehouses.get(body.warehouse_id)
        except DatabricksError as e:
            raise ApiError(400, "invalid_warehouse", f"SQL warehouse {body.warehouse_id} is not available to you: {_message(e)}") from None
    if body.serving_endpoint:
        try:
            user.client.serving_endpoints.get(body.serving_endpoint)
        except DatabricksError as e:
            raise ApiError(400, "invalid_model", f"Serving endpoint {body.serving_endpoint} is not available to you: {_message(e)}") from None
    preferences.save(user, body)
    return _state(user)


def _warehouses(user: User) -> list[dict]:
    found = [w for w in user.client.warehouses.list() if w.id and w.state not in (State.DELETED, State.DELETING)]
    listed = [{"id": w.id, "name": w.name or w.id, "state": w.state.value if w.state else "", "size": w.cluster_size or ""} for w in found]
    return sorted(listed, key=lambda w: w["name"].lower())


def _models(user: User) -> list[dict]:
    rank = {name: i for i, name in enumerate(PREFERRED)}
    found = sorted((e for e in user.client.serving_endpoints.list() if e.name and e.task == CHAT_TASK), key=lambda e: (rank.get(e.name, len(rank)), e.name))
    return [{"name": e.name, "ready": bool(e.state and e.state.ready == EndpointStateReady.READY)} for e in found]


def _listed(load, user: User) -> tuple[list[dict], str | None]:
    try:
        return load(user), None
    except DatabricksError as e:
        return [], _message(e)


@router.get("/options")
def options(user: User = Depends(current_user)) -> dict:
    with ThreadPoolExecutor(max_workers=2) as pool:
        warehouses, models = pool.submit(_listed, _warehouses, user), pool.submit(_listed, _models, user)
        (found, warehouses_error), (chat, models_error) = warehouses.result(), models.result()
    return {"warehouses": found, "models": chat, "warehouses_error": warehouses_error, "models_error": models_error}
