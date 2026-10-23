import io
import json
import threading
import time

from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.workspace import ImportFormat
from pydantic import BaseModel

from server.settings import report_root, settings
from server.user import User

CACHE_SECONDS = 60


class Preferences(BaseModel):
    warehouse_id: str = ""
    serving_endpoint: str = ""


_cache: dict[str, tuple[float, Preferences]] = {}
_lock = threading.Lock()


def path(email: str) -> str:
    return f"{report_root(email)}/settings.json"


def get(user: User) -> Preferences:
    if not user.obo:
        return Preferences()
    now = time.monotonic()
    with _lock:
        hit = _cache.get(user.email)
    if hit and now - hit[0] < CACHE_SECONDS:
        return hit[1]
    try:
        prefs = Preferences.model_validate(json.loads(user.client.workspace.download(path(user.email)).read()))
    except (NotFound, PermissionDenied, ValueError):
        prefs = Preferences()
    with _lock:
        _cache[user.email] = (now, prefs)
    return prefs


def save(user: User, prefs: Preferences) -> Preferences:
    user.client.workspace.mkdirs(report_root(user.email))
    user.client.workspace.upload(path(user.email), io.BytesIO(prefs.model_dump_json().encode()), format=ImportFormat.AUTO, overwrite=True)
    with _lock:
        _cache[user.email] = (time.monotonic(), prefs)
    return prefs


def warehouse_id(user: User) -> str:
    return get(user).warehouse_id or settings.warehouse_id
