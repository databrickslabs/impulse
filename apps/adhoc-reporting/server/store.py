import io
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat, ObjectType
from pydantic import ValidationError

from server.errors import ApiError
from server.settings import report_root
from server.spec import ReportSpec
from server.user import User


def _folder(user: User, name: str) -> str:
    return f"{report_root(user.email)}/{name}"


def _spec_path(user: User, name: str) -> str:
    return f"{_folder(user, name)}/spec.json"


def _read(user: User, name: str) -> ReportSpec:
    raw = user.client.workspace.download(_spec_path(user, name)).read()
    try:
        return ReportSpec.model_validate(json.loads(raw))
    except (ValidationError, ValueError):
        raise ApiError(422, "unreadable_report", f"Report {name} was saved in a format this app version cannot read") from None


def _try_read(user: User, name: str) -> ReportSpec | None:
    try:
        return _read(user, name)
    except NotFound:
        return None


def get(user: User, name: str) -> ReportSpec:
    spec = _try_read(user, name)
    if spec is None:
        raise ApiError(404, "not_found", f"Report {name} not found")
    return spec


def list_reports(user: User) -> list[dict]:
    try:
        objects = list(user.client.workspace.list(report_root(user.email)))
    except NotFound:
        return []
    names = [
        o.path.rsplit("/", 1)[-1]
        for o in objects
        if o.object_type == ObjectType.DIRECTORY and not o.path.rsplit("/", 1)[-1].startswith("_")
    ]
    with ThreadPoolExecutor(max_workers=8) as pool:
        summaries = [s for s in pool.map(lambda n: _summary(user, n), names) if s]
    return sorted(summaries, key=lambda s: s["updated_at"], reverse=True)


def _summary(user: User, name: str) -> dict | None:
    try:
        spec = _try_read(user, name)
    except ApiError as e:
        return {"name": name, "description": e.message, "updated_at": "", "uuts": 0, "aggregations": 0, "error": e.code}
    if spec is None:
        return None
    return {
        "name": spec.name,
        "description": spec.description,
        "updated_at": spec.updated_at,
        "uuts": len(spec.uuts),
        "aggregations": len(spec.aggregations),
    }


def save(user: User, spec: ReportSpec, expected_revision: int | None) -> ReportSpec:
    current = _try_read(user, spec.name)
    if current is not None and expected_revision is not None and current.revision != expected_revision:
        raise ApiError(409, "conflict", "This report was changed elsewhere. Reload to continue.")
    spec.revision = (current.revision if current else 0) + 1
    spec.owner = current.owner if current else user.email
    spec.updated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    user.client.workspace.mkdirs(_folder(user, spec.name))
    user.client.workspace.upload(
        _spec_path(user, spec.name),
        io.BytesIO(json.dumps(spec.dump(), indent=2).encode()),
        format=ImportFormat.AUTO,
        overwrite=True,
    )
    return spec


def create(user: User, spec: ReportSpec) -> ReportSpec:
    if _try_read(user, spec.name) is not None:
        raise ApiError(409, "exists", f"Report {spec.name} already exists")
    return save(user, spec, expected_revision=None)


def delete(user: User, name: str) -> None:
    try:
        user.client.workspace.get_status(_folder(user, name))
    except NotFound:
        raise ApiError(404, "not_found", f"Report {name} not found") from None
    user.client.workspace.delete(_folder(user, name), recursive=True)
