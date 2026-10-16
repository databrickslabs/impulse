import base64
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import (
    JobAccessControlRequest,
    JobEnvironment,
    JobPermissionLevel,
    JobSettings,
    NotebookTask,
    RunLifeCycleState,
    Source,
    Task,
)
from databricks.sdk.service.workspace import (
    ImportFormat,
    Language,
    WorkspaceObjectAccessControlRequest,
    WorkspaceObjectPermissionLevel,
)

from server import sql
from server.errors import ApiError
from server.settings import settings
from server.user import User

ACTIVE = {RunLifeCycleState.QUEUED, RunLifeCycleState.PENDING, RunLifeCycleState.RUNNING, RunLifeCycleState.TERMINATING, RunLifeCycleState.BLOCKED}
MAX_TRACE = 6000


@lru_cache(maxsize=1)
def runner() -> WorkspaceClient:
    return WorkspaceClient()


@lru_cache(maxsize=1)
def runner_identity() -> tuple[str, str]:
    me = runner().current_user.me()
    return me.user_name or "", me.display_name or me.user_name or ""


def _short(message: str) -> str:
    first = message.strip().splitlines()[0] if message.strip() else message
    return first[:400]


def _probe(principal: User, statement: str) -> str | None:
    try:
        sql.query(principal, statement, timeout_s=90)
        return None
    except ApiError as e:
        return _short(e.message)
    except DatabricksError as e:
        return _short(str(e).split(" Config:")[0])


def _checks(principal: User, tables: list[str], sink: dict) -> list[dict]:
    probe = sql.ident(sink["catalog"], sink["schema"], f"__impulse_probe_{uuid.uuid4().hex[:10]}")

    def read(table: str) -> dict | None:
        problem = _probe(principal, f"SELECT 1 FROM {sql.ident(*table.split('.'))} LIMIT 0")
        return {"object": table, "privilege": "SELECT", "message": problem} if problem else None

    def write() -> dict | None:
        problem = _probe(principal, f"CREATE TABLE {probe} (probe INT)")
        if problem:
            return {"object": f"{sink['catalog']}.{sink['schema']}", "privilege": "CREATE TABLE", "message": problem}
        _probe(principal, f"DROP TABLE IF EXISTS {probe}")
        return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(read, t) for t in tables] + [pool.submit(write)]
        return [f.result() for f in futures if f.result()]


def grant_script(principal: str, tables: list[str], sink: dict) -> str:
    who = sql.ident(principal)
    catalogs = list(dict.fromkeys([t.split(".")[0] for t in tables] + [sink["catalog"]]))
    schemas: dict[tuple[str, str], list[str]] = {}
    for t in tables:
        catalog, schema, _ = t.split(".")
        schemas.setdefault((catalog, schema), ["USE SCHEMA", "SELECT"])
    sink_privileges = schemas.setdefault((sink["catalog"], sink["schema"]), ["USE SCHEMA", "SELECT"])
    sink_privileges += [p for p in ("MODIFY", "CREATE TABLE") if p not in sink_privileges]
    lines = [f"GRANT USE CATALOG ON CATALOG {sql.ident(c)} TO {who};" for c in catalogs]
    lines += [f"GRANT {', '.join(p)} ON SCHEMA {sql.ident(c, s)} TO {who};" for (c, s), p in schemas.items()]
    return "\n".join(lines)


def preflight(user: User, config: dict) -> dict:
    tables = list(config["source"].values())
    sink = config["unity_sink"]
    app_id, app_name = runner_identity()
    app = User(app_id, runner())
    with ThreadPoolExecutor(max_workers=2) as pool:
        mine, theirs = pool.submit(_checks, user, tables, sink), pool.submit(_checks, app, tables, sink)
        user_problems, app_problems = mine.result(), theirs.result()
    return {
        "ok": not user_problems and not app_problems,
        "user": user_problems,
        "app": app_problems,
        "app_principal": {"id": app_id, "name": app_name},
        "grants": grant_script(app_id, tables, sink) if app_problems else None,
    }


def _job_name(email: str, report: str) -> str:
    return f"Impulse {report} ({email})"


def _tags(email: str, report: str) -> dict[str, str]:
    return {
        "impulse_app": settings.app_name or "impulse",
        "impulse_report": report,
        "impulse_user": email,
        "impulse_version": settings.framework["version"],
    }


def find_job(email: str, report: str) -> int | None:
    expected = _tags(email, report)
    for job in runner().jobs.list(name=_job_name(email, report)):
        tags = (job.settings.tags if job.settings else None) or {}
        if all(tags.get(k) == expected[k] for k in ("impulse_app", "impulse_report", "impulse_user")):
            return job.job_id
    return None


def _job_settings(email: str, report: str, notebook: str) -> JobSettings:
    lock = settings.framework
    return JobSettings(
        name=_job_name(email, report),
        tags=_tags(email, report),
        max_concurrent_runs=1,
        tasks=[
            Task(
                task_key="report",
                notebook_task=NotebookTask(notebook_path=notebook, source=Source.WORKSPACE),
                environment_key="impulse",
            )
        ],
        environments=[
            JobEnvironment(
                environment_key="impulse",
                spec=Environment(environment_version=lock["environment_version"], dependencies=[lock.get("wheel_override") or lock["wheel"]]),
            )
        ],
    )


def _folder(email: str, report: str) -> str:
    return f"/Users/{runner_identity()[0]}/Impulse/{email}/{report}"


def _publish(email: str, report: str, notebook: str) -> str:
    w = runner()
    app_id, _ = runner_identity()
    folder = _folder(email, report)
    path = f"{folder}/report"
    w.workspace.mkdirs(folder)
    w.workspace.import_(
        path=path,
        content=base64.b64encode(notebook.encode()).decode(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    if email != app_id:
        object_id = w.workspace.get_status(folder).object_id
        w.workspace.update_permissions(
            "directories",
            str(object_id),
            access_control_list=[WorkspaceObjectAccessControlRequest(user_name=email, permission_level=WorkspaceObjectPermissionLevel.CAN_READ)],
        )
    return path


def submit(user: User, report: str, notebook: str) -> dict:
    w = runner()
    path = _publish(user.email, report, notebook)
    job_settings = _job_settings(user.email, report, path)
    job_id = find_job(user.email, report)
    if job_id is None:
        job_id = w.jobs.create(**{k: getattr(job_settings, k) for k in ("name", "tags", "max_concurrent_runs", "tasks", "environments")}).job_id
    else:
        w.jobs.reset(job_id=job_id, new_settings=job_settings)
    if user.email != runner_identity()[0]:
        w.jobs.update_permissions(
            str(job_id),
            access_control_list=[JobAccessControlRequest(user_name=user.email, permission_level=JobPermissionLevel.CAN_MANAGE_RUN)],
        )
    run_id = w.jobs.run_now(job_id=job_id).run_id
    return {"job_id": job_id, "run_id": run_id}


def _summary(run) -> dict:
    state = run.state
    life = state.life_cycle_state if state else None
    return {
        "run_id": run.run_id,
        "active": life in ACTIVE,
        "state": life.value if life else None,
        "result": state.result_state.value if state and state.result_state else None,
        "message": state.state_message if state else "",
        "start_time": run.start_time,
        "end_time": run.end_time or None,
        "url": run.run_page_url,
    }


def history(user: User, report: str, limit: int = 10) -> list[dict]:
    job_id = find_job(user.email, report)
    if job_id is None:
        return []
    return [_summary(r) for r in runner().jobs.list_runs(job_id=job_id, limit=limit)]


def _owned_run(user: User, report: str, run_id: int):
    job_id = find_job(user.email, report)
    try:
        run = runner().jobs.get_run(run_id)
    except NotFound:
        run = None
    if job_id is None or run is None or run.job_id != job_id:
        raise ApiError(404, "not_found", f"Run {run_id} not found for report {report}")
    return run


def detail(user: User, report: str, run_id: int) -> dict:
    run = _owned_run(user, report, run_id)
    result = _summary(run)
    result["error"] = None
    result["trace"] = None
    if not result["active"] and run.tasks:
        output = runner().jobs.get_run_output(run.tasks[0].run_id)
        result["error"] = output.error
        result["trace"] = (output.error_trace or "")[-MAX_TRACE:] or None
    return result


def cancel(user: User, report: str, run_id: int) -> dict:
    _owned_run(user, report, run_id)
    runner().jobs.cancel_run(run_id)
    return {"cancelled": run_id}


def delete(email: str, report: str) -> None:
    w = runner()
    job_id = find_job(email, report)
    if job_id is not None:
        w.jobs.cancel_all_runs(job_id=job_id)
        w.jobs.delete(job_id)
    try:
        w.workspace.delete(_folder(email, report), recursive=True)
    except NotFound:
        pass
