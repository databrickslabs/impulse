import argparse
import base64
import json
import os
import time
import tomllib
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import JobEnvironment, NotebookTask, RunLifeCycleState, Source, Task
from databricks.sdk.service.workspace import ImportFormat, Language

ROOT = Path(__file__).resolve().parent.parent
TERMINAL = {RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR}


def generated_notebook(w: WorkspaceClient, email: str, spec_path: str, persist: bool) -> bytes:
    from server import codegen, datamodel
    from server.spec import ReportSpec
    from server.user import User

    spec = ReportSpec.model_validate(json.loads(Path(spec_path).read_text()))
    catalog, schema = os.environ["IMPULSE_DEMO_CATALOG"], os.environ["IMPULSE_DEMO_SCHEMA"]
    spec.source.catalog, spec.source.schema_ = catalog, schema
    spec.sink.catalog, spec.sink.schema_ = catalog, schema
    model = datamodel.DataModel()
    source = datamodel.resolve(User(email, w), model, spec.source.catalog, spec.source.schema_)
    return codegen.generate(spec, source, model, sinkless=not persist).notebook.encode()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--notebook")
    target.add_argument("--spec")
    parser.add_argument("--persist", action="store_true")
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()

    lock = tomllib.loads((ROOT / "impulse.lock").read_text())
    w = WorkspaceClient()
    email = w.current_user.me().user_name
    content = generated_notebook(w, email, args.spec, args.persist) if args.spec else Path(args.notebook).read_bytes()
    folder = f"/Users/{email}/Impulse/_smoke"
    notebook_path = f"{folder}/{args.name}"
    w.workspace.mkdirs(folder)
    w.workspace.import_(
        path=notebook_path,
        content=base64.b64encode(content).decode(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )

    job = w.jobs.create(
        name=f"impulse-adhoc-reporting smoke {args.name}",
        tasks=[
            Task(
                task_key="report",
                notebook_task=NotebookTask(notebook_path=notebook_path, source=Source.WORKSPACE),
                environment_key="impulse",
            )
        ],
        environments=[
            JobEnvironment(
                environment_key="impulse",
                spec=Environment(environment_version=lock["environment_version"], dependencies=[lock.get("wheel_override") or lock["wheel"]]),
            )
        ],
        tags={"impulse_app": "impulse-adhoc-reporting", "impulse_version": lock["version"]},
    )
    run_id = w.jobs.run_now(job_id=job.job_id).run_id
    print(f"job_id={job.job_id} run_id={run_id} run_as={w.jobs.get(job.job_id).run_as_user_name}", flush=True)

    started = time.time()
    run = w.jobs.get_run(run_id)
    while run.state.life_cycle_state not in TERMINAL:
        time.sleep(10)
        run = w.jobs.get_run(run_id)

    result = run.state.result_state.value if run.state.result_state else None
    print(f"result={result} seconds={int(time.time() - started)}")
    output = w.jobs.get_run_output(run.tasks[0].run_id)
    if output.notebook_output:
        print(f"notebook_result={output.notebook_output.result}")
    if output.error:
        print(f"error={output.error}")
        print(output.error_trace or "")
    if not args.keep:
        w.jobs.delete(job.job_id)
    raise SystemExit(0 if result == "SUCCESS" else 1)


if __name__ == "__main__":
    main()
