from fastapi import APIRouter, Depends

from server import codegen, datamodel, results, runs, store
from server.user import User, current_user

router = APIRouter(prefix="/api/reports/{name}")


def _generated(user: User, name: str):
    spec = store.get(user, name)
    model = datamodel.load(user)
    source = datamodel.resolve(user, model, spec.source.catalog, spec.source.schema_)
    return spec, codegen.generate(spec, source, model)


@router.post("/preflight")
def preflight(name: str, user: User = Depends(current_user)) -> dict:
    _, generated = _generated(user, name)
    return runs.preflight(user, generated.config)


@router.post("/runs")
def start(name: str, user: User = Depends(current_user)) -> dict:
    _, generated = _generated(user, name)
    checked = runs.preflight(user, generated.config)
    if not checked["ok"]:
        return {"preflight": checked, "run": None}
    return {"preflight": checked, "run": runs.submit(user, name, generated.notebook)}


@router.get("/runs")
def history(name: str, user: User = Depends(current_user)) -> dict:
    return {"runs": runs.history(user, name)}


@router.get("/runs/{run_id}")
def detail(name: str, run_id: int, user: User = Depends(current_user)) -> dict:
    return runs.detail(user, name, run_id)


@router.post("/runs/{run_id}/cancel")
def cancel(name: str, run_id: int, user: User = Depends(current_user)) -> dict:
    return runs.cancel(user, name, run_id)


@router.get("/results")
def report_results(name: str, user: User = Depends(current_user)) -> dict:
    return results.results(user, store.get(user, name), datamodel.load(user))
