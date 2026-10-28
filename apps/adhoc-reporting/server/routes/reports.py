from typing import Annotated

from fastapi import APIRouter, Depends, Path
from pydantic import BaseModel

from server import codegen, datamodel, runs, store
from server.datamodel import DataModel, Source
from server.errors import ApiError
from server.settings import settings
from server.spec import NAME_RE, Location, ReportSpec, Sink
from server.user import User, current_user

router = APIRouter(prefix="/api/reports")

ReportName = Annotated[str, Path(pattern=NAME_RE.pattern)]


class NewReport(BaseModel):
    name: str
    description: str = ""


def _source(user: User, model: DataModel, spec: ReportSpec) -> Source | None:
    if not (spec.source.catalog and spec.source.schema_):
        return None
    return datamodel.resolve(user, model, spec.source.catalog, spec.source.schema_)


@router.get("")
def list_reports(user: User = Depends(current_user)) -> dict:
    return {"reports": store.list_reports(user)}


@router.post("")
def create_report(body: NewReport, user: User = Depends(current_user)) -> dict:
    model = datamodel.load(user)
    spec = ReportSpec(
        name=body.name,
        description=body.description,
        source=Location(catalog=model.silver_catalog, schema=model.silver_schema),
        sink=Sink(
            catalog=model.gold_catalog or model.silver_catalog,
            schema=model.gold_schema or model.silver_schema,
            table_prefix=body.name,
        ),
        framework_version=settings.framework["version"],
    )
    return store.create(user, spec).dump()


@router.get("/{name}")
def get_report(name: ReportName, user: User = Depends(current_user)) -> dict:
    return store.get(user, name).dump()


@router.put("/{name}")
def save_report(name: ReportName, spec: ReportSpec, user: User = Depends(current_user)) -> dict:
    if spec.name != name:
        raise ApiError(400, "name_mismatch", "Report name cannot change")
    return store.save(user, spec, expected_revision=spec.revision).dump()


@router.delete("/{name}")
def delete_report(name: ReportName, user: User = Depends(current_user)) -> dict:
    store.delete(user, name)
    runs.delete(user.email, name)
    return {"deleted": name}


@router.post("/{name}/check")
def check_report(name: ReportName, spec: ReportSpec, user: User = Depends(current_user)) -> dict:
    model = datamodel.load(user)
    return {"issues": [i.model_dump() for i in codegen.issues(spec, _source(user, model, spec), model)]}
