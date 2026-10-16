from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from server import catalog as uc
from server import datamodel
from server.errors import ApiError
from server.spec import Window
from server.user import User, current_user

router = APIRouter(prefix="/api/data")


class ChannelQuery(BaseModel):
    catalog: str
    schema_: str = Field(alias="schema")
    uuts: list[str] = Field(default_factory=list)
    window: Window = Field(default_factory=Window)


class ModelText(BaseModel):
    yaml: str
    catalog: str = ""
    schema_: str = Field("", alias="schema")


@router.get("/catalogs")
def catalogs(user: User = Depends(current_user)) -> dict:
    return {"catalogs": uc.catalogs(user)}


@router.get("/schemas")
def schemas(catalog: str, user: User = Depends(current_user)) -> dict:
    return {"schemas": uc.schemas(user, catalog)}


@router.get("/source")
def source(catalog: str, schema: str, user: User = Depends(current_user)) -> dict:
    return datamodel.resolve(user, datamodel.load(user), catalog, schema).dump()


@router.get("/uuts")
def uuts(catalog: str, schema: str, user: User = Depends(current_user)) -> dict:
    model = datamodel.load(user)
    found = datamodel.resolve(user, model, catalog, schema)
    return {"uuts": datamodel.uuts(user, model, found), "issues": found.issues}


@router.post("/channels")
def channels(body: ChannelQuery, user: User = Depends(current_user)) -> dict:
    model = datamodel.load(user)
    found = datamodel.resolve(user, model, body.catalog, body.schema_)
    return {"channels": datamodel.channels(user, model, found, body.uuts, body.window), "issues": found.issues}


@router.get("/model")
def get_model(user: User = Depends(current_user)) -> dict:
    text = datamodel.read_text(user)
    error = None
    if text is not None:
        try:
            datamodel.parse(text)
        except ApiError as e:
            error = e.message
    return {
        "yaml": text or "",
        "reference": datamodel.reference(),
        "path": datamodel.config_path(),
        "exists": text is not None,
        "error": error,
    }


@router.post("/model/check")
def check_model(body: ModelText, user: User = Depends(current_user)) -> dict:
    model = datamodel.parse(body.yaml)
    catalog = body.catalog or model.silver_catalog
    schema = body.schema_ or model.silver_schema
    if not (catalog and schema):
        return {"source": None, "uuts": None, "issues": ["Set silver_catalog and silver_schema, or choose a schema to test against"]}
    found = datamodel.resolve(user, model, catalog, schema)
    count = None if found.issues else len(datamodel.uuts(user, model, found))
    return {"source": found.dump(), "uuts": count, "issues": found.issues}


@router.put("/model")
def put_model(body: ModelText, user: User = Depends(current_user)) -> dict:
    datamodel.save(user, body.yaml)
    return {"saved": True, "path": datamodel.config_path()}
