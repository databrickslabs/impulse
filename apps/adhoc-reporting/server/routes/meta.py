from fastapi import APIRouter, Depends

from server import preferences, sql
from server.settings import settings
from server.user import User, current_user

router = APIRouter(prefix="/api/meta")


@router.get("/health")
def health() -> dict:
    return {"status": "ok", "impulse_version": settings.framework["version"]}


@router.get("/me")
def me(user: User = Depends(current_user)) -> dict:
    return {
        "email": user.email,
        "app": settings.app_name,
        "impulse_version": settings.framework["version"],
        "warehouse_configured": bool(preferences.warehouse_id(user)),
    }


@router.post("/warmup")
def warmup(user: User = Depends(current_user)) -> dict:
    sql.warmup(user)
    return {"started": True}
