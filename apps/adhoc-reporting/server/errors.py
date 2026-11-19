from databricks.sdk.errors import DatabricksError, NotFound, PermissionDenied
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def from_sql_message(message: str) -> ApiError:
    upper = message.upper()
    if "INSUFFICIENT_PERMISSIONS" in upper or "PERMISSION_DENIED" in upper:
        return ApiError(403, "missing_privilege", message)
    if "NOT_FOUND" in upper or "DOES NOT EXIST" in upper:
        return ApiError(404, "not_found", message)
    return ApiError(400, "sql_error", message)


def _body(code: str, message: str) -> dict:
    return {"error": {"code": code, "message": message}}


def register(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    def api_error(request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(status_code=exc.status, content=_body(exc.code, exc.message))

    @app.exception_handler(PermissionDenied)
    def permission_denied(request: Request, exc: PermissionDenied) -> JSONResponse:
        return JSONResponse(status_code=403, content=_body("missing_privilege", str(exc).split(" Config:")[0]))

    @app.exception_handler(NotFound)
    def not_found(request: Request, exc: NotFound) -> JSONResponse:
        return JSONResponse(status_code=404, content=_body("not_found", str(exc).split(" Config:")[0]))

    @app.exception_handler(DatabricksError)
    def databricks_error(request: Request, exc: DatabricksError) -> JSONResponse:
        return JSONResponse(status_code=502, content=_body("databricks_error", str(exc).split(" Config:")[0]))
