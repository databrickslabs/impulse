import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from server import errors
from server.routes import chat, data, meta, reports, runs, settings

app = FastAPI(title="Impulse")
errors.register(app)
for module in (meta, data, reports, runs, chat, settings):
    app.include_router(module.router)

DIST = os.path.join(os.path.dirname(__file__), "frontend", "dist")

if os.path.isdir(DIST):
    app.mount("/assets", StaticFiles(directory=os.path.join(DIST, "assets")), name="assets")

    @app.get("/{path:path}")
    def index(path: str) -> FileResponse:
        if path.startswith("api/"):
            raise HTTPException(404)
        return FileResponse(os.path.join(DIST, "index.html"))
