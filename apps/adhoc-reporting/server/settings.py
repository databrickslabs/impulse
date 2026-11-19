import os
import tomllib
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Settings:
    host: str
    warehouse_id: str
    app_name: str
    shared_root: str
    framework: dict


def load() -> Settings:
    host = os.environ.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    return Settings(
        host=host,
        warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID", ""),
        app_name=os.environ.get("DATABRICKS_APP_NAME", ""),
        shared_root=os.environ.get("IMPULSE_SHARED_ROOT", "/Workspace/Shared/Impulse"),
        framework=tomllib.loads((ROOT / "impulse.lock").read_text()),
    )


settings = load()


def report_root(email: str) -> str:
    return f"/Users/{email}/Impulse"
