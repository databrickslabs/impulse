import csv
import io
import os
import time
import tomllib
import urllib.request
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

TABLES = ["container_metrics", "container_tags", "channel_metrics", "channel_tags", "channels", "poi_channels"]
ROOT = Path(__file__).resolve().parent.parent


def framework_tag() -> str:
    lock = tomllib.loads((ROOT / "impulse.lock").read_text())
    return f"v{lock['version']}"


def fetch_csv(tag: str, table: str) -> bytes:
    url = f"https://raw.githubusercontent.com/databrickslabs/impulse/{tag}/demos/data/reporting/{table}.csv"
    with urllib.request.urlopen(url) as resp:
        return resp.read()


def parses(values: list[str], cast) -> bool:
    try:
        for v in values:
            cast(v)
    except ValueError:
        return False
    return bool(values)


def column_types(data: bytes) -> list[tuple[str, str]]:
    rows = list(csv.reader(io.StringIO(data.decode())))
    header, body = rows[0], rows[1:]
    types = []
    for i, name in enumerate(header):
        values = [r[i] for r in body if i < len(r) and r[i] != ""]
        if parses(values, int):
            types.append((name, "BIGINT"))
        elif parses(values, float):
            types.append((name, "DOUBLE"))
        else:
            types.append((name, "STRING"))
    return types


def run(w: WorkspaceClient, warehouse_id: str, statement: str) -> None:
    resp = w.statement_execution.execute_statement(warehouse_id=warehouse_id, statement=statement, wait_timeout="50s")
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        detail = resp.status.error.message if resp.status.error else resp.status.state
        raise RuntimeError(f"{statement[:120]}: {detail}")


def main() -> None:
    catalog, schema, warehouse_id = (os.environ[k] for k in ("IMPULSE_DEMO_CATALOG", "IMPULSE_DEMO_SCHEMA", "DATABRICKS_WAREHOUSE_ID"))
    w = WorkspaceClient()
    tag = framework_tag()
    fq_schema = f"`{catalog}`.`{schema}`"
    volume_dir = f"/Volumes/{catalog}/{schema}/raw/demo_{tag}"

    run(w, warehouse_id, f"CREATE SCHEMA IF NOT EXISTS {fq_schema}")
    run(w, warehouse_id, f"CREATE VOLUME IF NOT EXISTS {fq_schema}.`raw`")

    for table in TABLES:
        data = fetch_csv(tag, table)
        path = f"{volume_dir}/{table}.csv"
        w.files.upload(path, io.BytesIO(data), overwrite=True)
        columns = ", ".join(f"CAST(`{name}` AS {kind}) AS `{name}`" for name, kind in column_types(data))
        run(
            w,
            warehouse_id,
            f"CREATE OR REPLACE TABLE {fq_schema}.`{table}` AS SELECT {columns} "
            f"FROM read_files('{path}', format => 'csv', header => true, inferSchema => false)",
        )
        print(f"{table}: loaded from {tag}")


if __name__ == "__main__":
    main()
