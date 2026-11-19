import time

from databricks.sdk.service.sql import StatementParameterListItem, StatementState

from server import preferences
from server.errors import ApiError, from_sql_message
from server.user import User

RUNNING = (StatementState.PENDING, StatementState.RUNNING)


def ident(*parts: str) -> str:
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


def query(user: User, statement: str, parameters: dict[str, str] | None = None, timeout_s: int = 100) -> list[dict]:
    w = user.client
    params = [StatementParameterListItem(name=k, value=v) for k, v in (parameters or {}).items()] or None
    resp = w.statement_execution.execute_statement(
        warehouse_id=preferences.warehouse_id(user), statement=statement, parameters=params, wait_timeout="30s"
    )
    deadline = time.monotonic() + timeout_s
    while resp.status.state in RUNNING:
        if time.monotonic() > deadline:
            w.statement_execution.cancel_execution(resp.statement_id)
            raise ApiError(504, "sql_timeout", "Query did not finish in time")
        time.sleep(1)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        raise from_sql_message(resp.status.error.message if resp.status.error else resp.status.state.value)
    columns = [c.name for c in resp.manifest.schema.columns]
    rows = list(resp.result.data_array or []) if resp.result else []
    chunk = resp.result.next_chunk_index if resp.result else None
    while chunk is not None:
        part = w.statement_execution.get_statement_result_chunk_n(resp.statement_id, chunk)
        rows.extend(part.data_array or [])
        chunk = part.next_chunk_index
    return [dict(zip(columns, r)) for r in rows]


def warmup(user: User) -> None:
    user.client.statement_execution.execute_statement(
        warehouse_id=preferences.warehouse_id(user), statement="SELECT 1", wait_timeout="0s"
    )
