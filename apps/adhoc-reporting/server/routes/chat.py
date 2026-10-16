from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from server import store
from server.agent import loop
from server.errors import ApiError
from server.spec import NAME_RE
from server.user import User, current_user

router = APIRouter(prefix="/api/chat")


class TurnRequest(BaseModel):
    message: str = Field(min_length=1, max_length=8000)
    report: str | None = None


def _report(user: User, report: str | None) -> str | None:
    if not report:
        return None
    if not NAME_RE.match(report):
        raise ApiError(400, "invalid_report", "Invalid report name")
    store.get(user, report)
    return report


def _display(messages: list[dict]) -> list[dict]:
    shown = []
    for m in messages:
        if m["role"] == "user":
            shown.append({"role": "user", "text": m["content"]})
        elif m["role"] == "assistant":
            calls = [loop.LABELS.get(c["function"]["name"], c["function"]["name"]) for c in m.get("tool_calls") or []]
            if m.get("content") or calls:
                shown.append({"role": "assistant", "text": m.get("content") or "", "tools": calls})
    return shown


@router.post("/turns")
def start_turn(body: TurnRequest, user: User = Depends(current_user)) -> dict:
    return {"turn_id": loop.start(user, _report(user, body.report), body.message).id}


@router.get("/turns/{turn_id}")
def turn_events(turn_id: str, after: int = 0, user: User = Depends(current_user)) -> dict:
    return loop.events(user, turn_id, after)


@router.get("/history")
def history(report: str | None = None, user: User = Depends(current_user)) -> dict:
    return {"messages": _display(loop.load_history(user, _report(user, report)))}


@router.delete("/history")
def clear(report: str | None = None, user: User = Depends(current_user)) -> dict:
    loop.clear_history(user, _report(user, report))
    return {"cleared": True}
