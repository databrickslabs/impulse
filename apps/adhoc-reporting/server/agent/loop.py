import io
import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field

from databricks.sdk.errors import DatabricksError, NotFound
from databricks.sdk.service.serving import EndpointStateReady
from databricks.sdk.service.workspace import ImportFormat

from server import datamodel, preferences, store
from server.agent import prompt, tools
from server.errors import ApiError
from server.settings import report_root
from server.user import User

logger = logging.getLogger(__name__)

MAX_ROUNDS = 12
MAX_TOOL_RESULT = 24000
HISTORY_TURNS = 8
HISTORY_TOOL_RESULT = 1500
TURN_TTL = 3600
MODEL_TTL = 600
PREFERRED = (
    "databricks-claude-sonnet-5-5",
    "databricks-claude-sonnet-5",
    "databricks-claude-sonnet-4-6",
    "databricks-claude-sonnet-4-5",
    "databricks-gpt-5-5",
    "databricks-gpt-5",
)
LABELS = {
    "list_reports": "Listing reports",
    "create_report": "Creating report",
    "open_report": "Opening report",
    "get_report": "Reading report",
    "list_catalogs": "Listing catalogs",
    "list_schemas": "Listing schemas",
    "set_data": "Setting data",
    "list_uuts": "Listing units under test",
    "set_uuts": "Selecting units under test",
    "search_channels": "Searching channels",
    "add_channel_signals": "Adding signals",
    "add_derived_signal": "Adding derived signal",
    "remove_signal": "Removing signal",
    "add_event": "Adding event",
    "remove_event": "Removing event",
    "add_histogram": "Adding histogram",
    "add_heatmap": "Adding heatmap",
    "add_statistics": "Adding statistics",
    "add_point_values": "Adding values at points",
    "remove_aggregation": "Removing aggregation",
    "validate_report": "Validating",
    "preview_code": "Generating notebook",
    "run_report": "Starting run",
    "get_run_status": "Checking run",
    "get_results": "Reading results",
    "run_sql": "Running SQL",
    "goto_step": "Opening step",
    "load_skill": "Loading skill",
    "read_skill_reference": "Reading skill reference",
}


@dataclass
class Turn:
    id: str
    email: str
    session: str
    events: list[dict] = field(default_factory=list)
    done: bool = False
    created: float = field(default_factory=time.time)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def emit(self, event: dict) -> None:
        with self.lock:
            self.events.append({**event, "seq": len(self.events)})


_turns: dict[str, Turn] = {}
_active: dict[tuple[str, str], str] = {}
_models: dict[str, tuple[float, str]] = {}
_lock = threading.Lock()


def _history_path(email: str, report: str | None) -> str:
    return f"{report_root(email)}/{report}/chat.json" if report else f"{report_root(email)}/_chat.json"


def load_history(user: User, report: str | None) -> list[dict]:
    try:
        return json.loads(user.client.workspace.download(_history_path(user.email, report)).read())
    except NotFound:
        return []
    except ValueError:
        return []


def clip(text: str, limit: int) -> str:
    return text if len(text) <= limit else f"{text[:limit]} [truncated]"


def _save_history(user: User, report: str | None, messages: list[dict]) -> None:
    starts = [i for i, m in enumerate(messages) if m["role"] == "user"]
    kept = messages[starts[-HISTORY_TURNS]:] if len(starts) > HISTORY_TURNS else messages
    trimmed = [
        {**m, "content": clip(m["content"], HISTORY_TOOL_RESULT)} if m["role"] == "tool" and isinstance(m.get("content"), str) else m for m in kept
    ]
    path = _history_path(user.email, report)
    user.client.workspace.mkdirs(path.rsplit("/", 1)[0])
    user.client.workspace.upload(path, io.BytesIO(json.dumps(trimmed).encode()), format=ImportFormat.AUTO, overwrite=True)


def clear_history(user: User, report: str | None) -> None:
    try:
        user.client.workspace.delete(_history_path(user.email, report))
    except NotFound:
        pass


def default_model(user: User) -> str | None:
    now = time.monotonic()
    with _lock:
        hit = _models.get(user.email)
    if hit and now - hit[0] < MODEL_TTL:
        return hit[1]
    for name in PREFERRED:
        try:
            endpoint = user.client.serving_endpoints.get(name)
        except DatabricksError:
            continue
        if endpoint.state and endpoint.state.ready == EndpointStateReady.READY:
            with _lock:
                _models[user.email] = (now, name)
            return name
    return None


def pick_model(user: User) -> str:
    model = preferences.get(user).serving_endpoint or default_model(user)
    if not model:
        raise ApiError(503, "no_model", "No chat model endpoint is available to you")
    return model


def _text(content: object) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(part.get("text", "") for part in content if isinstance(part, dict))
    return ""


def _summary(result: dict) -> str:
    if "error" in result:
        return str(result["error"])[:300]
    if "issues" in result and isinstance(result["issues"], list) and result["issues"]:
        return f"{len(result['issues'])} open issue{'s' if len(result['issues']) > 1 else ''}"
    return ""


def _report_summary(user: User, report: str | None) -> dict | None:
    if not report:
        return None
    try:
        return tools.summary(store.get(user, report))
    except ApiError:
        return None


def _run(turn: Turn, user: User, report: str | None, message: str) -> None:
    ctx = tools.Context(user=user, report=report, emit=turn.emit)
    history = load_history(user, report)
    messages = [*history, {"role": "user", "content": message}]
    try:
        model = pick_model(user)
        schemas = tools.schemas()
        time_unit = datamodel.load(user).time_unit
        for _ in range(MAX_ROUNDS):
            system = {"role": "system", "content": prompt.system(_report_summary(user, ctx.report), time_unit)}
            response = user.client.api_client.do(
                "POST",
                f"/serving-endpoints/{model}/invocations",
                body={"messages": [system, *messages], "tools": schemas, "max_tokens": 4000},
            )
            reply = response["choices"][0]["message"]
            text = _text(reply.get("content"))
            calls = reply.get("tool_calls") or []
            entry: dict = {"role": "assistant", "content": text or None}
            if calls:
                entry["tool_calls"] = [{"id": c["id"], "type": "function", "function": c["function"]} for c in calls]
            messages.append(entry)
            if text:
                turn.emit({"type": "text", "text": text})
            if not calls:
                break
            for call in calls:
                name = call["function"]["name"]
                turn.emit({"type": "tool", "id": call["id"], "name": name, "label": LABELS.get(name, name), "status": "running"})
                result = tools.execute(ctx, name, call["function"].get("arguments") or "{}")
                turn.emit(
                    {
                        "type": "tool",
                        "id": call["id"],
                        "name": name,
                        "label": LABELS.get(name, name),
                        "status": "error" if "error" in result else "done",
                        "detail": _summary(result),
                    }
                )
                messages.append({"role": "tool", "tool_call_id": call["id"], "content": clip(json.dumps(result, default=str), MAX_TOOL_RESULT)})
        else:
            turn.emit({"type": "text", "text": "I stopped after many steps. Tell me how to continue."})
        if ctx.report and ctx.report != report:
            _save_history(user, ctx.report, [*load_history(user, ctx.report), *messages[len(history):]])
        else:
            _save_history(user, report, messages)
    except ApiError as e:
        turn.emit({"type": "error", "message": e.message})
    except DatabricksError as e:
        turn.emit({"type": "error", "message": str(e).split(" Config:")[0]})
    except Exception as e:
        logger.exception("Assistant turn failed")
        turn.emit({"type": "error", "message": f"The assistant failed: {e}"})
    finally:
        turn.done = True
        turn.emit({"type": "done"})


def start(user: User, report: str | None, message: str) -> Turn:
    session = report or "_home"
    with _lock:
        for turn_id in [t for t, turn in _turns.items() if time.time() - turn.created > TURN_TTL]:
            _turns.pop(turn_id, None)
        active = _active.get((user.email, session))
        if active and active in _turns and not _turns[active].done:
            raise ApiError(409, "busy", "The assistant is still answering")
        turn = Turn(id=uuid.uuid4().hex, email=user.email, session=session)
        _turns[turn.id] = turn
        _active[(user.email, session)] = turn.id
    threading.Thread(target=_run, args=(turn, user, report, message), daemon=True).start()
    return turn


def events(user: User, turn_id: str, after: int) -> dict:
    turn = _turns.get(turn_id)
    if turn is None or turn.email != user.email:
        raise ApiError(404, "not_found", "Turn not found")
    with turn.lock:
        return {"events": turn.events[after:], "done": turn.done}
