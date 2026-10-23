import argparse
import time
from concurrent.futures import ThreadPoolExecutor

from databricks.sdk import WorkspaceClient

from server import store
from server.agent import loop
from server.errors import ApiError
from server.spec import ReportSpec
from server.user import User
from tests.evals.scenarios import SCENARIOS, Scenario


def run(user: User, scenario: Scenario) -> tuple[str, list[str], float, int]:
    name = f"zz_eval_{scenario.name}"
    try:
        store.delete(user, name)
    except ApiError:
        pass
    store.create(user, ReportSpec(name=name, sink={"table_prefix": name}))
    started = time.time()
    events: list[dict] = []
    text = ""
    for message in scenario.turns:
        turn = loop.Turn(id=f"{name}-{len(events)}", email=user.email, session=name)
        loop._run(turn, user, name, message)
        events += turn.events
        text = "\n".join(e["text"] for e in turn.events if e["type"] == "text") or text
    errors = [e["message"] for e in events if e["type"] == "error"]
    problems = errors + scenario.check(store.get(user, name), text, events)
    tools = sum(1 for e in events if e["type"] == "tool" and e["status"] != "running")
    store.delete(user, name)
    return scenario.name, problems, time.time() - started, tools


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", action="append", default=[])
    args = parser.parse_args()
    w = WorkspaceClient()
    user = User(w.current_user.me().user_name, w)
    chosen = [s for s in SCENARIOS if not args.only or s.name in args.only]
    with ThreadPoolExecutor(max_workers=4) as pool:
        outcomes = list(pool.map(lambda s: run(user, s), chosen))
    failed = 0
    for name, problems, seconds, tools in outcomes:
        failed += bool(problems)
        print(f"{'PASS' if not problems else 'FAIL'}  {name:16} {seconds:5.1f}s {tools:3d} tools  {'; '.join(problems)}")
    print(f"{len(outcomes) - failed}/{len(outcomes)} passed")
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
