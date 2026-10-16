import json
from types import SimpleNamespace

import pytest

from server.agent import prompt, tools
from server.agent.loop import LABELS, clip
from server.spec import ReportSpec

CTX = tools.Context(user=None, report=None, emit=lambda e: None)


def test_every_tool_has_a_label_and_valid_schema():
    assert set(tools.TOOLS) == set(LABELS)
    for schema in tools.schemas():
        function = schema["function"]
        assert function["parameters"]["type"] == "object"
        assert len(function["description"]) > 15
        json.dumps(schema)


@pytest.mark.parametrize(
    ("name", "arguments", "message"),
    [
        ("nope", "{}", "Unknown tool"),
        ("get_report", "not json", "JSON object"),
        ("set_uuts", '{"uuts": "x"}', "Invalid arguments"),
        ("get_report", "{}", "No report is open"),
        ("add_event", '{"name": "a", "kind": "sometimes", "expr": "x"}', "Invalid arguments"),
        ("run_sql", '{"statement": "DROP TABLE t"}', "Only SELECT"),
        ("run_sql", '{"statement": "SELECT 1; DROP TABLE t"}', "one statement"),
        ("run_sql", '{"statement": "INSERT INTO t SELECT 1"}', "Only SELECT"),
        ("load_skill", '{"name": "missing"}', "Unknown skill"),
    ],
)
def test_errors_are_returned_not_raised(name, arguments, message):
    assert message in tools.execute(CTX, name, arguments)["error"]


def test_bins():
    assert tools.regular_bins(0, 1000, 250) == [0, 250, 500, 750, 1000]
    assert tools.nice_bins(0, 3385) == [float(x) for x in range(0, 3600, 200)]
    assert tools.nice_bins(-11, 6)[0] <= -11 and tools.nice_bins(-11, 6)[-1] >= 6
    with pytest.raises(tools.ToolError):
        tools.regular_bins(0, 1e6, 1)


def test_prompt_lists_skills_and_report():
    text = prompt.system({"report": "r1", "signals": []})
    assert "impulse-app-signals" in text and "impulse-tsal" in text and '"report":"r1"' in text
    assert "No report is open" in prompt.system(None)


def test_prompt_states_the_time_unit():
    assert "microseconds, so expand(10e6)" in prompt.system(None)
    assert "nanoseconds, so expand(10e9)" in prompt.system(None, "ns")


def test_clip_marks_truncation():
    assert clip("abc", 3) == "abc"
    assert clip("abcdef", 3) == "abc [truncated]"


def test_uut_tools_scale_to_large_fleets(monkeypatch):
    rows = [{"id": f"v{i:03}", "containers": 1, "first": "2026-01-01 00:00:00", "last": "2026-01-02 00:00:00"} for i in range(120)]
    saved = {"spec": ReportSpec(name="r")}

    def change(ctx, mutate):
        data = saved["spec"].dump()
        mutate(data)
        saved["spec"] = ReportSpec.model_validate(data)
        return saved["spec"]

    monkeypatch.setattr(tools, "_spec", lambda ctx: saved["spec"])
    monkeypatch.setattr(tools, "_source", lambda ctx, spec: (None, SimpleNamespace(issues=[])))
    monkeypatch.setattr(tools.datamodel, "uuts", lambda user, model, found: rows)
    monkeypatch.setattr(tools, "_change", change)

    listed = tools.list_uuts(CTX, tools.UutQuery())
    assert (listed["total"], len(listed["uuts"]), listed["more"]) == (120, tools.MAX_ROWS, 120 - tools.MAX_ROWS)
    assert [v["id"] for v in tools.list_uuts(CTX, tools.UutQuery(query="V11"))["uuts"]] == [f"v11{i}" for i in range(10)]

    selected = tools.set_uuts(CTX, tools.UutArgs(all=True))
    assert selected["selected"] == 120 and len(selected["uuts"]) == tools.MAX_ROWS
    assert saved["spec"].uuts == [r["id"] for r in rows]
    assert tools.summary(saved["spec"])["uut_count"] == 120
    with pytest.raises(tools.ToolError, match="Unknown units under test: x"):
        tools.set_uuts(CTX, tools.UutArgs(uuts=["v001", "x"]))
