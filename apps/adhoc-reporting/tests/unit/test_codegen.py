import ast

import pytest

from server import codegen
from server.datamodel import DataModel
from server.errors import ApiError
from tests.fixtures import demo


def test_notebook_compiles_and_uses_report_name():
    generated = codegen.generate(demo.spec(), demo.source(), DataModel())
    compile(generated.notebook, "report", "exec")
    assert generated.notebook.startswith("# Databricks notebook source\n")
    assert "report = Report(name='demo_full'," in generated.notebook
    assert "eng_rpm = db.query.channel(channel_name='Engine RPM')" in generated.notebook
    assert "avg_temp = (amb_temp + intake_temp) / 2" in generated.notebook
    assert "ContainerEvent" not in generated.notebook
    assert "name='recording_stats',\n        input_expressions=[eng_rpm, veh_spd, avg_temp],\n        channel_names=['Engine RPM', 'Vehicle Speed Sensor', 'avg_temp'],\n        statistics=['min', 'max', 'mean', 'median', 'start', 'end'],\n    )" in generated.notebook
    assert generated.notebook.endswith("report.run()\n")
    assert "dbutils.widgets" not in generated.notebook
    assert "DROP TABLE" not in generated.notebook


def test_config_for_uut_column_and_window():
    spec = demo.spec()
    spec.window.start = "2026-01-01T00:00:00"
    config = codegen.config(spec, demo.source("column"), DataModel())
    assert config["source"] == {
        "container_metrics_table": "main.impulse_demo.container_metrics",
        "channel_metrics_table": "main.impulse_demo.channel_metrics",
        "channels_uri": "main.impulse_demo.channels",
        "container_tags_table": "main.impulse_demo.container_tags",
        "channel_tags_table": "main.impulse_demo.channel_tags",
    }
    assert config["unity_sink"]["table_prefix"] == "demo_full"
    assert config["query_engine"] == {"solver": "DefaultSolver", "data_type": "RAW"}
    assert config["container_filters"] == {
        "metric_filters": [
            [
                {"column_name": "vehicle_key", "comparator": "==", "value": "Seat_Leon"},
                {"column_name": "start_dt", "comparator": ">=", "value": "2026-01-01T00:00:00", "value_type": "timestamp"},
            ]
        ]
    }
    assert "measurement_dimensions" not in config


def test_config_for_uut_tag_and_window():
    spec = demo.spec()
    spec.uuts = ["A", "B"]
    spec.window.stop = "2026-02-01T00:00:00"
    config = codegen.config(spec, demo.source("tag"), DataModel(uut={"key": "vin"}))
    assert config["container_filters"] == {
        "tag_filters": [
            [{"tag_name": "vin", "comparator": "==", "value": "A", "cast_type": "string"}],
            [{"tag_name": "vin", "comparator": "==", "value": "B", "cast_type": "string"}],
        ],
        "metric_filters": [[{"column_name": "stop_dt", "comparator": "<=", "value": "2026-02-01T00:00:00", "value_type": "timestamp"}]],
    }


def test_solver_config_and_dimensions_follow_data_model():
    model = DataModel(
        solver_config={"container_metrics": {"column_name_mapping": {"rec": "container_id"}}},
        measurement_dimensions=["container_id"],
    )
    config = codegen.config(demo.spec(), demo.source(), model)
    assert config["query_engine"]["solver_config"] == model.solver_config
    assert config["measurement_dimensions"] == ["container_id"]


def test_sinkless_notebook_reports_counts():
    generated = codegen.generate(demo.spec(), demo.source(), DataModel(), sinkless=True)
    compile(generated.notebook, "report", "exec")
    assert "unity_sink" not in generated.config
    assert "dbutils.widgets" not in generated.notebook
    assert "report.run(persist_results=False)" in generated.notebook
    assert "dbutils.notebook.exit(json.dumps(counts))" in generated.notebook


def test_hostile_text_stays_in_literals():
    spec = demo.spec()
    spec.description = "x'); import os; ('"
    spec.signals[0].channel = {"channel_name": "Engine RPM'); import os; ('"}
    spec.signals[0].desc = '"""'
    spec.events[0].desc = "\\'\n'"
    spec.aggregations[0].values_unit = "s'); os.system('x'); ('"
    generated = codegen.generate(spec, demo.source(), DataModel())
    tree = ast.parse(generated.notebook)
    imports = {alias.name for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom)) for alias in node.names}
    assert "os" not in imports
    calls = {node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)}
    assert "system" not in calls


def test_channel_with_non_identifier_tag():
    spec = demo.spec()
    model = DataModel(channel_identity=["channel-name"])
    spec.signals[0].channel = {"channel-name": "Engine RPM"}
    for s in spec.signals[1:4]:
        s.channel = {"channel-name": s.channel["channel_name"]}
    generated = codegen.generate(spec, demo.source(), model)
    assert "eng_rpm = db.query.channel(**{'channel-name': 'Engine RPM'})" in generated.notebook


def test_invalid_spec_is_refused():
    spec = demo.spec()
    spec.aggregations = []
    with pytest.raises(ApiError) as error:
        codegen.generate(spec, demo.source(), DataModel())
    assert error.value.status == 422
