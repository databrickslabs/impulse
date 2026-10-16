import inspect
from types import SimpleNamespace

import pytest

pytest.importorskip("impulse_reporting", reason="run with make contract")

import databricks.sdk
import impulse_reporting.core.report as report_module
from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_reporting.aggregations.histogram import HistogramDistance, HistogramDuration
from impulse_reporting.aggregations.histogram2d import Histogram2DDuration
from impulse_reporting.aggregations.point_value_aggregator import PointValueAggregator
from impulse_reporting.aggregations.stats_aggregator import StatsAggregator
from impulse_reporting.config.config_parser import ImpulseConfig
from impulse_reporting.events.basic_event import BasicEvent
from impulse_reporting.events.points_in_time_event import PointsInTimeEvent

from server import codegen, tsal
from server.datamodel import DataModel
from tests.fixtures import demo
from tests.unit.test_tsal import SIGNALS, TYPES

CLASSES = {
    "histogram": (HistogramDuration, HistogramDistance),
    "histogram_2d": (Histogram2DDuration,),
    "statistics": (StatsAggregator,),
    "point_values": (PointValueAggregator,),
}
EVALUATES = {"SampleSeries": tsal.SIGNAL, "Intervals": tsal.INTERVALS, "PointsInTime": tsal.POINTS, "PointsInTimeSeries": tsal.POINT_VALUES}
RUN = inspect.signature(report_module.Report.run)


def query_builder() -> QueryBuilder:
    return QueryBuilder(SimpleNamespace(ws=None, config=SimpleNamespace(channel_mapping_table=None)))


class FakeReport:
    created: list["FakeReport"] = []
    add_event = report_module.Report.add_event
    add_page = report_module.Report.add_page
    _validate_aggregation_events = report_module.Report._validate_aggregation_events

    def __init__(self, name, spark, workspace_client, config):
        self.name = name
        self.config = ImpulseConfig.model_validate(config)
        self.report_id = 1
        self.events = []
        self.pages = []
        self.event_dfs = {}
        self.aggregation_dfs = {}
        self.determined = False
        self.persisted = False
        self.query = query_builder()
        FakeReport.created.append(self)

    def get_db(self):
        return SimpleNamespace(query=self.query)

    def run(self, *args, **kwargs):
        # Accept exactly the arguments the real Report.run accepts.
        call = RUN.bind(self, *args, **kwargs)
        call.apply_defaults()
        self._validate_aggregation_events()
        self.determined = True
        self.persisted = call.arguments["persist_results"]


class FakeDbutils:
    def __init__(self):
        self.exit_value = None
        self.notebook = SimpleNamespace(exit=self._exit)

    def _exit(self, value):
        self.exit_value = value


class FakeSpark:
    def __init__(self):
        self.statements: list[str] = []

    def sql(self, statement):
        self.statements.append(statement)


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    monkeypatch.setattr(report_module, "Report", FakeReport)
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", lambda: None)
    FakeReport.created.clear()


def run(notebook: str) -> tuple[FakeReport, FakeSpark, FakeDbutils]:
    spark, dbutils = FakeSpark(), FakeDbutils()
    exec(compile(notebook, "report", "exec"), {"spark": spark, "dbutils": dbutils, "__name__": "__main__"})
    return FakeReport.created[-1], spark, dbutils


@pytest.mark.parametrize("uut_source", ["column", "tag"])
@pytest.mark.parametrize("window", [(None, None), ("2018-01-01T00:00:00", "2019-01-01T00:00:00")])
@pytest.mark.parametrize("sinkless", [False, True])
def test_generated_report_builds_on_framework(uut_source, window, sinkless):
    spec = demo.spec()
    spec.window.start, spec.window.stop = window
    generated = codegen.generate(spec, demo.source(uut_source), DataModel(), sinkless=sinkless)
    report, spark, dbutils = run(generated.notebook)

    assert report.name == spec.name
    assert spark.statements == []
    assert report.determined
    assert report.persisted is not sinkless
    assert (report.config.unity_sink is None) is sinkless
    assert [type(e) for e in report.events] == [
        BasicEvent if e.kind == "condition" else PointsInTimeEvent for e in spec.events
    ]
    aggregations = [a for page in report.pages for a in page.aggregations]
    assert [a.name for a in aggregations] == [a.name for a in spec.aggregations]
    for built, defined in zip(aggregations, spec.aggregations):
        assert isinstance(built, CLASSES[defined.kind])
    if sinkless:
        assert dbutils.exit_value == "{}"


@pytest.mark.parametrize(("expr", "expected"), [t for t in TYPES if t[1] != tsal.NUMBER])
def test_type_inference_matches_framework(expr, expected):
    q = query_builder()
    names = {s: q.channel(channel_name=s) for s in SIGNALS}
    built = eval(tsal.check(expr, SIGNALS).code, {"__builtins__": {}}, names)
    assert EVALUATES[built.evaluation_type().__name__] == expected
