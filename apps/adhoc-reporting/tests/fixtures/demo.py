import json
from pathlib import Path

from server.datamodel import Source
from server.spec import ReportSpec

SPECS = Path(__file__).parent / "specs"
COLUMNS = {
    "container_metrics": ["container_id", "vehicle_key", "start_ts", "stop_ts", "start_dt", "stop_dt", "duration_ms", "num_channels"],
    "channel_metrics": [
        "container_id",
        "channel_id",
        "sample_count",
        "min",
        "max",
        "mean",
        "begin_ms",
        "end_ms",
        "duration_ms",
        "sample_rate",
        "value_type",
    ],
    "channels": ["container_id", "channel_id", "timestamp", "value"],
    "container_tags": ["container_id", "key", "value"],
    "channel_tags": ["container_id", "channel_id", "key", "value"],
}


def source(uut_source: str = "column", catalog: str = "main", schema: str = "impulse_demo") -> Source:
    return Source(
        catalog=catalog,
        schema=schema,
        tables={t: f"{catalog}.{schema}.{t}" for t in COLUMNS},
        columns=COLUMNS,
        data_type="RAW",
        uut_source=uut_source,
        uut_key="vehicle_key",
        issues=[],
    )


def spec(name: str = "demo_full", catalog: str | None = None, schema: str | None = None) -> ReportSpec:
    data = json.loads((SPECS / f"{name}.json").read_text())
    if catalog and schema:
        data["source"] = {"catalog": catalog, "schema": schema}
        data["sink"] = {**data["sink"], "catalog": catalog, "schema": schema}
    return ReportSpec.model_validate(data)
