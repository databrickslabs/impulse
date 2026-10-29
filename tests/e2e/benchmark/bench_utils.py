"""Shared helpers for the benchmark notebooks."""

import json
import os
import sys
from importlib.metadata import version

import pyspark.sql.functions as F
from databricks.sdk.runtime import dbutils
from pyspark.sql import types as T

# The e2e helpers are one folder up.
sys.path.append(os.path.abspath(".."))
from benchmark_metrics import METRICS, cluster_spec  # noqa: E402
from benchmark_report import BENCHMARK_VERSION, MEASUREMENT_DIMENSIONS  # noqa: E402
from e2e_utils import config, spark  # noqa: E402

PARAMS = [
    "catalog",
    "schema",
    "source_catalog",
    "source_schema",
    "results_schema",
    "prefix",
    "expected_dir",
    "actual_dir",
    "git_sha",
    "git_ref",
    "trigger",
    "config_overrides",
    "baseline_ref",
    "max_regression",
    "job_id",
    "job_run_id",
]

RESULTS_SCHEMA = T.StructType(
    [
        T.StructField("job_run_id", T.StringType()),
        T.StructField("ts", T.TimestampType()),
        T.StructField("git_sha", T.StringType()),
        T.StructField("git_ref", T.StringType()),
        T.StructField("trigger", T.StringType()),
        T.StructField("impulse_version", T.StringType()),
        T.StructField("benchmark_version", T.IntegerType()),
        T.StructField("compute", T.StringType()),
        T.StructField("config_overrides", T.StringType()),
        T.StructField("scenario", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("error", T.StringType()),
        *[T.StructField(m, T.DoubleType()) for m in METRICS],
        T.StructField("dbus", T.DoubleType()),
        T.StructField("fingerprint", T.MapType(T.StringType(), T.DoubleType())),
    ]
)


class Params:
    def __init__(self):
        for name in PARAMS:
            setattr(self, name, dbutils.widgets.get(name))
        self.source = f"{self.source_catalog}.{self.source_schema}."
        self.results = f"{self.catalog}.{self.results_schema}.results"
        self.overrides = json.loads(self.config_overrides or "{}")
        self.max_regression = float(self.max_regression)


def params():
    return Params()


def report_config(p):
    """The benchmark's report config, with the run's `config_overrides` merged per section."""
    cfg = config(
        p,
        incremental={"enabled": True, "silver_last_modified_column": "_ingested_at"},
        calculated_channels={"emit_channel_metrics": True},
    )
    cfg["measurement_dimensions"] = MEASUREMENT_DIMENSIONS
    for key, value in p.overrides.items():
        cfg[key] = {**cfg.get(key, {}), **value} if isinstance(value, dict) else value
    return cfg


def run_info(p):
    """The columns that identify a run and what it measured."""
    return {
        "job_run_id": p.job_run_id,
        "git_sha": p.git_sha,
        "git_ref": p.git_ref,
        "trigger": p.trigger,
        # The wheel's version has a build-time suffix (dynamic_version in databricks.yml).
        "impulse_version": version("databricks-impulse").split("+")[0],
        "benchmark_version": BENCHMARK_VERSION,
        "compute": cluster_spec(),
        "config_overrides": json.dumps(p.overrides, sort_keys=True),
    }


def fingerprint(p):
    """Row count and rounded sums of the numeric columns of each gold table.

    Ids, hashes and bookkeeping columns are skipped; they differ between runs (a config hash
    covers the run's schema name). Sums are in double, as
    sums of epoch-µs timestamps overflow a long.
    """
    result = {}
    tables = spark.sql(f"SHOW TABLES IN {p.catalog}.{p.schema}").collect()
    for name in sorted(t.tableName for t in tables if t.tableName.startswith(f"{p.prefix}_")):
        df = spark.read.table(f"{p.catalog}.{p.schema}.{name}")
        numeric = [
            f.name
            for f in df.schema.fields
            if isinstance(f.dataType, T.NumericType)
            and not f.name.startswith("_")
            and not f.name.endswith(("_id", "_hash"))
        ]
        aggs = [F.count(F.lit(1)).alias("count")] + [
            F.sum(F.when(~F.isnan(c), F.col(c).cast("double"))).alias(c) for c in numeric
        ]
        row = df.agg(*aggs).first().asDict()
        table = name.removeprefix(f"{p.prefix}_")
        result.update({f"{table}.{k}": float(v) for k, v in row.items() if v is not None})
    return result
