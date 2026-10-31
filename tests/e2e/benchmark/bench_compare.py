# Databricks notebook source
# MAGIC %md
# MAGIC # Benchmark: compare
# MAGIC Compares each scenario with the median of its last 5 runs on `baseline_ref` that passed the
# MAGIC gate (same benchmark version and compute, no config overrides). Returns markdown, stores the
# MAGIC verdict in `gate_passed` and sets the task value `regressions` for `gate`. Warns when gold
# MAGIC tables differ from the baseline's, or `incremental_delta`'s from `full`'s.

# COMMAND ----------

# The bundle's wheel, installed per notebook: as a cluster library it breaks the metastore init.
import glob

whl = max(glob.glob(f"{dbutils.widgets.get('wheel_dir')}/databricks_impulse-*.whl"))

# COMMAND ----------

# MAGIC %pip install --quiet $whl

# COMMAND ----------

import json
import statistics

import pyspark.sql.functions as F
from bench_utils import params, spark
from benchmark_metrics import GATED, METRICS, compute_kind
from benchmark_report import COMPUTE_SCENARIOS

p = params()
BASELINE_RUNS = 5
# Gated metrics of this compute, checked in the longer scenarios.
GATED = GATED[compute_kind()]
GATED_SCENARIOS = {"full", "wide_report", "python_udfs"} & set(COMPUTE_SCENARIOS[compute_kind()])
# Fingerprint sums may differ in the last digits, as Spark sums in any order.
FINGERPRINT_RTOL = 1e-6

results = spark.read.table(p.results)
current = {r.scenario: r for r in results.where(F.col("job_run_id") == p.job_run_id).collect()}
if not current:
    raise RuntimeError(f"no results of run {p.job_run_id}")
first = next(iter(current.values()))
baseline_rows = (
    results.where(
        (F.col("status") == "success")
        & F.col("gate_passed")
        & (F.col("git_ref") == p.baseline_ref)
        & (F.col("benchmark_version") == first.benchmark_version)
        & (F.col("compute") == first.compute)
        & (F.col("config_overrides") == "{}")
        & (F.col("job_run_id") != p.job_run_id)
    )
    .orderBy(F.col("ts").desc())
    .collect()
)

# COMMAND ----------


def fmt(metric, value):
    if value is None:
        return "–"
    if metric.endswith("_bytes"):
        return f"{value / 2**30:.2f} GiB"
    if metric.endswith("_s"):
        return f"{value:.1f} s"
    return f"{value:,.0f}"


def fingerprint_diff(actual, expected):
    # Hashes differ per run.
    keys = sorted(k for k in set(actual or {}) | set(expected or {}) if not k.endswith("_hash"))
    return [
        k
        for k in keys
        if k not in actual
        or k not in expected
        or abs(actual[k] - expected[k]) > FINGERPRINT_RTOL * max(abs(expected[k]), 1.0)
    ]


lines = [
    f"## Impulse benchmark (`{first.git_ref}` @ `{first.git_sha[:8]}`)",
    "",
    f"Impulse {first.impulse_version}, benchmark v{first.benchmark_version}, {first.compute}, "
    f"overrides `{first.config_overrides}`, baseline: last {BASELINE_RUNS} runs on "
    f"`{p.baseline_ref}`.",
]
regressions, warnings = [], []
for scenario in COMPUTE_SCENARIOS[compute_kind()]:
    run = current.get(scenario)
    if run is None:  # Its task failed before recording anything.
        lines += ["", f"### `{scenario}` (not run)"]
        regressions.append(f"{scenario} not run")
        continue
    base = [r for r in baseline_rows if r.scenario == scenario][:BASELINE_RUNS]
    lines += ["", f"### `{scenario}` ({run.status})", ""]
    if run.status != "success":
        lines.append(f"Failed: `{run.error}`")
        regressions.append(f"{scenario} failed")
        continue
    if not base:
        lines += ["No baseline yet: this run starts it.", ""]
    lines += [f"| Metric | This run | Baseline (n={len(base)}) | Δ |", "|---|---|---|---|"]
    for metric in METRICS:
        value = run[metric]
        if value is None:  # A metric of the other compute.
            continue
        values = [r[metric] for r in base if r[metric] is not None]
        median = statistics.median(values) if values else None
        # No relative change against a baseline of (nearly) nothing.
        significant = median and abs(median) >= 1e-3 * max(abs(value), 1.0)
        delta = (value - median) / median if significant else None
        flag = ""
        gated = scenario in GATED_SCENARIOS and metric in GATED
        if gated and delta is not None and delta > p.max_regression:
            regressions.append(f"{scenario}.{metric} {delta:+.0%}")
            flag = " ❌"
        delta_text = f"{delta:+.1%}{flag}" if delta is not None else "–"
        lines.append(
            f"| `{metric}` | {fmt(metric, value)} | {fmt(metric, median)} | {delta_text} |"
        )
    if run.fingerprint is not None and base and base[0].fingerprint is not None:
        diff = fingerprint_diff(run.fingerprint, base[0].fingerprint)
        if diff:
            warnings.append(f"{scenario}: gold tables differ from the baseline: {diff[:10]}")

# Re-ingested trips give the same gold tables again.
full, delta = current.get("full"), current.get("incremental_delta")
if full and delta and full.fingerprint is not None and delta.fingerprint is not None:
    diff = fingerprint_diff(delta.fingerprint, full.fingerprint)
    if diff:
        warnings.append(f"incremental_delta: gold tables differ from full's: {diff[:10]}")

lines.append("")
lines += [f"⚠️ {w}" for w in warnings]
limit = f"{p.max_regression:.0%}"
if regressions:
    lines.append(f"❌ Failed or above {limit}: {', '.join(regressions)}")
else:
    lines.append(
        f"✅ No regression above {limit} in {', '.join(GATED)} of "
        f"{', '.join(sorted(GATED_SCENARIOS))}."
    )
markdown = "\n".join(lines)
print(markdown)

spark.sql(
    f"UPDATE {p.results} SET gate_passed = {not regressions} WHERE job_run_id = '{p.job_run_id}'"
)
dbutils.jobs.taskValues.set(key="regressions", value=json.dumps(regressions))
dbutils.notebook.exit(markdown)
