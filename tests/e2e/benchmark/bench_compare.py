# Databricks notebook source
# MAGIC %md
# MAGIC # Benchmark: compare
# MAGIC Compares this run with its baseline: the median of the last 5 successful runs on
# MAGIC `baseline_ref` with the same benchmark version and compute, and no config overrides. Returns
# MAGIC the comparison as markdown and sets the task value `regressions`, which `gate` checks:
# MAGIC a failed scenario, or
# MAGIC a gated metric of the `full` scenario more than `max_regression` above the baseline.
# MAGIC Gold tables that differ from the baseline's are reported as a warning.

# COMMAND ----------

# Impulse is installed per notebook: as a cluster library, the bundle's wheel keeps the
# notebook from starting ("Hive metastore initialization failed").
import glob

whl = max(glob.glob(f"{dbutils.widgets.get('wheel_dir')}/databricks_impulse-*.whl"))

# COMMAND ----------

# MAGIC %pip install --quiet $whl

# COMMAND ----------

import json
import statistics

import pyspark.sql.functions as F
from bench_utils import params, spark
from benchmark_metrics import METRICS

p = params()
BASELINE_RUNS = 5
GATED = ["wall_s", "executor_run_time_s", "peak_executor_python_rss_bytes"]
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
    # Hashes differ per run (older rows still have them).
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
for scenario, run in current.items():
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
        values = [r[metric] for r in base if r[metric] is not None]
        median = statistics.median(values) if values else None
        # No relative change against a baseline of (nearly) nothing.
        significant = median and abs(median) >= 1e-3 * max(abs(value), 1.0)
        delta = (value - median) / median if significant else None
        flag = ""
        gated = scenario == "full" and metric in GATED
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

lines.append("")
lines += [f"⚠️ {w}" for w in warnings]
limit = f"{p.max_regression:.0%}"
if regressions:
    lines.append(f"❌ Failed or above {limit}: {', '.join(regressions)}")
else:
    lines.append(f"✅ No regression above {limit} in {', '.join(GATED)}.")
markdown = "\n".join(lines)
print(markdown)

dbutils.jobs.taskValues.set(key="regressions", value=json.dumps(regressions))
dbutils.notebook.exit(markdown)
