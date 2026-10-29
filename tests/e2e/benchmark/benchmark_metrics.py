"""Runtime and memory metrics of a benchmark scenario on the job cluster.

Spark metrics come from the Spark UI REST API of the driver: the stages a scenario ran (those
with a higher id than any stage before it), summed, and their executor memory peaks. Executor
process-tree metrics (the RSS of the pandas UDF workers) need
`spark.executor.processTreeMetrics.enabled`. The driver's Python memory, where Impulse plans and
collects, is sampled with psutil. DBUs come from `system.billing.usage`, which lags by hours;
`setup` fills them in for earlier runs.
"""

import threading
import time

import psutil
import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Stage metrics summed over a scenario's stages, by result column, with their scale.
STAGE_SUMS = {
    "executor_run_time_s": ("executorRunTime", 1e-3),
    "executor_cpu_time_s": ("executorCpuTime", 1e-9),
    "jvm_gc_time_s": ("jvmGcTime", 1e-3),
    "input_bytes": ("inputBytes", 1),
    "output_bytes": ("outputBytes", 1),
    "shuffle_read_bytes": ("shuffleReadBytes", 1),
    "shuffle_write_bytes": ("shuffleWriteBytes", 1),
    "memory_spilled_bytes": ("memoryBytesSpilled", 1),
    "disk_spilled_bytes": ("diskBytesSpilled", 1),
    "num_tasks": ("numCompleteTasks", 1),
    "failed_tasks": ("numFailedTasks", 1),
}

# Executor memory peaks (max over the scenario's stages and executors), by result column.
EXECUTOR_PEAKS = {
    "peak_executor_python_rss_bytes": "ProcessTreePythonRSSMemory",
    "peak_executor_jvm_rss_bytes": "ProcessTreeJVMRSSMemory",
    "peak_executor_jvm_heap_bytes": "JVMHeapMemory",
    "peak_executor_onheap_execution_bytes": "OnHeapExecutionMemory",
}

METRICS = [
    "wall_s",
    "num_stages",
    *STAGE_SUMS,
    *EXECUTOR_PEAKS,
    "peak_driver_jvm_heap_bytes",
    "peak_driver_python_rss_bytes",
]


def _api(path):
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    response = requests.get(base + path, timeout=60)
    response.raise_for_status()
    return response.json()


def _stages():
    """All stage attempts of the application."""
    return _api("/stages?details=false&withSummaries=false")


def _executor_peaks(executor_id=None, exclude_driver=False):
    """Lifetime memory peaks of the executors, or of one of them (e.g. "driver")."""
    executors = [
        e
        for e in _api("/allexecutors")
        if (executor_id is None or e["id"] == executor_id)
        and not (exclude_driver and e["id"] == "driver")
    ]
    return [e.get("peakMemoryMetrics") or {} for e in executors]


class _RssSampler(threading.Thread):
    """Peak RSS of the notebook's Python process and its children, sampled every second."""

    def __init__(self):
        super().__init__(daemon=True)
        self.peak = 0
        self._done = threading.Event()

    def run(self):
        process = psutil.Process()
        while not self._done.is_set():
            try:
                rss = process.memory_info().rss + sum(
                    c.memory_info().rss for c in process.children(recursive=True)
                )
                self.peak = max(self.peak, rss)
            except psutil.Error:
                pass
            self._done.wait(1.0)

    def stop(self):
        self._done.set()
        self.join()
        return self.peak


class Measurement:
    """Measures the Spark work inside a `with` block. Nothing else may run on the cluster."""

    def __enter__(self):
        self._last_stage = max((s["stageId"] for s in _stages()), default=-1)
        self._sampler = _RssSampler()
        self._sampler.start()
        self._start = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.wall_s = time.perf_counter() - self._start
        self.driver_python_rss = self._sampler.stop()
        return False

    def metrics(self):
        """Metrics of the block, by column of `METRICS`."""
        # Let the listener process the last stages and executor heartbeats.
        time.sleep(15)
        stages = [s for s in _stages() if s["stageId"] > self._last_stage]
        result = {"wall_s": self.wall_s, "num_stages": len(stages)}
        for column, (field, scale) in STAGE_SUMS.items():
            result[column] = sum(s.get(field) or 0 for s in stages) * scale
        stage_peaks = [s.get("peakExecutorMetrics") or {} for s in stages]
        if not any(stage_peaks):
            # The stage list doesn't carry them: the executors' lifetime peaks instead.
            stage_peaks = _executor_peaks(exclude_driver=True)
        for column, field in EXECUTOR_PEAKS.items():
            result[column] = max((p.get(field) or 0 for p in stage_peaks), default=0)
        # The driver's JVM peak is only known for the application's lifetime.
        driver = _executor_peaks("driver")
        result["peak_driver_jvm_heap_bytes"] = driver[0].get("JVMHeapMemory", 0) if driver else 0
        result["peak_driver_python_rss_bytes"] = self.driver_python_rss
        return {k: float(v) for k, v in result.items()}


def cluster_spec():
    """Runtime, node types and size of the cluster; runs compare only within one spec."""
    tag = lambda name: spark.conf.get(f"spark.databricks.clusterUsageTags.{name}", "")
    return (
        f"{tag('sparkVersion')} driver={tag('driverNodeType')} "
        f"workers={tag('clusterTargetWorkers')}x{tag('clusterNodeType')}"
    )
