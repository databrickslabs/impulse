"""Runtime and memory metrics of a benchmark scenario, on the job cluster or on serverless.

On the job cluster, Spark metrics come from the Spark UI REST API of the driver: the stages a
scenario ran (those with a higher id than any stage before it), summed, and their executor
memory peaks. Executor process-tree metrics (the RSS of the pandas UDF workers) need
`spark.executor.processTreeMetrics.enabled`.

Serverless has no Spark UI, so its metrics come from the query history: the queries the task run
ran in the scenario's time window (task time, bytes read and written, spill). Executor memory
isn't available there.

On both, the driver's Python memory, where Impulse plans and collects, is sampled with psutil.
"""

import threading
import time

import psutil
import requests
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
ws = WorkspaceClient()

# Job cluster: stage metrics summed over a scenario's stages, by result column, with their scale.
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

# Job cluster: executor memory peaks (max over the scenario's stages and executors).
EXECUTOR_PEAKS = {
    "peak_executor_python_rss_bytes": "ProcessTreePythonRSSMemory",
    "peak_executor_jvm_rss_bytes": "ProcessTreeJVMRSSMemory",
    "peak_executor_jvm_heap_bytes": "JVMHeapMemory",
    "peak_executor_onheap_execution_bytes": "OnHeapExecutionMemory",
}

# Serverless: query metrics summed over a scenario's queries, by result column, with their scale.
QUERY_SUMS = {
    "task_time_s": ("task_total_time_ms", 1e-3),
    "photon_task_time_s": ("photon_total_time_ms", 1e-3),
    "read_bytes": ("read_bytes", 1),
    "read_rows": ("rows_read_count", 1),
    "written_bytes": ("write_remote_bytes", 1),
    "written_rows": ("write_remote_rows", 1),
    "spilled_bytes": ("spill_to_disk_bytes", 1),
}

CLUSTER_METRICS = [
    "wall_s",
    "num_stages",
    *STAGE_SUMS,
    *EXECUTOR_PEAKS,
    "peak_driver_jvm_heap_bytes",
    "peak_driver_python_rss_bytes",
]
SERVERLESS_METRICS = [
    "wall_s",
    "num_queries",
    "max_query_s",
    *QUERY_SUMS,
    "num_tasks",
    "failed_tasks",
    "peak_driver_python_rss_bytes",
]
# Columns of the results table; a run fills those of its compute, the others stay empty.
METRICS = list(dict.fromkeys(CLUSTER_METRICS + SERVERLESS_METRICS))

# Metrics of the `full` scenario that `gate` checks, by compute.
GATED = {
    "cluster": ["wall_s", "executor_run_time_s", "peak_executor_python_rss_bytes"],
    "serverless": ["wall_s", "task_time_s", "peak_driver_python_rss_bytes"],
}


def is_serverless():
    """Whether the notebook runs on serverless: Spark Connect, without a SparkContext."""
    try:
        return spark.sparkContext is None
    except Exception:
        return True


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


class _Measurement:
    """Wall time and driver memory of a `with` block. Nothing else may run on the compute."""

    def __enter__(self):
        self._sampler = _RssSampler()
        self._sampler.start()
        self._start_ms = int(time.time() * 1000)
        self._start = time.perf_counter()
        return self

    def __exit__(self, *exc):
        self.wall_s = time.perf_counter() - self._start
        self._end_ms = int(time.time() * 1000)
        self.driver_python_rss = self._sampler.stop()
        return False

    def metrics(self):
        """Metrics of the block, by column of `METRICS`."""
        result = {"wall_s": self.wall_s, **self._spark_metrics()}
        result["peak_driver_python_rss_bytes"] = self.driver_python_rss
        return {k: float(v) for k, v in result.items()}


class _ClusterMeasurement(_Measurement):
    """Spark metrics from the driver's Spark UI REST API."""

    def __init__(self):
        self._sc = spark.sparkContext

    def _api(self, path):
        base = f"{self._sc.uiWebUrl}/api/v1/applications/{self._sc.applicationId}"
        response = requests.get(base + path, timeout=60)
        response.raise_for_status()
        return response.json()

    def _stages(self):
        return self._api("/stages?details=false&withSummaries=false")

    def _executor_peaks(self, driver):
        """Lifetime memory peaks of the driver, or of the executors."""
        return [
            e.get("peakMemoryMetrics") or {}
            for e in self._api("/allexecutors")
            if (e["id"] == "driver") == driver
        ]

    def __enter__(self):
        self._last_stage = max((s["stageId"] for s in self._stages()), default=-1)
        return super().__enter__()

    def _spark_metrics(self):
        # Let the listener process the last stages and executor heartbeats.
        time.sleep(15)
        stages = [s for s in self._stages() if s["stageId"] > self._last_stage]
        result = {"num_stages": len(stages)}
        for column, (field, scale) in STAGE_SUMS.items():
            result[column] = sum(s.get(field) or 0 for s in stages) * scale
        peaks = [s.get("peakExecutorMetrics") or {} for s in stages]
        if not any(peaks):
            # The stage list doesn't carry them: the executors' lifetime peaks instead.
            peaks = self._executor_peaks(driver=False)
        for column, field in EXECUTOR_PEAKS.items():
            result[column] = max((p.get(field) or 0 for p in peaks), default=0)
        # The driver's JVM peak is only known for the application's lifetime.
        driver = self._executor_peaks(driver=True)
        result["peak_driver_jvm_heap_bytes"] = driver[0].get("JVMHeapMemory", 0) if driver else 0
        return result


class _ServerlessMeasurement(_Measurement):
    """Spark metrics from the query history of this task run."""

    def __init__(self, task_run_id):
        self.task_run_id = str(task_run_id)

    def _queries(self):
        """Queries of this task run that started in the block, as returned by the API.

        The raw API, as the SDK's `QueryMetrics` drops `task_progress`.
        """
        query = {
            "include_metrics": "true",
            "max_results": 1000,
            "filter_by.query_start_time_range.start_time_ms": self._start_ms,
            "filter_by.query_start_time_range.end_time_ms": self._end_ms,
        }
        queries = []
        while True:
            page = ws.api_client.do("GET", "/api/2.0/sql/history/queries", query=query)
            queries += [
                q
                for q in page.get("res", [])
                if str(q.get("query_source", {}).get("job_info", {}).get("job_task_run_id"))
                == self.task_run_id
            ]
            if not page.get("has_next_page"):
                return queries
            # Further pages take only the token.
            query = {"include_metrics": "true", "page_token": page["next_page_token"]}

    def _final_queries(self, timeout_s=300):
        """The block's queries, once the history has all of them with their final metrics."""
        deadline = time.monotonic() + timeout_s
        previous = None
        while True:
            queries = self._queries()
            final = all(q.get("is_final") and q.get("metrics") for q in queries)
            # Two equal polls in a row: the history has caught up.
            if final and queries and previous == len(queries):
                return queries
            if time.monotonic() > deadline:
                print(f"query history incomplete after {timeout_s} s: {len(queries)} queries")
                return queries
            previous = len(queries) if final else None
            time.sleep(15)

    def _spark_metrics(self):
        queries = self._final_queries()
        metrics = [q.get("metrics") or {} for q in queries]
        progress = [m.get("task_progress") or {} for m in metrics]
        result = {
            "num_queries": len(queries),
            "max_query_s": max((q.get("duration", 0) for q in queries), default=0) / 1000,
            "num_tasks": sum(p.get("completed_task_count", 0) for p in progress),
            "failed_tasks": sum(p.get("failed_task_count", 0) for p in progress),
        }
        for column, (field, scale) in QUERY_SUMS.items():
            result[column] = sum(m.get(field) or 0 for m in metrics) * scale
        return result


def Measurement(task_run_id):
    """Measures the Spark work of a `with` block on this notebook's compute."""
    return _ServerlessMeasurement(task_run_id) if is_serverless() else _ClusterMeasurement()


def compute_kind():
    """ "serverless" or "cluster", the key of `GATED`."""
    return "serverless" if is_serverless() else "cluster"


def cluster_spec():
    """Runtime, node types and size of the job cluster; runs compare only within one spec."""
    tag = lambda name: spark.conf.get(f"spark.databricks.clusterUsageTags.{name}", "")
    return (
        f"{tag('sparkVersion')} driver={tag('driverNodeType')} "
        f"workers={tag('clusterTargetWorkers')}x{tag('clusterNodeType')}"
    )
