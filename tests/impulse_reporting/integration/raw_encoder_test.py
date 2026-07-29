"""Integration tests: the RAW encoders on our test dataset.

``data_type=RAW`` supports two encoders (``QueryEngineConfig.raw_encoder``):

- ``RLE`` (default) collapses consecutive equal-valued samples into a single
  ``[tstart, tend)`` interval per run.
- ``INTERVAL`` keeps every original sample and only derives ``tend``.

The two produce a *different number of intervals* for the RAW signal in
``raw_encoder_csv/channels.csv`` (its RPM samples repeat consecutively, so
RLE merges runs while INTERVAL keeps every sample), but because a time series
is considered valid within its intervals, any duration-weighted aggregate
must be identical.

The dataset also carries an ``is_plausible`` column with one implausible
sample in the middle of the ``3100`` run.  With ``drop_implausible_data`` the
two encoders reach that point differently -- RLE forces an interval boundary
at the implausible sample (via the ``& is_plausible`` term) before dropping
it, INTERVAL simply filters it -- so the tests assert they *still* agree, and
that dropping the sample measurably removes its duration.
"""

from unittest.mock import create_autospec

import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from impulse_query_engine.analyze.query.solvers.solver_config import RawEncoder
from impulse_reporting.aggregations.histogram import HistogramDuration
from impulse_reporting.config.config_parser import ImpulseConfig, Source, QueryEngine, DataType
from impulse_reporting.core.page import Page
from impulse_reporting.core.report import Report

# Duration-weighted histogram of Engine RPM over the RAW samples in
# ``raw_encoder_csv/channels.csv``.
EXPECTED_RPM_HISTOGRAM = {
    "0.0-250.0": 5_000_000.0,
    "500.0-750.0": 1_000_000.0,
    "1500.0-1750.0": 5_000_000.0,
    "2500.0-2750.0": 2_000_000.0,
    "3000.0-3250.0": 3_000_000.0,
}

# Same signal with ``drop_implausible_data=True``.  One of the three ``3100``
# samples is flagged implausible, so its 1,000,000 us span is removed and the
# ``3000.0-3250.0`` bin drops from 3,000,000 to 2,000,000 us.  Every other bin
# is unaffected.  Both encoders must agree on this outcome.
EXPECTED_RPM_HISTOGRAM_DROP_IMPLAUSIBLE = {
    "0.0-250.0": 5_000_000.0,
    "500.0-750.0": 1_000_000.0,
    "1500.0-1750.0": 5_000_000.0,
    "2500.0-2750.0": 2_000_000.0,
    "3000.0-3250.0": 2_000_000.0,
}


def _run_histogram_report(
    spark: SparkSession, raw_encoder: RawEncoder, drop_implausible_data: bool = False
):
    """Run a sinkless HistogramDuration report over Engine RPM with the given encoder."""
    config = ImpulseConfig(
        source=Source(
            container_metrics_table="spark_catalog.silver_raw.container_metrics",
            channel_metrics_table="spark_catalog.silver_raw.channel_metrics",
            channels_uri="spark_catalog.silver_raw.channels",
        ),
        query_engine=QueryEngine(
            data_type=DataType.RAW,
            raw_encoder=raw_encoder,
            drop_implausible_data=drop_implausible_data,
        ),
    )
    report = Report(
        name="raw_encoder_equivalence",
        spark=spark,
        workspace_client=create_autospec(WorkspaceClient),
        config=dict(config),
    )

    engine_rpm = report.get_db().query.channel(channel_name="Engine RPM")
    page = Page(page_number=1)
    report.add_page(page)
    page.add_aggregation(
        HistogramDuration(
            name="rpm_hist_encoder_equivalence",
            base_expr=engine_rpm,
            bins=[float(i) for i in range(0, 8000, 250)],
        )
    )

    report.determine_report()
    return report.aggregation_dfs["HISTOGRAM"]["changed"]


def _filter_populated_bins(hist_df) -> dict[str, float]:
    """Collect a duration histogram into ``{bin_name: hist_value}`` of non-empty bins."""
    return {
        row["bin_name"]: row["hist_value"]
        for row in hist_df.filter(F.col("hist_value") > 0).collect()
    }


# ---------------------------------------------------------------------------
# drop_implausible_data = False (default): all samples kept
# ---------------------------------------------------------------------------


def test_rle_encoder_produces_expected_histogram(spark, setup_raw_channels_db):
    """The RLE encoder yields the expected duration histogram for Engine RPM.

    RLE merges the repeated RPM samples into fewer intervals, but the
    duration-weighted histogram must still match the expected values.
    """
    hist_rle = _run_histogram_report(spark, RawEncoder.RLE)
    assert _filter_populated_bins(hist_rle) == EXPECTED_RPM_HISTOGRAM


def test_interval_encoder_produces_expected_histogram(spark, setup_raw_channels_db):
    """The INTERVAL encoder yields the expected duration histogram for Engine RPM.

    INTERVAL keeps one interval per raw sample, but the duration-weighted
    histogram must match the same expected values as RLE.
    """
    hist_interval = _run_histogram_report(spark, RawEncoder.INTERVAL)
    assert _filter_populated_bins(hist_interval) == EXPECTED_RPM_HISTOGRAM


def test_rle_and_interval_encoders_produce_same_histogram(spark, setup_raw_channels_db):
    """RLE and INTERVAL encoders yield identical duration histograms.

    The encoders emit a different number of intervals for the same RAW signal
    (RLE merges equal-valued runs, INTERVAL keeps every sample), but the
    duration-weighted histogram over Engine RPM must be identical bin for bin.
    """
    hist_rle = _run_histogram_report(spark, RawEncoder.RLE)
    hist_interval = _run_histogram_report(spark, RawEncoder.INTERVAL)
    assert _filter_populated_bins(hist_rle) == _filter_populated_bins(hist_interval)


# ---------------------------------------------------------------------------
# drop_implausible_data = True: the implausible 3100 sample is removed
# ---------------------------------------------------------------------------


def test_rle_encoder_drops_implausible_sample(spark, setup_raw_channels_db):
    """RLE with ``drop_implausible_data`` removes the implausible sample's duration.

    The implausible sample forces an interval boundary before being dropped,
    so the ``3000.0-3250.0`` bin loses exactly its 1,000,000 us span.
    """
    hist_rle = _run_histogram_report(spark, RawEncoder.RLE, drop_implausible_data=True)
    assert _filter_populated_bins(hist_rle) == EXPECTED_RPM_HISTOGRAM_DROP_IMPLAUSIBLE


def test_interval_encoder_drops_implausible_sample(spark, setup_raw_channels_db):
    """INTERVAL with ``drop_implausible_data`` removes the implausible sample's duration.

    INTERVAL filters the implausible sample directly; the ``3000.0-3250.0``
    bin loses the same 1,000,000 us span as under RLE.
    """
    hist_interval = _run_histogram_report(spark, RawEncoder.INTERVAL, drop_implausible_data=True)
    assert _filter_populated_bins(hist_interval) == EXPECTED_RPM_HISTOGRAM_DROP_IMPLAUSIBLE


def test_encoders_agree_when_dropping_implausible(spark, setup_raw_channels_db):
    """RLE and INTERVAL agree even though they drop the implausible sample differently.

    RLE forces an interval boundary at the implausible sample (via the
    ``& is_plausible`` term) and then removes it; INTERVAL simply filters it.
    Despite the different internal handling, both must produce the expected
    drop-implausible histogram bin for bin.
    """
    hist_rle = _run_histogram_report(spark, RawEncoder.RLE, drop_implausible_data=True)
    hist_interval = _run_histogram_report(spark, RawEncoder.INTERVAL, drop_implausible_data=True)
    assert _filter_populated_bins(hist_rle) == EXPECTED_RPM_HISTOGRAM_DROP_IMPLAUSIBLE
    assert _filter_populated_bins(hist_interval) == EXPECTED_RPM_HISTOGRAM_DROP_IMPLAUSIBLE
