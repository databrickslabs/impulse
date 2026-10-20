# pylint: disable=missing-function-docstring
"""Calculated channels that are pushdowns as a whole are solved without the pandas UDF.

Each case solves the same calculated channels with pushdown off (pandas UDF) and on
(native when every channel qualifies) and requires identical rows, types, nulls and
NaNs.
"""

import pandas as pd
import pyspark.sql.functions as F
import pytest
from pyspark.sql import DataFrame

from impulse_query_engine.analyze.query.channels.calculated_channel import CalculatedChannel
from impulse_query_engine.analyze.query.optimizer import ResamplePushdownRule, query_optimizer
from impulse_query_engine.analyze.query.solvers.default_solver import DefaultSolver
from impulse_query_engine.analyze.query.solvers.solver_config import (
    ChannelMappingConfig,
    SolverConfig,
    TableConfig,
)
from impulse_query_engine.measurement_db import MeasurementDB, MeasurementDBConfig
from tests.conftest import spark
from tests.impulse_query_engine.unit.analyze.query.optimizer.pipeline_helpers import DiffRule
from tests.impulse_query_engine.unit.analyze.query.solvers.default_solver_pushdown_test import (
    PUSHED_SCANS,
    SAMPLE_SHUFFLE,
    channel_data_scans,
    container_shuffles_of_channel_data,
    shuffles,
    small_chunks,
)

RATE = 1e7  # 10 s in µs; the fixture channels are sampled roughly every 0.9 s
_KVS_SCHEMA = "spark_catalog.silver_key_value_store"


def _kvs_config() -> SolverConfig:
    return SolverConfig(
        project_id="SAMPLE_PROJECT",
        container_tags=TableConfig(column_name_mapping={"element_id": "key"}),
        container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
        channel_mapping=ChannelMappingConfig(),
    )


def _unit_conversion_config() -> SolverConfig:
    return SolverConfig(
        project_id="SAMPLE_PROJECT",
        container_metrics=TableConfig(column_name_mapping={"project": "project_id"}),
        channel_mapping=ChannelMappingConfig(filters={"toolbox_id": "container_concept"}),
    )


@pytest.fixture
def udf_calls(monkeypatch) -> list:
    """Number of grouped-map UDF runs, per solve."""
    calls: list[int] = []
    original = DefaultSolver._apply_grouped_map

    def _counting(self, *args, **kwargs):
        calls[-1] += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(DefaultSolver, "_apply_grouped_map", _counting)
    return calls


def _solve(spark, db, make_channels, enable_pushdown, udf_calls=None, **solver_kwargs):
    if udf_calls is not None:
        udf_calls.append(0)
    q = db.query
    solver = DefaultSolver(spark, enable_pushdown=enable_pushdown, **solver_kwargs)
    return q.select(*make_channels(q)).solve_calculated_channels(spark, solver=solver)


def _nan_and_null_counts(df: DataFrame) -> tuple:
    return tuple(
        df.select(
            F.sum(F.isnan("value").cast("int")), F.sum(F.col("value").isNull().cast("int"))
        ).first()
    )


def _rows(df: DataFrame) -> pd.DataFrame:
    pdf = df.toPandas()
    pdf["identity"] = pdf["identity"].map(lambda m: tuple(sorted(m.items())))
    return pdf.sort_values(["container_id", "channel_id", "tstart", "tend"]).reset_index(drop=True)


def _assert_same(native: DataFrame, udf: DataFrame) -> pd.DataFrame:
    assert [(f.name, f.dataType) for f in native.schema] == [
        (f.name, f.dataType) for f in udf.schema
    ]
    assert _nan_and_null_counts(native) == _nan_and_null_counts(udf)
    expected = _rows(udf)
    assert len(expected) > 0
    pd.testing.assert_frame_equal(_rows(native), expected, check_exact=True)
    return expected


def _solve_both(spark, db, make_channels, udf_calls, **solver_kwargs) -> pd.DataFrame:
    udf = _solve(spark, db, make_channels, False, udf_calls, **solver_kwargs)
    native = _solve(spark, db, make_channels, True, udf_calls, **solver_kwargs)
    return _assert_same(native, udf), native


@pytest.mark.usefixtures("small_chunks")
class TestNativeCalculatedChannels:
    def test_resampled_channels_skip_the_udf(self, spark, key_value_store_db, udf_calls):
        def channels(q):
            return [
                CalculatedChannel(
                    q.channel(channel_name="Engine RPM").resample(RATE), {"channel_name": "rpm"}
                ),
                CalculatedChannel(
                    q.channel(channel_name="Vehicle Speed Sensor").resample(RATE / 2),
                    {"channel_name": "speed", "data_key": "CALC"},
                ),
            ]

        rows, native = _solve_both(
            spark, key_value_store_db, channels, udf_calls, config=_kvs_config()
        )
        assert udf_calls == [1, 0]
        assert rows["value"].gt(0).any()
        assert rows["channel_id"].nunique() == 2
        plan = native._jdf.queryExecution().executedPlan().toString()
        assert "FlatMapGroupsInPandas" not in plan
        assert channel_data_scans(native) == PUSHED_SCANS
        # The samples are shuffled by series and chunk, never by container.
        assert len([e for e in shuffles(native) if e.startswith(SAMPLE_SHUFFLE)]) == 1
        assert container_shuffles_of_channel_data(native) == 0

    def test_two_rules_share_the_reads(self, spark, key_value_store_db, udf_calls, monkeypatch):
        monkeypatch.setattr(
            query_optimizer, "default_rules", lambda: (ResamplePushdownRule(), DiffRule())
        )

        def channels(q):
            return [
                CalculatedChannel(
                    q.channel(channel_name="Engine RPM").resample(RATE), {"channel_name": "rpm"}
                ),
                CalculatedChannel(
                    q.channel(channel_name="Vehicle Speed Sensor").diff(),
                    {"channel_name": "speed_diff"},
                ),
            ]

        rows, native = _solve_both(
            spark, key_value_store_db, channels, udf_calls, config=_kvs_config()
        )
        assert udf_calls == [1, 0]
        assert rows["channel_id"].nunique() == 2
        assert rows["value"].ne(0).any()
        assert channel_data_scans(native) == PUSHED_SCANS
        assert len([e for e in shuffles(native) if e.startswith(SAMPLE_SHUFFLE)]) == 1

    def test_same_expression_under_two_identities_and_two_rates(
        self, spark, key_value_store_db, udf_calls
    ):
        def channels(q):
            rpm = q.channel(channel_name="Engine RPM")
            return [
                CalculatedChannel(rpm.resample(RATE), {"channel_name": "rpm_a"}),
                CalculatedChannel(rpm.resample(RATE), {"channel_name": "rpm_b"}),
                CalculatedChannel(rpm.resample(RATE / 4), {"channel_name": "rpm_c"}),
            ]

        rows, _ = _solve_both(spark, key_value_store_db, channels, udf_calls, config=_kvs_config())
        assert udf_calls == [1, 0]
        assert rows["channel_id"].nunique() == 3

    def test_mixed_selection_uses_the_udf(self, spark, key_value_store_db, udf_calls):
        def channels(q):
            rpm = q.channel(channel_name="Engine RPM")
            return [
                CalculatedChannel(rpm.resample(RATE), {"channel_name": "rpm"}),
                CalculatedChannel(rpm.resample(RATE) * 2, {"channel_name": "rpm_x2"}),
            ]

        _solve_both(spark, key_value_store_db, channels, udf_calls, config=_kvs_config())
        assert udf_calls == [1, 1]

    def test_aliased_and_direct_selection_of_one_channel_with_unit_conversion(
        self, spark, key_value_store_unit_conversion_db, udf_calls
    ):
        # vehicle_speed resolves to the "Vehicle Speed Sensor" channel; only the
        # aliased selection converts its values.
        def channels(q):
            return [
                CalculatedChannel(
                    q.channel_with_alias(channel_alias="vehicle_speed").resample(RATE),
                    {"channel_name": "speed_converted"},
                ),
                CalculatedChannel(
                    q.channel(channel_name="Vehicle Speed Sensor").resample(RATE),
                    {"channel_name": "speed_raw"},
                ),
            ]

        rows, _ = _solve_both(
            spark,
            key_value_store_unit_conversion_db,
            channels,
            udf_calls,
            config=_unit_conversion_config(),
        )
        assert udf_calls == [1, 0]
        by_channel = rows.groupby("channel_id")["value"].sum()
        assert len(by_channel) == 2 and by_channel.iloc[0] != by_channel.iloc[1]

    def test_nan_conversion_factor_leaves_values_unconverted(
        self, spark, unit_conversion_dataframes, mock_workspace_client, udf_calls
    ):
        # vehicle_speed converts km/h to m/s; a NaN km/h factor must be skipped like
        # the pandas evaluation does, not turn every value into null.
        tables = dict(unit_conversion_dataframes)
        tables["unit_conversion"] = tables["unit_conversion"].withColumn(
            "conversion_factor",
            F.when(F.col("unit") == "km/h", F.lit(float("nan"))).otherwise(
                F.col("conversion_factor").cast("double")
            ),
        )
        cfg = MeasurementDBConfig.for_debug(tables)
        cfg.channel_mapping_table = "channel_mapping"
        cfg.unit_conversion_table = "unit_conversion"
        db = MeasurementDB(cfg, ws=mock_workspace_client)

        def channels(q):
            return [
                CalculatedChannel(
                    q.channel_with_alias(channel_alias="vehicle_speed").resample(RATE),
                    {"channel_name": "speed_aliased"},
                ),
                CalculatedChannel(
                    q.channel(channel_name="Vehicle Speed Sensor").resample(RATE),
                    {"channel_name": "speed_raw"},
                ),
            ]

        rows, _ = _solve_both(spark, db, channels, udf_calls, config=_unit_conversion_config())
        assert udf_calls == [1, 0]
        aliased, raw = (
            rows[rows["identity"] == (("channel_name", name),)]
            for name in ("speed_aliased", "speed_raw")
        )
        same_samples = aliased.merge(raw, on=["container_id", "tstart", "tend"])
        assert len(same_samples) > 0 and same_samples["value_x"].notna().all()
        pd.testing.assert_series_equal(
            same_samples["value_x"], same_samples["value_y"], check_names=False
        )

    def test_raw_data(self, spark, setup_raw_channels_db, mock_workspace_client, udf_calls):
        tables = {
            name: spark.read.table(f"spark_catalog.silver_raw.{name}")
            for name in ("container_metrics", "channel_metrics", "channels")
        }
        db = MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=mock_workspace_client)

        def channels(q):
            rpm = q.channel(channel_name="Engine RPM")
            return [CalculatedChannel(rpm.resample(2e6), {"channel_name": "rpm_2s"})]

        _solve_both(spark, db, channels, udf_calls, is_raw_data=True)
        assert udf_calls == [1, 0]

    def test_ns_timestamps_nan_values_and_a_single_sample(
        self, spark, setup_key_value_store_db, mock_workspace_client, udf_calls
    ):
        # Timestamps beyond 2^53 (not exact in float64), NaN values, and container 3's
        # Engine RPM reduced to a single sample (passed through by resample).
        tables = {
            name: spark.read.table(f"{_KVS_SCHEMA}.{name}")
            for name in ("container_tags", "container_metrics", "channel_metrics", "channels")
        }
        data = tables["channels"]
        rpm_3 = (F.col("container_id") == 3) & (F.col("channel_id") == 5)
        first_3 = data.where(rpm_3).agg(F.min("tstart")).first()[0]
        rpm_1 = (F.col("container_id") == 1) & (F.col("channel_id") == 5)
        first_1 = data.where(rpm_1).agg(F.min("tstart")).first()[0]
        tables["channels"] = (
            data.where(~rpm_3 | (F.col("tstart") == first_3))
            .withColumn("tstart", F.col("tstart") * 1000 + 1)
            .withColumn("tend", F.col("tend") * 1000 + 1)
            .withColumn(
                "value",
                # tstart is already scaled here; every sample but the first becomes NaN.
                F.when(
                    rpm_1 & (F.col("tstart") > first_1 * 1000 + 1), F.lit(float("nan"))
                ).otherwise(F.col("value").cast("double")),
            )
        )
        db = MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=mock_workspace_client)

        def channels(q):
            rpm = q.channel(channel_name="Engine RPM")
            return [CalculatedChannel(rpm.resample(RATE * 1000), {"channel_name": "rpm_ns"})]

        rows, native = _solve_both(spark, db, channels, udf_calls, config=_kvs_config())
        assert udf_calls == [1, 0]
        assert rows["tstart"].max() > 2**53
        assert len(rows[rows.container_id == 3]) == 1
        assert _nan_and_null_counts(native)[1] > 0


@pytest.mark.parametrize("channel_id_type", ["short", "string"])
@pytest.mark.parametrize("enable_pushdown", [False, True])
def test_channel_id_must_be_integer_or_long(
    spark,
    setup_key_value_store_db,
    mock_workspace_client,
    monkeypatch,
    channel_id_type,
    enable_pushdown,
):
    def _prelude(*args, **kwargs):
        raise AssertionError("the channel data was prepared before the channel_id check")

    # The check reads only the schema, before the prelude runs its first Spark job.
    monkeypatch.setattr(DefaultSolver, "_prepare_channels_join", _prelude)
    tables = {
        name: spark.read.table(f"{_KVS_SCHEMA}.{name}")
        for name in ("container_tags", "container_metrics", "channel_metrics", "channels")
    }
    for name in ("channel_metrics", "channels"):
        tables[name] = tables[name].withColumn(
            "channel_id", F.col("channel_id").cast(channel_id_type)
        )
    db = MeasurementDB(MeasurementDBConfig.for_debug(tables), ws=mock_workspace_client)

    def channels(q):
        rpm = q.channel(channel_name="Engine RPM")
        return [CalculatedChannel(rpm.resample(RATE), {"channel_name": "rpm"})]

    with pytest.raises(ValueError, match="integer or long channel_id"):
        _solve(spark, db, channels, enable_pushdown, config=_kvs_config())
