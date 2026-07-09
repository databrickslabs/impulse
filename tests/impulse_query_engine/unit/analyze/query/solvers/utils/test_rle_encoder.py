"""Tests for the PySpark run-length (RLE) encoder utility."""

import pyspark.sql.types as T
import pytest
from pyspark.sql import Row, SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from impulse_query_engine.analyze.query.solvers.solver_config import SolverConfig
from impulse_query_engine.analyze.query.solvers.utils.rle_encoder import RleEncoder

silver_schema_without_rle = T.StructType(
    [
        T.StructField("container_id", T.StringType(), True),
        T.StructField("channel_id", T.StringType(), True),
        T.StructField("timestamp", T.DoubleType(), True),
        T.StructField("value", T.DoubleType(), True),
    ]
)

silver_rle_encoded_schema = T.StructType(
    [
        T.StructField("container_id", T.StringType(), True),
        T.StructField("channel_id", T.StringType(), True),
        T.StructField("tstart", T.DoubleType(), True),
        T.StructField("tend", T.DoubleType(), True),
        T.StructField("value", T.DoubleType(), True),
    ]
)

silver_schema_with_plausible = T.StructType(
    silver_schema_without_rle.fields
    + [T.StructField("is_plausible", T.BooleanType(), True)]
)


class TestRleEncoder:
    """Test class for RLE encoder functionality."""

    def test_prepare_channels_df_single_channel_rle_compression(self, spark: SparkSession):
        """Test that consecutive equal values are merged into runs.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 0.0       | 10.0  |
            | c1           | ch1        | 1.0       | 10.0  |
            | c1           | ch1        | 2.0       | 10.0  |
            | c1           | ch1        | 3.0       | 20.0  |
            | c1           | ch1        | 4.0       | 20.0  |
            | c1           | ch1        | 5.0       | 30.0  |

        Expects 3 intervals -- one per constant-value run.  Each run's ``tend``
        is the timestamp at which the value next changes; the final run ends at
        its last observed timestamp.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=20.0),
            Row(container_id="c1", channel_id="ch1", timestamp=4.0, value=20.0),
            Row(container_id="c1", channel_id="ch1", timestamp=5.0, value=30.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = RleEncoder(SolverConfig()).prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=3.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=3.0, tend=5.0, value=20.0),
            Row(container_id="c1", channel_id="ch1", tstart=5.0, tend=5.0, value=30.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_all_identical_values(self, spark: SparkSession):
        """Test that a channel with a single repeated value collapses to one run.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 0.0       | 5.0   |
            | c1           | ch1        | 1.0       | 5.0   |
            | c1           | ch1        | 2.0       | 5.0   |
            | c1           | ch1        | 3.0       | 5.0   |

        Expects 1 interval spanning the whole channel.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=5.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=5.0),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=5.0),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=5.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = RleEncoder(SolverConfig()).prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=3.0, value=5.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_all_distinct_values(self, spark: SparkSession):
        """Test that a channel with no repeats yields one interval per sample.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 0.0       | 10.0  |
            | c1           | ch1        | 1.0       | 20.0  |
            | c1           | ch1        | 2.0       | 30.0  |
            | c1           | ch1        | 3.0       | 40.0  |

        Expects 4 intervals (no compression possible); the last row has
        ``tend = tstart``.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=20.0),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=30.0),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=40.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = RleEncoder(SolverConfig()).prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=1.0, tend=2.0, value=20.0),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=3.0, value=30.0),
            Row(container_id="c1", channel_id="ch1", tstart=3.0, tend=3.0, value=40.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_multiple_channels(self, spark: SparkSession):
        """Test run-length encoding for multiple channels with different patterns.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 0.0       | 10.0  |
            | c1           | ch1        | 1.0       | 10.0  |
            | c1           | ch1        | 2.0       | 10.0  |
            | c1           | ch2        | 0.0       | 100.0 |
            | c1           | ch2        | 1.0       | 200.0 |
            | c1           | ch2        | 2.0       | 200.0 |
            | c1           | ch3        | 0.0       | 300.0 |

        Expects ch1 collapsed to one run, ch2 to two runs, ch3 a single row.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch2", timestamp=0.0, value=100.0),
            Row(container_id="c1", channel_id="ch2", timestamp=1.0, value=200.0),
            Row(container_id="c1", channel_id="ch2", timestamp=2.0, value=200.0),
            Row(container_id="c1", channel_id="ch3", timestamp=0.0, value=300.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = RleEncoder(SolverConfig()).prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch2", tstart=0.0, tend=1.0, value=100.0),
            Row(container_id="c1", channel_id="ch2", tstart=1.0, tend=2.0, value=200.0),
            Row(container_id="c1", channel_id="ch3", tstart=0.0, tend=0.0, value=300.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_multiple_containers(self, spark: SparkSession):
        """Test that each (container_id, channel_id) partition is encoded independently.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 0.0       | 10.0  |
            | c1           | ch1        | 1.0       | 10.0  |
            | c2           | ch1        | 0.0       | 20.0  |
            | c2           | ch1        | 1.0       | 30.0  |
            | c1           | ch2        | 0.0       | 100.0 |

        Expects c1/ch1 collapsed to one run; the same channel_id in c2 is
        treated separately.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0),
            Row(container_id="c2", channel_id="ch1", timestamp=0.0, value=20.0),
            Row(container_id="c2", channel_id="ch1", timestamp=1.0, value=30.0),
            Row(container_id="c1", channel_id="ch2", timestamp=0.0, value=100.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = RleEncoder(SolverConfig()).prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=10.0),
            Row(container_id="c2", channel_id="ch1", tstart=0.0, tend=1.0, value=20.0),
            Row(container_id="c2", channel_id="ch1", tstart=1.0, tend=1.0, value=30.0),
            Row(container_id="c1", channel_id="ch2", tstart=0.0, tend=0.0, value=100.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_single_point(self, spark: SparkSession):
        """Test run-length encoding with a single data point per channel.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 5.0       | 42.0  |
            | c1           | ch2        | 10.0      | 84.0  |

        Expects 2 intervals, each with ``tend = tstart`` (no successor row).
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=5.0, value=42.0),
            Row(container_id="c1", channel_id="ch2", timestamp=10.0, value=84.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = RleEncoder(SolverConfig()).prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=5.0, tend=5.0, value=42.0),
            Row(container_id="c1", channel_id="ch2", tstart=10.0, tend=10.0, value=84.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_unsorted_timestamps(self, spark: SparkSession):
        """Test that unsorted input is ordered before run detection.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 3.0       | 10.0  |
            | c1           | ch1        | 1.0       | 10.0  |
            | c1           | ch1        | 2.0       | 10.0  |
            | c1           | ch1        | 0.0       | 10.0  |
            | c1           | ch1        | 4.0       | 20.0  |

        Expects 2 intervals: the four ``10.0`` samples merge into one run after
        the window sorts by timestamp, then ``20.0`` starts a new run.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=4.0, value=20.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = RleEncoder(SolverConfig()).prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=4.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=4.0, tend=4.0, value=20.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_duplicate_timestamps(self, spark: SparkSession):
        """Test that duplicate (timestamp, value) rows fold into the surrounding run.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 0.0       | 10.0  |
            | c1           | ch1        | 1.0       | 10.0  |
            | c1           | ch1        | 1.0       | 10.0  |  <-- duplicate
            | c1           | ch1        | 2.0       | 20.0  |

        Expects 2 intervals: the repeated ``10.0`` samples (including the
        duplicate) collapse into a single run.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=20.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = RleEncoder(SolverConfig()).prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=2.0, value=20.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_null_values_not_merged(self, spark: SparkSession):
        """Test that consecutive nulls are NOT merged into a run.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 0.0       | 10.0  |
            | c1           | ch1        | 1.0       | None  |
            | c1           | ch1        | 2.0       | None  |
            | c1           | ch1        | 3.0       | 10.0  |

        Expects 4 intervals.  Because the run-change flag uses ``==`` (not
        null-safe), a NULL compared to any value -- including another NULL --
        yields NULL and starts a new run, so consecutive nulls do not collapse.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=None),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=None),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=10.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = RleEncoder(SolverConfig()).prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=1.0, tend=2.0, value=None),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=3.0, value=None),
            Row(container_id="c1", channel_id="ch1", tstart=3.0, tend=3.0, value=10.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_negative_timestamps(self, spark: SparkSession):
        """Test run-length encoding with negative timestamps.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | -2.0      | 10.0  |
            | c1           | ch1        | -1.0      | 10.0  |
            | c1           | ch1        | 0.0       | 20.0  |
            | c1           | ch1        | 1.0       | 20.0  |

        Expects 2 intervals; negative timestamps sort correctly via the window.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=-2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=-1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=20.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=20.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = RleEncoder(SolverConfig()).prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=-2.0, tend=0.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=20.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_empty_dataframe(self, spark: SparkSession):
        """Test run-length encoding with an empty DataFrame.

        Input:
            (empty -- no rows)

        Expects an empty result with the RLE schema.
        """
        df = spark.createDataFrame([], silver_schema_without_rle)
        result = RleEncoder(SolverConfig()).prepare_channels_df(df)

        expected_result = spark.createDataFrame([], silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_assign_run_ids(self, spark: SparkSession):
        """Test that _assign_run_ids tags each row with prev_value/next_time/run id.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 1.0       | 10.0  |
            | c1           | ch1        | 2.0       | 10.0  |
            | c1           | ch1        | 3.0       | 20.0  |

        Expected:
            prev_value = LAG(value); next_time = LEAD(timestamp) (own ts for the
            last row); value_diff = 1 on a change else 0; value_id = the running
            sum of value_diff (constant within a run).
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=20.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = RleEncoder(SolverConfig())._assign_run_ids(df)

        expected_schema = T.StructType(
            silver_schema_without_rle.fields
            + [
                T.StructField("prev_value", T.DoubleType(), True),
                T.StructField("next_time", T.DoubleType(), True),
                T.StructField("value_diff", T.IntegerType(), False),
                T.StructField("value_id", T.LongType(), True),
            ]
        )
        expected_data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=10.0,
                prev_value=None,
                next_time=2.0,
                value_diff=1,
                value_id=1,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                value=10.0,
                prev_value=10.0,
                next_time=3.0,
                value_diff=0,
                value_id=1,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=3.0,
                value=20.0,
                prev_value=10.0,
                next_time=3.0,
                value_diff=1,
                value_id=2,
            ),
        ]
        expected_result = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result, expected_result, checkRowOrder=False)

    def test_prepare_channels_df_drops_implausible(self, spark: SparkSession):
        """Test that implausible samples are dropped before run detection.

        Input (drop_implausible_data_points=True):
            | container_id | channel_id | timestamp | value | is_plausible |
            |--------------|------------|-----------|-------|--------------|
            | c1           | ch1        | 0.0       | 10.0  | True         |
            | c1           | ch1        | 1.0       | 10.0  | True         |
            | c1           | ch1        | 2.0       | 999.0 | False        |
            | c1           | ch1        | 3.0       | 10.0  | True         |

        Expects 1 interval: the implausible ``999.0`` sample is removed before
        encoding, so the remaining ``10.0`` samples collapse into one run.
        """
        schema = T.StructType(
            silver_schema_without_rle.fields
            + [T.StructField("is_plausible", T.BooleanType(), True)]
        )
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0, is_plausible=True),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0, is_plausible=True),
            Row(
                container_id="c1", channel_id="ch1", timestamp=2.0, value=999.0, is_plausible=False
            ),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=10.0, is_plausible=True),
        ]
        df = spark.createDataFrame(data, schema)
        result = RleEncoder(SolverConfig(), drop_implausible_data_points=True).prepare_channels_df(
            df
        )

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=3.0, tend=3.0, value=10.0)
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_keeps_implausible_when_disabled(self, spark: SparkSession):
        """Test that an implausible point inside the df is kept when filtering is off.

        Input (drop_implausible_data_points=False -- the default; the
        ``is_plausible`` column is present but must NOT be consulted):
            | container_id | channel_id | timestamp | value | is_plausible |
            |--------------|------------|-----------|-------|--------------|
            | c1           | ch1        | 0.0       | 10.0  | True         |
            | c1           | ch1        | 1.0       | 10.0  | True         |
            | c1           | ch1        | 2.0       | 999.0 | False        |  <-- implausible, mid-run
            | c1           | ch1        | 3.0       | 10.0  | True         |

        Expects 3 intervals: the implausible ``999.0`` sample is retained and
        forms its own run, splitting the surrounding ``10.0`` run. The
        ``is_plausible`` column is dropped from the output by the run aggregation.
        """
        schema = T.StructType(
            silver_schema_without_rle.fields
            + [T.StructField("is_plausible", T.BooleanType(), True)]
        )
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0, is_plausible=True),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0, is_plausible=True),
            Row(
                container_id="c1", channel_id="ch1", timestamp=2.0, value=999.0, is_plausible=False
            ),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=10.0, is_plausible=True),
        ]
        df = spark.createDataFrame(data, schema)
        # drop_implausible_data_points defaults to False
        result = RleEncoder(SolverConfig()).prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=3.0, value=999.0),
            Row(container_id="c1", channel_id="ch1", tstart=3.0, tend=3.0, value=10.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_remove_implausible_data_points(self, spark: SparkSession):
        """Test that _remove_implausible_data_points respects the flag and column.

        Input:
            | container_id | channel_id | timestamp | value  | is_plausible |
            |--------------|------------|-----------|--------|--------------|
            | c1           | ch1        | 0.0       | 0.0    | True         |
            | c1           | ch1        | 1.0       | 1000.0 | False        |
            | c1           | ch1        | 2.0       | 0.0    | None         |

        With filtering enabled, expects only the first row (False and None are
        dropped).  With filtering disabled, expects all rows unchanged.
        """
        schema = T.StructType(
            silver_schema_without_rle.fields
            + [T.StructField("is_plausible", T.BooleanType(), True)]
        )
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=0.0, is_plausible=True),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=1000.0,
                is_plausible=False,
            ),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=0.0, is_plausible=None),
        ]
        df = spark.createDataFrame(data, schema)

        result = RleEncoder(
            SolverConfig(), drop_implausible_data_points=True
        )._remove_implausible_data_points(df)
        expected_data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=0.0, is_plausible=True),
        ]
        assertDataFrameEqual(result, spark.createDataFrame(expected_data, schema))

        result_no_filter = RleEncoder(
            SolverConfig(), drop_implausible_data_points=False
        )._remove_implausible_data_points(df)
        assertDataFrameEqual(result_no_filter, df)

    def test_remove_implausible_data_points_missing_column(self, spark: SparkSession):
        """Test that a ValueError is raised when is_plausible is missing but required.

        Input:
            (empty DataFrame without an ``is_plausible`` column)

        With filtering enabled, expects ValueError; disabled, returns unchanged.
        """
        df = spark.createDataFrame([], silver_schema_without_rle)

        with pytest.raises(
            ValueError,
            match="DataFrame must contain an 'is_plausible' column",
        ):
            RleEncoder(
                SolverConfig(), drop_implausible_data_points=True
            )._remove_implausible_data_points(df)

        result = RleEncoder(
            SolverConfig(), drop_implausible_data_points=False
        )._remove_implausible_data_points(df)
        assertDataFrameEqual(result, df)

    def test_aggregate_runs(self, spark: SparkSession):
        """Test that _aggregate_runs collapses tagged rows into one row per run.

        Input (already tagged with next_time and value_id):
            | container_id | channel_id | timestamp | next_time | value | value_id |
            |--------------|------------|-----------|-----------|-------|----------|
            | c1           | ch1        | 0.0       | 1.0       | 10.0  | 1        |
            | c1           | ch1        | 1.0       | 2.0       | 10.0  | 1        |
            | c1           | ch1        | 2.0       | 2.0       | 20.0  | 2        |

        Expected:
            Run 1 -> tstart=min(0,1)=0, tend=max(1,2)=2, value=10;
            Run 2 -> tstart=2, tend=2, value=20.  ``value_id`` is dropped.
        """
        tagged_schema = T.StructType(
            [
                T.StructField("container_id", T.StringType(), True),
                T.StructField("channel_id", T.StringType(), True),
                T.StructField("timestamp", T.DoubleType(), True),
                T.StructField("next_time", T.DoubleType(), True),
                T.StructField("value", T.DoubleType(), True),
                T.StructField("value_id", T.LongType(), True),
            ]
        )
        data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=0.0,
                next_time=1.0,
                value=10.0,
                value_id=1,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                next_time=2.0,
                value=10.0,
                value_id=1,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                next_time=2.0,
                value=20.0,
                value_id=2,
            ),
        ]
        df = spark.createDataFrame(data, tagged_schema)
        result = RleEncoder(SolverConfig())._aggregate_runs(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=2.0, value=20.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_drops_implausible_same_value_splits_run(
        self, spark: SparkSession
    ):
        """Test that dropping a same-valued implausible sample splits the interval.

        This is the case that exercises the ``& is_plausible`` term in the
        run-change condition of :meth:`_assign_run_ids`.  Without that term the
        implausible sample would carry ``value_diff = 0`` (its value matches its
        neighbours), so the surrounding samples would share a single run id and,
        after the implausible row is dropped, be *bridged* into one interval.
        With the term the implausible sample forces a run boundary, so dropping
        it splits the run instead.

        Input (drop_implausible_data_points=True):
            | container_id | channel_id | timestamp | value | is_plausible |
            |--------------|------------|-----------|-------|--------------|
            | c1           | ch1        | 0.0       | 10.0  | True         |
            | c1           | ch1        | 1.0       | 10.0  | True         |
            | c1           | ch1        | 2.0       | 10.0  | False        |  <-- same value!
            | c1           | ch1        | 3.0       | 10.0  | True         |

        Expects 2 intervals -- NOT one merged interval.  If the encoder merged
        across the dropped sample the result would instead be a single
        ``(0.0, 3.0, 10.0)`` run, which this test guards against.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0, is_plausible=True),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0, is_plausible=True),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=10.0, is_plausible=False),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=10.0, is_plausible=True),
        ]
        df = spark.createDataFrame(data, silver_schema_with_plausible)
        result = RleEncoder(SolverConfig(), drop_implausible_data_points=True).prepare_channels_df(
            df
        )

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=3.0, tend=3.0, value=10.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_drops_implausible_tail_extends_tend(self, spark: SparkSession):
        """Test that a run's ``tend`` bridges over a dropped trailing implausible sample.

        A dropped implausible sample is removed *after* ``next_time`` is
        computed, so the preceding run's ``tend`` still points at the dropped
        sample's timestamp.  The final interval therefore extends up to the
        instant the implausible reading occurred, rather than stopping at the
        last plausible sample.  This test pins that behaviour down.

        Input (drop_implausible_data_points=True):
            | container_id | channel_id | timestamp | value  | is_plausible |
            |--------------|------------|-----------|--------|--------------|
            | c1           | ch1        | 0.0       | 10.0   | True         |
            | c1           | ch1        | 1.0       | 10.0   | True         |
            | c1           | ch1        | 2.0       | 999.0  | False        |  <-- trailing

        Expects 1 interval ending at ``tend = 2.0`` (the dropped sample's
        timestamp), even though the last plausible sample is at ``1.0``.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0, is_plausible=True),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0, is_plausible=True),
            Row(
                container_id="c1", channel_id="ch1", timestamp=2.0, value=999.0, is_plausible=False
            ),
        ]
        df = spark.createDataFrame(data, silver_schema_with_plausible)
        result = RleEncoder(SolverConfig(), drop_implausible_data_points=True).prepare_channels_df(
            df
        )

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=2.0, value=10.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_drops_leading_implausible(self, spark: SparkSession):
        """Test that a leading implausible sample is dropped and the rest collapse.

        Input (drop_implausible_data_points=True):
            | container_id | channel_id | timestamp | value  | is_plausible |
            |--------------|------------|-----------|--------|--------------|
            | c1           | ch1        | 0.0       | 999.0  | False        |  <-- leading
            | c1           | ch1        | 1.0       | 10.0   | True         |
            | c1           | ch1        | 2.0       | 10.0   | True         |

        Expects 1 interval spanning the two surviving samples.
        """
        data = [
            Row(
                container_id="c1", channel_id="ch1", timestamp=0.0, value=999.0, is_plausible=False
            ),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0, is_plausible=True),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=10.0, is_plausible=True),
        ]
        df = spark.createDataFrame(data, silver_schema_with_plausible)
        result = RleEncoder(SolverConfig(), drop_implausible_data_points=True).prepare_channels_df(
            df
        )

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=1.0, tend=2.0, value=10.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_drops_consecutive_implausible(self, spark: SparkSession):
        """Test that a run of consecutive implausible samples is fully removed.

        Input (drop_implausible_data_points=True):
            | container_id | channel_id | timestamp | value  | is_plausible |
            |--------------|------------|-----------|--------|--------------|
            | c1           | ch1        | 0.0       | 10.0   | True         |
            | c1           | ch1        | 1.0       | 999.0  | False        |
            | c1           | ch1        | 2.0       | 888.0  | False        |
            | c1           | ch1        | 3.0       | 10.0   | True         |

        Expects 2 intervals -- both implausible samples are removed and the
        surrounding ``10.0`` samples are split (not bridged).
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0, is_plausible=True),
            Row(
                container_id="c1", channel_id="ch1", timestamp=1.0, value=999.0, is_plausible=False
            ),
            Row(
                container_id="c1", channel_id="ch1", timestamp=2.0, value=888.0, is_plausible=False
            ),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=10.0, is_plausible=True),
        ]
        df = spark.createDataFrame(data, silver_schema_with_plausible)
        result = RleEncoder(SolverConfig(), drop_implausible_data_points=True).prepare_channels_df(
            df
        )

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=3.0, tend=3.0, value=10.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_drops_entire_partition_when_all_implausible(
        self, spark: SparkSession
    ):
        """Test that a partition with only implausible samples vanishes from the output.

        Input (drop_implausible_data_points=True):
            | container_id | channel_id | timestamp | value  | is_plausible |
            |--------------|------------|-----------|--------|--------------|
            | c1           | ch1        | 0.0       | 10.0   | False        |  <-- all bad
            | c1           | ch1        | 1.0       | 20.0   | False        |  <-- all bad
            | c1           | ch2        | 0.0       | 5.0    | True         |
            | c1           | ch2        | 1.0       | 5.0    | True         |

        Expects only ch2 in the output; ch1 is dropped entirely.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0, is_plausible=False),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=20.0, is_plausible=False),
            Row(container_id="c1", channel_id="ch2", timestamp=0.0, value=5.0, is_plausible=True),
            Row(container_id="c1", channel_id="ch2", timestamp=1.0, value=5.0, is_plausible=True),
        ]
        df = spark.createDataFrame(data, silver_schema_with_plausible)
        result = RleEncoder(SolverConfig(), drop_implausible_data_points=True).prepare_channels_df(
            df
        )

        expected_result_data = [
            Row(container_id="c1", channel_id="ch2", tstart=0.0, tend=1.0, value=5.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_drops_implausible_across_partitions(self, spark: SparkSession):
        """Test that dropping is applied independently per container/channel.

        Input (drop_implausible_data_points=True):
            | container_id | channel_id | timestamp | value  | is_plausible |
            |--------------|------------|-----------|--------|--------------|
            | c1           | ch1        | 0.0       | 10.0   | True         |
            | c1           | ch1        | 1.0       | 999.0  | False        |  <-- different value
            | c1           | ch1        | 2.0       | 10.0   | True         |
            | c2           | ch1        | 0.0       | 20.0   | True         |
            | c2           | ch1        | 1.0       | 20.0   | False        |  <-- same value
            | c2           | ch1        | 2.0       | 20.0   | True         |

        Expects each partition split around its own dropped sample, with no
        cross-partition leakage of run ids.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0, is_plausible=True),
            Row(
                container_id="c1", channel_id="ch1", timestamp=1.0, value=999.0, is_plausible=False
            ),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=10.0, is_plausible=True),
            Row(container_id="c2", channel_id="ch1", timestamp=0.0, value=20.0, is_plausible=True),
            Row(container_id="c2", channel_id="ch1", timestamp=1.0, value=20.0, is_plausible=False),
            Row(container_id="c2", channel_id="ch1", timestamp=2.0, value=20.0, is_plausible=True),
        ]
        df = spark.createDataFrame(data, silver_schema_with_plausible)
        result = RleEncoder(SolverConfig(), drop_implausible_data_points=True).prepare_channels_df(
            df
        )

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=2.0, value=10.0),
            Row(container_id="c2", channel_id="ch1", tstart=0.0, tend=1.0, value=20.0),
            Row(container_id="c2", channel_id="ch1", tstart=2.0, tend=2.0, value=20.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_drops_null_plausible_splits_run(self, spark: SparkSession):
        """Test that a NULL ``is_plausible`` sample is dropped and splits the run.

        A NULL flag is treated as not-plausible: the ``& is_plausible`` term
        evaluates to NULL (falsy) so the sample forces a run boundary, and the
        ``F.col("is_plausible")`` filter drops NULL rows.  The surrounding
        same-valued samples are therefore split rather than bridged.

        Input (drop_implausible_data_points=True):
            | container_id | channel_id | timestamp | value | is_plausible |
            |--------------|------------|-----------|-------|--------------|
            | c1           | ch1        | 0.0       | 10.0  | True         |
            | c1           | ch1        | 1.0       | 10.0  | None         |  <-- NULL
            | c1           | ch1        | 2.0       | 10.0  | True         |

        Expects 2 intervals.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0, is_plausible=True),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0, is_plausible=None),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=10.0, is_plausible=True),
        ]
        df = spark.createDataFrame(data, silver_schema_with_plausible)
        result = RleEncoder(SolverConfig(), drop_implausible_data_points=True).prepare_channels_df(
            df
        )

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=2.0, value=10.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_drops_implausible_matching_following_value(
        self, spark: SparkSession
    ):
        """Test dropping an implausible sample whose value equals the NEXT sample's.

        The following plausible sample compares against the dropped sample's
        value (its ``prev_value``); because they are equal it would carry
        ``value_diff = 0``.  It nonetheless ends up in its own run: the
        implausible sample already opened a fresh run id which the following
        sample simply continues, and once the implausible row is filtered out
        that run stands alone.

        Input (drop_implausible_data_points=True):
            | container_id | channel_id | timestamp | value | is_plausible |
            |--------------|------------|-----------|-------|--------------|
            | c1           | ch1        | 0.0       | 10.0  | True         |
            | c1           | ch1        | 1.0       | 20.0  | False        |  <-- == next
            | c1           | ch1        | 2.0       | 20.0  | True         |

        Expects 2 intervals: ``10.0`` then ``20.0``.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0, is_plausible=True),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=20.0, is_plausible=False),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=20.0, is_plausible=True),
        ]
        df = spark.createDataFrame(data, silver_schema_with_plausible)
        result = RleEncoder(SolverConfig(), drop_implausible_data_points=True).prepare_channels_df(
            df
        )

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=2.0, value=20.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)
