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
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=3.0, value=10.0),
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
