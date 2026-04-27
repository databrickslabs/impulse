"""Tests for PySpark RLE encoder utility."""

import pyspark.sql.types as T
import pytest
from pyspark.sql import Row, SparkSession
from pyspark.testing.utils import assertDataFrameEqual

from mda_query_engine.analyze.query.solvers.utils.interval_encoder import IntervalEncoder

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


class TestRLEEncoder:
    """Test class for RLE encoder functionality."""

    def test_prepare_channels_df_with_existing_tend_column(self, spark: SparkSession):
        """Test that data already in RLE format is returned unchanged.

        Input:
            | container_id | channel_id | tstart | tend | value |
            |--------------|------------|--------|------|-------|
            | c1           | ch1        | 0.0    | 1.0  | 10.0  |
            | c1           | ch1        | 1.0    | 2.0  | 20.0  |
            | c1           | ch2        | 0.0    | 3.0  | 30.0  |

        Expects the same 3 rows back unmodified because ``tend`` already exists.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=1.0, tend=2.0, value=20.0),
            Row(container_id="c1", channel_id="ch2", tstart=0.0, tend=3.0, value=30.0),
        ]
        df = spark.createDataFrame(data, silver_rle_encoded_schema)
        result = IntervalEncoder().prepare_channels_df(df)

        assertDataFrameEqual(result, df)

    def test_prepare_channels_df_missing_timestamp_and_tend(self, spark: SparkSession):
        """Test that a ValueError is raised when neither timestamp nor tend is present.

        Input:
            | container_id | channel_id | value |
            |--------------|------------|-------|
            | c1           | ch1        | 10.0  |

        Expects ValueError because the DataFrame has no ``tend`` or ``timestamp``.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", value=10.0),
        ]
        schema = T.StructType(
            [
                T.StructField("container_id", T.StringType(), True),
                T.StructField("channel_id", T.StringType(), True),
                T.StructField("value", T.DoubleType(), True),
            ]
        )
        df = spark.createDataFrame(data, schema)

        with pytest.raises(ValueError, match="DataFrame must contain either a 'tend' column"):
            IntervalEncoder().prepare_channels_df(df)

    def test_prepare_channels_df_single_channel_rle_compression(self, spark: SparkSession):
        """Test point-to-interval conversion for a single channel with repeated values.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 0.0       | 10.0  |
            | c1           | ch1        | 1.0       | 10.0  |
            | c1           | ch1        | 2.0       | 10.0  |
            | c1           | ch1        | 3.0       | 20.0  |
            | c1           | ch1        | 4.0       | 20.0  |
            | c1           | ch1        | 5.0       | 30.0  |

        Expects 6 intervals.  Each row's ``tend`` equals the next row's
        ``timestamp``; the last row has ``tend = tstart``.
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
        result = IntervalEncoder().prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=1.0, tend=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=3.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=3.0, tend=4.0, value=20.0),
            Row(container_id="c1", channel_id="ch1", tstart=4.0, tend=5.0, value=20.0),
            Row(container_id="c1", channel_id="ch1", tstart=5.0, tend=5.0, value=30.0),
        ]

        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_multiple_channels(self, spark: SparkSession):
        """Test point-to-interval conversion for multiple channels with different patterns.

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

        Expects 7 intervals across 3 channels; ch3 has a single row with
        ``tend = tstart``.
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
        result = IntervalEncoder().prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=1.0, tend=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch2", tstart=0.0, tend=1.0, value=100.0),
            Row(container_id="c1", channel_id="ch2", tstart=1.0, tend=2.0, value=200.0),
            Row(container_id="c1", channel_id="ch2", tstart=2.0, tend=2.0, value=200.0),
            Row(container_id="c1", channel_id="ch3", tstart=0.0, tend=0.0, value=300.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_multiple_containers(self, spark: SparkSession):
        """Test point-to-interval conversion across multiple containers.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 0.0       | 10.0  |
            | c1           | ch1        | 1.0       | 10.0  |
            | c2           | ch1        | 0.0       | 20.0  |
            | c2           | ch1        | 1.0       | 30.0  |
            | c1           | ch2        | 0.0       | 100.0 |

        Expects 5 intervals; each (container_id, channel_id) partition is
        processed independently.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0),
            Row(container_id="c2", channel_id="ch1", timestamp=0.0, value=20.0),
            Row(container_id="c2", channel_id="ch1", timestamp=1.0, value=30.0),
            Row(container_id="c1", channel_id="ch2", timestamp=0.0, value=100.0),
        ]

        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = IntervalEncoder().prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=1.0, tend=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch2", tstart=0.0, tend=0.0, value=100.0),
            Row(container_id="c2", channel_id="ch1", tstart=0.0, tend=1.0, value=20.0),
            Row(container_id="c2", channel_id="ch1", tstart=1.0, tend=1.0, value=30.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_with_null_values(self, spark: SparkSession):
        """Test point-to-interval conversion with null values interspersed.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 0.0       | 10.0  |
            | c1           | ch1        | 1.0       | None  |
            | c1           | ch1        | 2.0       | None  |
            | c1           | ch1        | 3.0       | 10.0  |

        Expects 4 intervals.  Consecutive NULLs share the same value but
        have different timestamps, so they are not considered duplicates.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=None),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=None),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=10.0),
        ]

        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = IntervalEncoder().prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=1.0, tend=2.0, value=None),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=3.0, value=None),
            Row(container_id="c1", channel_id="ch1", tstart=3.0, tend=3.0, value=10.0),
        ]

        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_single_point(self, spark: SparkSession):
        """Test point-to-interval conversion with one data point per channel.

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
        result = IntervalEncoder().prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=5.0, tend=5.0, value=42.0),
            Row(container_id="c1", channel_id="ch2", tstart=10.0, tend=10.0, value=84.0),
        ]
        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_floating_point_precision(self, spark: SparkSession):
        """Test that tiny floating-point differences are not collapsed.

        Input:
            | container_id | channel_id | timestamp | value          |
            |--------------|------------|-----------|----------------|
            | c1           | ch1        | 0.0       | 0.1            |
            | c1           | ch1        | 1.0       | 0.1 + 1e-15    |
            | c1           | ch1        | 2.0       | 0.1            |
            | c1           | ch1        | 3.0       | 0.2            |

        Expects 4 intervals because the tiny difference makes the second
        row's value distinct from the first.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=0.1),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=0.1 + 1e-15),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=0.1),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=0.2),
        ]

        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = IntervalEncoder().prepare_channels_df(df)

        assert result.count() == 4

    def test_prepare_channels_df_empty_dataframe(self, spark: SparkSession):
        """Test point-to-interval conversion with an empty DataFrame.

        Input:
            (empty -- no rows)

        Expects an empty result with the RLE schema.
        """
        df = spark.createDataFrame([], silver_schema_without_rle)
        result = IntervalEncoder().prepare_channels_df(df)

        expected_result = spark.createDataFrame([], silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_unsorted_timestamps(self, spark: SparkSession):
        """Test that unsorted input timestamps are handled correctly.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 3.0       | 10.0  |
            | c1           | ch1        | 1.0       | 10.0  |
            | c1           | ch1        | 2.0       | 10.0  |
            | c1           | ch1        | 0.0       | 10.0  |
            | c1           | ch1        | 4.0       | 20.0  |

        Expects 5 intervals ordered by ``tstart``, with ``tend = tstart``
        for the last row.  The window ``ORDER BY timestamp ASC`` handles
        the sorting.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=4.0, value=20.0),
        ]

        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = IntervalEncoder().prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=1.0, tend=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=3.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=3.0, tend=4.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=4.0, tend=4.0, value=20.0),
        ]

        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_duplicate_timestamps(self, spark: SparkSession):
        """Test that exact duplicate rows (same timestamp and value) are removed.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 0.0       | 10.0  |
            | c1           | ch1        | 1.0       | 10.0  |
            | c1           | ch1        | 1.0       | 10.0  |  <-- duplicate
            | c1           | ch1        | 2.0       | 20.0  |

        The pipeline deduplicates the row at (1.0, 10.0) whose LEAD is also
        (1.0, 10.0).  Expects 3 intervals after dedup.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=20.0),
        ]

        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = IntervalEncoder().prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=1.0, tend=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=2.0, value=20.0),
        ]

        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_negative_timestamps(self, spark: SparkSession):
        """Test point-to-interval conversion with negative timestamps.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | -2.0      | 10.0  |
            | c1           | ch1        | -1.0      | 10.0  |
            | c1           | ch1        | 0.0       | 20.0  |
            | c1           | ch1        | 1.0       | 20.0  |

        Expects 4 intervals; negative timestamps sort correctly via the
        window's ``ORDER BY timestamp ASC``.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=-2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=-1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=20.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=20.0),
        ]

        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = IntervalEncoder().prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=-2.0, tend=-1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=-1.0, tend=0.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=20.0),
            Row(container_id="c1", channel_id="ch1", tstart=1.0, tend=1.0, value=20.0),
        ]

        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_extreme_values(self, spark: SparkSession):
        """Test point-to-interval conversion with extreme float values.

        Input:
            | container_id | channel_id | timestamp | value                  |
            |--------------|------------|-----------|------------------------|
            | c1           | ch1        | 0.0       | +inf                   |
            | c1           | ch1        | 1.0       | +inf                   |
            | c1           | ch1        | 2.0       | -inf                   |
            | c1           | ch1        | 3.0       | 1.7976931348623157e308 |

        Expects 4 intervals; extreme float values do not affect LEAD logic.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=float("inf")),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=float("inf")),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=float("-inf")),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=1.7976931348623157e308),
        ]

        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = IntervalEncoder().prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=float("inf")),
            Row(container_id="c1", channel_id="ch1", tstart=1.0, tend=2.0, value=float("inf")),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=3.0, value=float("-inf")),
            Row(
                container_id="c1",
                channel_id="ch1",
                tstart=3.0,
                tend=3.0,
                value=1.7976931348623157e308,
            ),
        ]

        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_nan_values(self, spark: SparkSession):
        """Test point-to-interval conversion with NaN values.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 0.0       | 10.0  |
            | c1           | ch1        | 1.0       | NaN   |
            | c1           | ch1        | 2.0       | NaN   |
            | c1           | ch1        | 3.0       | 10.0  |

        Expects 4 intervals.  NaN values have different timestamps so they
        are not flagged as duplicates.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=float("nan")),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=float("nan")),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=10.0),
        ]

        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = IntervalEncoder().prepare_channels_df(df)

        assert result.count() == 4

    def test_prepare_channels_df_all_null_values(self, spark: SparkSession):
        """Test point-to-interval conversion when all values are null.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 0.0       | None  |
            | c1           | ch1        | 1.0       | None  |
            | c1           | ch1        | 2.0       | None  |

        Expects 3 intervals.  All values are null-safe-equal but timestamps
        differ, so no rows are deduplicated.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=None),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=None),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=None),
        ]

        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = IntervalEncoder().prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=None),
            Row(container_id="c1", channel_id="ch1", tstart=1.0, tend=2.0, value=None),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=2.0, value=None),
        ]

        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_prepare_channels_df_zero_values(self, spark: SparkSession):
        """Test point-to-interval conversion with zero and negative-zero values.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 0.0       | 0.0   |
            | c1           | ch1        | 1.0       | 0.0   |
            | c1           | ch1        | 2.0       | -0.0  |
            | c1           | ch1        | 3.0       | 1.0   |

        Expects 4 intervals.  ``-0.0`` is treated as equal to ``0.0`` by
        IEEE 754 and Spark.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=0.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=0.0),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=-0.0),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=1.0),
        ]

        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = IntervalEncoder().prepare_channels_df(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", tstart=0.0, tend=1.0, value=0.0),
            Row(container_id="c1", channel_id="ch1", tstart=1.0, tend=2.0, value=0.0),
            Row(container_id="c1", channel_id="ch1", tstart=2.0, tend=3.0, value=0.0),
            Row(container_id="c1", channel_id="ch1", tstart=3.0, tend=3.0, value=1.0),
        ]

        expected_result = spark.createDataFrame(expected_result_data, silver_rle_encoded_schema)
        assertDataFrameEqual(result, expected_result, ignoreColumnOrder=True)

    def test_remove_implausible_data_points(self, spark: SparkSession):
        """Test that rows with is_plausible=False or None are filtered out.

        Input:
            | container_id | channel_id | timestamp | value   | is_plausible |
            |--------------|------------|-----------|---------|--------------|
            | c1           | ch1        | 0.0       | 0.0     | True         |
            | c1           | ch1        | 1.0       | 1000.0  | False        |
            | c1           | ch1        | 2.0       | 0.0     | None         |

        With filtering enabled, expects only the first row.
        With filtering disabled, expects all 3 rows unchanged.
        """
        is_plausible_schema = T.StructType(
            [
                T.StructField("container_id", T.StringType(), True),
                T.StructField("channel_id", T.StringType(), True),
                T.StructField("timestamp", T.DoubleType(), True),
                T.StructField("value", T.DoubleType(), True),
                T.StructField("is_plausible", T.BooleanType(), True),
            ]
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
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                value=0.0,
                is_plausible=None,
            ),
        ]
        df = spark.createDataFrame(data, is_plausible_schema)
        result = IntervalEncoder(
            drop_implausible_data_points=True
        )._remove_implausible_data_points(df)

        expected_result_data = [
            Row(container_id="c1", channel_id="ch1", timestamp=0.0, value=0.0, is_plausible=True)
        ]
        expected_result = spark.createDataFrame(expected_result_data, is_plausible_schema)
        assertDataFrameEqual(result, expected_result)

        result_no_filter = IntervalEncoder(
            drop_implausible_data_points=False
        )._remove_implausible_data_points(df)

        assertDataFrameEqual(result_no_filter, df)

    def test_remove_implausible_data_points_missing_is_plausible(self, spark: SparkSession):
        """Test that a ValueError is raised when is_plausible column is missing.

        Input:
            (empty DataFrame with columns: container_id, channel_id, value)

        With filtering enabled, expects ValueError.
        With filtering disabled, expects an empty result.
        """
        missing_is_plausible_schema = T.StructType(
            [
                T.StructField("container_id", T.StringType(), True),
                T.StructField("channel_id", T.StringType(), True),
                T.StructField("value", T.DoubleType(), True),
            ]
        )
        df = spark.createDataFrame([], missing_is_plausible_schema)

        with pytest.raises(
            ValueError,
            match="DataFrame must contain an 'is_plausible' column to drop implausible data points.",
        ):
            IntervalEncoder(drop_implausible_data_points=True)._remove_implausible_data_points(df)

        result = IntervalEncoder(
            drop_implausible_data_points=False
        )._remove_implausible_data_points(df)
        assert result.isEmpty()

    # Tests for _extract_next_data_point_info method
    def test_extract_next_data_point_info_single_partition(self, spark: SparkSession):
        """Test _extract_next_data_point_info adds correct next data point columns.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 1.0       | 10.0  |
            | c1           | ch1        | 2.0       | 20.0  |
            | c1           | ch1        | 3.0       | 30.0  |

        Expected:
            Adds _timestamp_of_next_data_point and _value_of_next_data_point columns.
            The last row should have NULLs for these columns.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=20.0),
            Row(container_id="c1", channel_id="ch1", timestamp=3.0, value=30.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = IntervalEncoder()._extract_next_data_point_info(df)

        expected_schema = T.StructType(
            silver_schema_without_rle.fields
            + [
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
                T.StructField("_value_of_next_data_point", T.DoubleType(), True),
            ]
        )

        expected_data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=10.0,
                _timestamp_of_next_data_point=2.0,
                _value_of_next_data_point=20.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                value=20.0,
                _timestamp_of_next_data_point=3.0,
                _value_of_next_data_point=30.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=3.0,
                value=30.0,
                _timestamp_of_next_data_point=None,
                _value_of_next_data_point=None,
            ),
        ]
        expected_result = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result, expected_result)

    def test_extract_next_data_point_info_multiple_partitions(self, spark: SparkSession):
        """Test _extract_next_data_point_info with multiple partitions.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 1.0       | 10.0  |
            | c1           | ch1        | 2.0       | 20.0  |
            | c1           | ch2        | 1.0       | 100.0 |
            | c2           | ch1        | 1.0       | 200.0 |

        Expected:
            Each (container_id, channel_id) partition is processed independently.
            Last row in each partition should have NULL for next data point columns.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=20.0),
            Row(container_id="c1", channel_id="ch2", timestamp=1.0, value=100.0),
            Row(container_id="c2", channel_id="ch1", timestamp=1.0, value=200.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = IntervalEncoder()._extract_next_data_point_info(df)

        expected_schema = T.StructType(
            silver_schema_without_rle.fields
            + [
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
                T.StructField("_value_of_next_data_point", T.DoubleType(), True),
            ]
        )

        expected_data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=10.0,
                _timestamp_of_next_data_point=2.0,
                _value_of_next_data_point=20.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                value=20.0,
                _timestamp_of_next_data_point=None,
                _value_of_next_data_point=None,
            ),
            Row(
                container_id="c1",
                channel_id="ch2",
                timestamp=1.0,
                value=100.0,
                _timestamp_of_next_data_point=None,
                _value_of_next_data_point=None,
            ),
            Row(
                container_id="c2",
                channel_id="ch1",
                timestamp=1.0,
                value=200.0,
                _timestamp_of_next_data_point=None,
                _value_of_next_data_point=None,
            ),
        ]
        expected_result = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result, expected_result, checkRowOrder=False)

    def test_extract_next_data_point_info_with_ordering(self, spark: SparkSession):
        """Test _extract_next_data_point_info respects timestamp ASC, value DESC ordering.

        Input (deliberately unsorted):
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 2.0       | 10.0  |
            | c1           | ch1        | 1.0       | 30.0  |
            | c1           | ch1        | 1.0       | 20.0  |

        Expected:
            Sorted by timestamp ASC, then value DESC.
            Window function should produce correct LEAD values.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=10.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=30.0),
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=20.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = IntervalEncoder()._extract_next_data_point_info(df)

        expected_schema = T.StructType(
            silver_schema_without_rle.fields
            + [
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
                T.StructField("_value_of_next_data_point", T.DoubleType(), True),
            ]
        )

        expected_data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=30.0,
                _timestamp_of_next_data_point=1.0,
                _value_of_next_data_point=20.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=20.0,
                _timestamp_of_next_data_point=2.0,
                _value_of_next_data_point=10.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                value=10.0,
                _timestamp_of_next_data_point=None,
                _value_of_next_data_point=None,
            ),
        ]
        expected_result = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result, expected_result, checkRowOrder=False)

    def test_extract_next_data_point_info_empty_dataframe(self, spark: SparkSession):
        """Test _extract_next_data_point_info with empty DataFrame.

        Input:
            Empty DataFrame with silver_schema_without_rle

        Expected:
            Empty DataFrame with additional columns for next data point info.
        """
        df = spark.createDataFrame([], silver_schema_without_rle)
        result = IntervalEncoder()._extract_next_data_point_info(df)

        expected_schema = T.StructType(
            silver_schema_without_rle.fields
            + [
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
                T.StructField("_value_of_next_data_point", T.DoubleType(), True),
            ]
        )

        expected_result = spark.createDataFrame([], expected_schema)
        assertDataFrameEqual(result, expected_result)

    def test_extract_next_data_point_info_with_nulls(self, spark: SparkSession):
        """Test _extract_next_data_point_info with NULL values in timestamp and value.

        Input:
            | container_id | channel_id | timestamp | value |
            |--------------|------------|-----------|-------|
            | c1           | ch1        | 1.0       | NULL  |
            | c1           | ch1        | 2.0       | 20.0  |
            | c1           | ch1        | NULL      | 30.0  |

        Expected:
            NULL values should be properly handled by LEAD function.
        """
        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=None),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=20.0),
            Row(container_id="c1", channel_id="ch1", timestamp=None, value=30.0),
        ]
        df = spark.createDataFrame(data, silver_schema_without_rle)
        result = IntervalEncoder()._extract_next_data_point_info(df)

        expected_schema = T.StructType(
            silver_schema_without_rle.fields
            + [
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
                T.StructField("_value_of_next_data_point", T.DoubleType(), True),
            ]
        )

        expected_data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=None,
                _timestamp_of_next_data_point=2.0,
                _value_of_next_data_point=20.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                value=20.0,
                _timestamp_of_next_data_point=None,
                _value_of_next_data_point=None,
            ),
        ]
        expected_result = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result, expected_result, checkRowOrder=False)

    # Tests for _drop_duplicate_data_points method
    def test_drop_duplicate_data_points_basic(self, spark: SparkSession):
        """Test _drop_duplicate_data_points removes exact duplicates.

        Input (with next data point info already added):
            | container_id | channel_id | timestamp | value | _timestamp_of_next_data_point | _value_of_next_data_point |
            |--------------|------------|-----------|-------|------------------------------|---------------------------|
            | c1           | ch1        | 1.0       | 10.0  | 1.0                          | 10.0                      |
            | c1           | ch1        | 1.0       | 10.0  | 2.0                          | 20.0                      |
            | c1           | ch1        | 2.0       | 20.0  | None                         | None                      |

        Expected:
            First row is removed as it's an exact duplicate of the next row.
        """
        schema_with_next_info = T.StructType(
            silver_schema_without_rle.fields
            + [
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
                T.StructField("_value_of_next_data_point", T.DoubleType(), True),
            ]
        )

        data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=10.0,
                _timestamp_of_next_data_point=1.0,
                _value_of_next_data_point=10.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=10.0,
                _timestamp_of_next_data_point=2.0,
                _value_of_next_data_point=20.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                value=20.0,
                _timestamp_of_next_data_point=None,
                _value_of_next_data_point=None,
            ),
        ]
        df = spark.createDataFrame(data, schema_with_next_info)
        result = IntervalEncoder()._drop_duplicate_data_points(df)

        expected_schema = T.StructType(
            [
                T.StructField("container_id", T.StringType(), True),
                T.StructField("channel_id", T.StringType(), True),
                T.StructField("timestamp", T.DoubleType(), True),
                T.StructField("value", T.DoubleType(), True),
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
            ]
        )

        expected_data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=10.0,
                _timestamp_of_next_data_point=2.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                value=20.0,
                _timestamp_of_next_data_point=None,
            ),
        ]
        expected_result = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result, expected_result)

    def test_drop_duplicate_data_points_no_duplicates(self, spark: SparkSession):
        """Test _drop_duplicate_data_points when there are no exact duplicates.

        Input:
            | container_id | channel_id | timestamp | value | _timestamp_of_next_data_point | _value_of_next_data_point |
            |--------------|------------|-----------|-------|------------------------------|---------------------------|
            | c1           | ch1        | 1.0       | 10.0  | 2.0                          | 20.0                      |
            | c1           | ch1        | 2.0       | 20.0  | 3.0                          | 30.0                      |
            | c1           | ch1        | 3.0       | 30.0  | None                         | None                      |

        Expected:
            All rows retained as there are no exact duplicates.
        """
        schema_with_next_info = T.StructType(
            silver_schema_without_rle.fields
            + [
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
                T.StructField("_value_of_next_data_point", T.DoubleType(), True),
            ]
        )

        data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=10.0,
                _timestamp_of_next_data_point=2.0,
                _value_of_next_data_point=20.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                value=20.0,
                _timestamp_of_next_data_point=3.0,
                _value_of_next_data_point=30.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=3.0,
                value=30.0,
                _timestamp_of_next_data_point=None,
                _value_of_next_data_point=None,
            ),
        ]
        df = spark.createDataFrame(data, schema_with_next_info)
        result = IntervalEncoder()._drop_duplicate_data_points(df)

        expected_schema = T.StructType(
            [
                T.StructField("container_id", T.StringType(), True),
                T.StructField("channel_id", T.StringType(), True),
                T.StructField("timestamp", T.DoubleType(), True),
                T.StructField("value", T.DoubleType(), True),
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
            ]
        )

        expected_data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=10.0,
                _timestamp_of_next_data_point=2.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                value=20.0,
                _timestamp_of_next_data_point=3.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=3.0,
                value=30.0,
                _timestamp_of_next_data_point=None,
            ),
        ]
        expected_result = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result, expected_result)

    def test_drop_duplicate_data_points_with_nulls(self, spark: SparkSession):
        """Test _drop_duplicate_data_points with NULL values using eqNullSafe.

        Input:
            | container_id | channel_id | timestamp | value | _timestamp_of_next_data_point | _value_of_next_data_point |
            |--------------|------------|-----------|-------|------------------------------|---------------------------|
            | c1           | ch1        | 1.0       | NULL  | 1.0                          | NULL                      |
            | c1           | ch1        | 1.0       | NULL  | 2.0                          | 20.0                      |
            | c1           | ch1        | 2.0       | 20.0  | None                         | None                      |

        Expected:
            First row is removed as it's a duplicate (both timestamp and value are NULL-safe equal).
        """
        schema_with_next_info = T.StructType(
            silver_schema_without_rle.fields
            + [
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
                T.StructField("_value_of_next_data_point", T.DoubleType(), True),
            ]
        )

        data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=None,
                _timestamp_of_next_data_point=1.0,
                _value_of_next_data_point=None,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=None,
                _timestamp_of_next_data_point=2.0,
                _value_of_next_data_point=20.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                value=20.0,
                _timestamp_of_next_data_point=None,
                _value_of_next_data_point=None,
            ),
        ]
        df = spark.createDataFrame(data, schema_with_next_info)
        result = IntervalEncoder()._drop_duplicate_data_points(df)

        expected_schema = T.StructType(
            [
                T.StructField("container_id", T.StringType(), True),
                T.StructField("channel_id", T.StringType(), True),
                T.StructField("timestamp", T.DoubleType(), True),
                T.StructField("value", T.DoubleType(), True),
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
            ]
        )

        expected_data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=None,
                _timestamp_of_next_data_point=2.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                value=20.0,
                _timestamp_of_next_data_point=None,
            ),
        ]
        expected_result = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result, expected_result)

    def test_drop_duplicate_data_points_partial_duplicates(self, spark: SparkSession):
        """Test _drop_duplicate_data_points with partial duplicates (only timestamp or value matches).

        Input:
            | container_id | channel_id | timestamp | value | _timestamp_of_next_data_point | _value_of_next_data_point |
            |--------------|------------|-----------|-------|------------------------------|---------------------------|
            | c1           | ch1        | 1.0       | 10.0  | 1.0                          | 20.0                      |
            | c1           | ch1        | 1.0       | 20.0  | 2.0                          | 10.0                      |
            | c1           | ch1        | 2.0       | 10.0  | None                         | None                      |

        Expected:
            All rows retained as none are exact duplicates (both timestamp AND value must match).
        """
        schema_with_next_info = T.StructType(
            silver_schema_without_rle.fields
            + [
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
                T.StructField("_value_of_next_data_point", T.DoubleType(), True),
            ]
        )

        data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=10.0,
                _timestamp_of_next_data_point=1.0,
                _value_of_next_data_point=20.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=20.0,
                _timestamp_of_next_data_point=2.0,
                _value_of_next_data_point=10.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                value=10.0,
                _timestamp_of_next_data_point=None,
                _value_of_next_data_point=None,
            ),
        ]
        df = spark.createDataFrame(data, schema_with_next_info)
        result = IntervalEncoder()._drop_duplicate_data_points(df)

        expected_schema = T.StructType(
            [
                T.StructField("container_id", T.StringType(), True),
                T.StructField("channel_id", T.StringType(), True),
                T.StructField("timestamp", T.DoubleType(), True),
                T.StructField("value", T.DoubleType(), True),
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
            ]
        )

        expected_data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=10.0,
                _timestamp_of_next_data_point=1.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=20.0,
                _timestamp_of_next_data_point=2.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=2.0,
                value=10.0,
                _timestamp_of_next_data_point=None,
            ),
        ]
        expected_result = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result, expected_result)

    def test_drop_duplicate_data_points_empty_dataframe(self, spark: SparkSession):
        """Test _drop_duplicate_data_points with empty DataFrame.

        Input:
            Empty DataFrame with schema including next data point columns.

        Expected:
            Empty DataFrame with _value_of_next_data_point column removed.
        """
        schema_with_next_info = T.StructType(
            silver_schema_without_rle.fields
            + [
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
                T.StructField("_value_of_next_data_point", T.DoubleType(), True),
            ]
        )

        df = spark.createDataFrame([], schema_with_next_info)
        result = IntervalEncoder()._drop_duplicate_data_points(df)

        expected_schema = T.StructType(
            [
                T.StructField("container_id", T.StringType(), True),
                T.StructField("channel_id", T.StringType(), True),
                T.StructField("timestamp", T.DoubleType(), True),
                T.StructField("value", T.DoubleType(), True),
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
            ]
        )

        expected_result = spark.createDataFrame([], expected_schema)
        assertDataFrameEqual(result, expected_result)

    def test_drop_duplicate_data_points_multiple_partitions(self, spark: SparkSession):
        """Test _drop_duplicate_data_points across multiple partitions.

        Input:
            | container_id | channel_id | timestamp | value | _timestamp_of_next_data_point | _value_of_next_data_point |
            |--------------|------------|-----------|-------|------------------------------|---------------------------|
            | c1           | ch1        | 1.0       | 10.0  | 1.0                          | 10.0                      |
            | c1           | ch1        | 1.0       | 10.0  | None                         | None                      |
            | c1           | ch2        | 1.0       | 20.0  | 1.0                          | 20.0                      |
            | c1           | ch2        | 1.0       | 20.0  | None                         | None                      |

        Expected:
            First row of each partition is removed as it's a duplicate of the next row.
        """
        schema_with_next_info = T.StructType(
            silver_schema_without_rle.fields
            + [
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
                T.StructField("_value_of_next_data_point", T.DoubleType(), True),
            ]
        )

        data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=10.0,
                _timestamp_of_next_data_point=1.0,
                _value_of_next_data_point=10.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=10.0,
                _timestamp_of_next_data_point=None,
                _value_of_next_data_point=None,
            ),
            Row(
                container_id="c1",
                channel_id="ch2",
                timestamp=1.0,
                value=20.0,
                _timestamp_of_next_data_point=1.0,
                _value_of_next_data_point=20.0,
            ),
            Row(
                container_id="c1",
                channel_id="ch2",
                timestamp=1.0,
                value=20.0,
                _timestamp_of_next_data_point=None,
                _value_of_next_data_point=None,
            ),
        ]
        df = spark.createDataFrame(data, schema_with_next_info)
        result = IntervalEncoder()._drop_duplicate_data_points(df)

        expected_schema = T.StructType(
            [
                T.StructField("container_id", T.StringType(), True),
                T.StructField("channel_id", T.StringType(), True),
                T.StructField("timestamp", T.DoubleType(), True),
                T.StructField("value", T.DoubleType(), True),
                T.StructField("_timestamp_of_next_data_point", T.DoubleType(), True),
            ]
        )

        expected_data = [
            Row(
                container_id="c1",
                channel_id="ch1",
                timestamp=1.0,
                value=10.0,
                _timestamp_of_next_data_point=None,
            ),
            Row(
                container_id="c1",
                channel_id="ch2",
                timestamp=1.0,
                value=20.0,
                _timestamp_of_next_data_point=None,
            ),
        ]
        expected_result = spark.createDataFrame(expected_data, expected_schema)
        assertDataFrameEqual(result, expected_result, checkRowOrder=False)

    # Additional tests for _remove_implausible_data_points method
    def test_remove_implausible_data_points_all_true(self, spark: SparkSession):
        """Test _remove_implausible_data_points when all values are True.

        Input:
            | container_id | channel_id | timestamp | value | is_plausible |
            |--------------|------------|-----------|-------|--------------|
            | c1           | ch1        | 1.0       | 10.0  | True         |
            | c1           | ch1        | 2.0       | 20.0  | True         |

        Expected:
            All rows retained when filtering is enabled.
        """
        is_plausible_schema = T.StructType(
            [
                T.StructField("container_id", T.StringType(), True),
                T.StructField("channel_id", T.StringType(), True),
                T.StructField("timestamp", T.DoubleType(), True),
                T.StructField("value", T.DoubleType(), True),
                T.StructField("is_plausible", T.BooleanType(), True),
            ]
        )

        data = [
            Row(container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0, is_plausible=True),
            Row(container_id="c1", channel_id="ch1", timestamp=2.0, value=20.0, is_plausible=True),
        ]
        df = spark.createDataFrame(data, is_plausible_schema)

        result = IntervalEncoder(
            drop_implausible_data_points=True
        )._remove_implausible_data_points(df)
        assertDataFrameEqual(result, df)

    def test_remove_implausible_data_points_all_false(self, spark: SparkSession):
        """Test _remove_implausible_data_points when all values are False.

        Input:
            | container_id | channel_id | timestamp | value | is_plausible |
            |--------------|------------|-----------|-------|--------------|
            | c1           | ch1        | 1.0       | 10.0  | False        |
            | c1           | ch1        | 2.0       | 20.0  | False        |

        Expected:
            All rows removed when filtering is enabled.
        """
        is_plausible_schema = T.StructType(
            [
                T.StructField("container_id", T.StringType(), True),
                T.StructField("channel_id", T.StringType(), True),
                T.StructField("timestamp", T.DoubleType(), True),
                T.StructField("value", T.DoubleType(), True),
                T.StructField("is_plausible", T.BooleanType(), True),
            ]
        )

        data = [
            Row(
                container_id="c1", channel_id="ch1", timestamp=1.0, value=10.0, is_plausible=False
            ),
            Row(
                container_id="c1", channel_id="ch1", timestamp=2.0, value=20.0, is_plausible=False
            ),
        ]
        df = spark.createDataFrame(data, is_plausible_schema)

        result = IntervalEncoder(
            drop_implausible_data_points=True
        )._remove_implausible_data_points(df)
        expected_result = spark.createDataFrame([], is_plausible_schema)
        assertDataFrameEqual(result, expected_result)

    def test_remove_implausible_data_points_empty_dataframe(self, spark: SparkSession):
        """Test _remove_implausible_data_points with empty DataFrame.

        Input:
            Empty DataFrame with is_plausible column.

        Expected:
            Empty DataFrame returned regardless of filtering setting.
        """
        is_plausible_schema = T.StructType(
            [
                T.StructField("container_id", T.StringType(), True),
                T.StructField("channel_id", T.StringType(), True),
                T.StructField("timestamp", T.DoubleType(), True),
                T.StructField("value", T.DoubleType(), True),
                T.StructField("is_plausible", T.BooleanType(), True),
            ]
        )

        df = spark.createDataFrame([], is_plausible_schema)

        result_with_filter = IntervalEncoder(
            drop_implausible_data_points=True
        )._remove_implausible_data_points(df)
        result_without_filter = IntervalEncoder(
            drop_implausible_data_points=False
        )._remove_implausible_data_points(df)

        assertDataFrameEqual(result_with_filter, df)
        assertDataFrameEqual(result_without_filter, df)
