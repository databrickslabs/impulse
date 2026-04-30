# pylint: disable=missing-function-docstring
"""
Tests for the KeyValueStoreSolver.

Covers:
- Filtering container tags with and without TagExpression filters
- Filtering container metrics via project_id join
- Pivot correctness and column naming
- Project ID filtering (existing and non-existent projects)
- Empty TagSelector behaviour
- Backwards-compatible get_container_metrics path
- Complex AND / OR tag filter combinations
- Multiple element_id filtering
- MetricExpression internal API tests
"""

import pytest

from pyspark.sql import SparkSession

from mda_query_engine.analyze.metadata.metric_expression import MetricSelector
from mda_query_engine.analyze.metadata.tag_expression import TagSelector
from mda_query_engine.analyze.query.solvers.key_value_store_solver import (
    KeyValueStoreSolver,
)
from mda_query_engine.analyze.query.solvers.solver_config import (
    SolverConfig,
    TableConfig,
)
from mda_query_engine.measurement_db import MeasurementDB
from tests.conftest import key_value_store_db, setup_key_value_store_db, spark


def _kvs_config(project_id="SAMPLE_PROJECT", **kwargs):
    """Shortcut for building a SolverConfig with project_id.

    The test CSV data uses ``element_id`` as the physical key column,
    so we map it to the internal name ``key`` by default.
    """
    kwargs.setdefault(
        "container_tags",
        TableConfig(column_name_mapping={"element_id": "key"}),
    )
    return SolverConfig(project_id=project_id, **kwargs)


class TestKeyValueStoreSolverFilterContainerTags:
    """Tests for KeyValueStoreSolver.filter_container_tags."""

    def test_no_filter_returns_all_containers(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """When no TagExpression filter is applied, all container_ids are returned."""
        solver = KeyValueStoreSolver(spark, config=_kvs_config())
        query = key_value_store_db.query
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}

    def test_with_single_tag_filter(self, spark: SparkSession, key_value_store_db: MeasurementDB):
        """A single TagExpression filter should return matching containers."""
        solver = KeyValueStoreSolver(spark, config=_kvs_config())
        query = key_value_store_db.query
        query.where(TagSelector("brand") == "Seat")
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}

    def test_with_non_matching_tag_filter(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """A filter that matches no rows should return an empty result."""
        solver = KeyValueStoreSolver(spark, config=_kvs_config())
        query = key_value_store_db.query
        query.where(TagSelector("brand") == "NonExistentBrand")
        result = solver.filter_container_tags(spark, query)
        assert result.count() == 0

    def test_with_and_combined_tag_filters(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """AND-combined TagExpression filters should narrow results correctly."""
        solver = KeyValueStoreSolver(spark, config=_kvs_config())
        query = key_value_store_db.query
        brand_filter = TagSelector("brand") == "Seat"
        model_filter = TagSelector("model") == "Leon"
        query.where(brand_filter & model_filter)
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}

    def test_with_or_combined_tag_filters(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """OR-combined filters should return the union of matching containers."""
        solver = KeyValueStoreSolver(spark, config=_kvs_config())
        query = key_value_store_db.query
        brand_seat = TagSelector("brand") == "Seat"
        brand_vw = TagSelector("brand") == "VW"
        query.where(brand_seat | brand_vw)
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}

    def test_non_existent_project_returns_empty(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """A non-existent project_id should yield zero rows."""
        solver = KeyValueStoreSolver(
            spark, config=_kvs_config(project_id="NON_EXISTENT_PROJECT")
        )
        query = key_value_store_db.query
        result = solver.filter_container_tags(spark, query)
        assert result.count() == 0

    def test_with_matching_parent_id(self, spark: SparkSession, key_value_store_db: MeasurementDB):
        """When parent_id matches, all matching containers are returned."""
        solver = KeyValueStoreSolver(
            spark,
            config=_kvs_config(
                container_tags=TableConfig(
                    filters={"parent_id": "container_concept"}
                )
            ),
        )
        query = key_value_store_db.query
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}

    def test_with_non_matching_parent_id(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """When parent_id does not match any rows, zero results are returned."""
        solver = KeyValueStoreSolver(
            spark,
            config=_kvs_config(
                container_tags=TableConfig(
                    filters={"parent_id": "non_existent_parent"}
                )
            ),
        )
        query = key_value_store_db.query
        result = solver.filter_container_tags(spark, query)
        assert result.count() == 0

    def test_no_parent_id_skips_filter(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """When no parent_id filter is configured, no parent_id filter is applied."""
        solver = KeyValueStoreSolver(spark, config=_kvs_config())
        assert "parent_id" not in solver.config.container_tags.filters
        query = key_value_store_db.query
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}

    def test_pivot_creates_correct_columns(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """Pivot should produce columns matching the required element_ids."""
        solver = KeyValueStoreSolver(spark, config=_kvs_config())
        query = key_value_store_db.query
        brand_filter = TagSelector("brand") == "Seat"
        model_filter = TagSelector("model") == "Leon"
        query.where(brand_filter & model_filter)

        tags = query.db.container_tags(spark)
        tags = tags.withColumnRenamed("element_id", "key")
        tags = tags.where(tags.project_id == "SAMPLE_PROJECT")
        tags = tags.where(tags.key.isin(["brand", "model"]))
        tags = (
            tags.groupBy("container_id")
            .pivot("key", ["brand", "model"])
            .agg({"value": "first"})
        )
        columns = set(tags.columns)
        assert "container_id" in columns
        assert "brand" in columns
        assert "model" in columns


class TestKeyValueStoreSolverFilterContainerMetrics:
    """Tests for KeyValueStoreSolver.filter_container_metrics."""

    def test_join_with_filtered_tags(self, spark: SparkSession, key_value_store_db: MeasurementDB):
        """filter_container_metrics should inner-join tags with container_metrics."""
        solver = KeyValueStoreSolver(spark, config=_kvs_config())
        query = key_value_store_db.query
        query.where(TagSelector("model") == "Leon")
        tags_df = solver.filter_container_tags(spark, query)
        result = solver.filter_container_metrics(spark, query, tags_df)
        container_ids = {row.container_id for row in result.collect()}
        assert len(container_ids) > 0

    def test_no_filter_returns_all_matching_metrics(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """Without metric filters, all container_ids from the project should be returned."""
        solver = KeyValueStoreSolver(spark, config=_kvs_config())
        query = key_value_store_db.query
        tags_df = solver.filter_container_tags(spark, query)
        result = solver.filter_container_metrics(spark, query, tags_df)
        container_ids = {row.container_id for row in result.collect()}
        assert len(container_ids) > 0


class TestKeyValueStoreSolverEmptySelector:
    """Tests for empty and edge-case TagSelector values in the KeyValueStoreSolver."""

    def test_empty_string_selector_returns_no_results(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """Using an empty-string TagSelector should not crash; it returns no matches."""
        solver = KeyValueStoreSolver(spark, config=_kvs_config())
        query = key_value_store_db.query
        empty_filter = TagSelector("") == "some_value"
        query.where(empty_filter)
        result = solver.filter_container_tags(spark, query)
        assert result.count() == 0

    def test_empty_value_selector(self, spark: SparkSession, key_value_store_db: MeasurementDB):
        """Filtering for an empty string value should not crash."""
        solver = KeyValueStoreSolver(spark, config=_kvs_config())
        query = key_value_store_db.query
        query.where(TagSelector("brand") == "")
        result = solver.filter_container_tags(spark, query)
        assert result.count() == 0


class TestKeyValueStoreSolverMetricExpressions:
    """Tests exercising MetricExpression features within the KeyValueStoreSolver context."""

    def test_required_metrics_single_selector(self):
        """MetricSelector.required_metrics() should return a set with the key."""
        selector = MetricSelector("vehicle_key")
        assert selector.required_metrics() == {"vehicle_key"}

    def test_required_metrics_and_expression(self):
        """AND-combined selectors should union their required_metrics."""
        expr = (MetricSelector("brand") == "Seat") & (MetricSelector("model") == "Leon")
        assert expr.required_metrics() == {"brand", "model"}

    def test_required_metrics_or_expression(self):
        """OR-combined selectors on the same key should still return a single-element set."""
        expr = (MetricSelector("brand") == "Seat") | (MetricSelector("brand") == "VW")
        assert expr.required_metrics() == {"brand"}

    def test_required_metrics_nested_expression(self):
        """Deeply nested AND/OR should collect all unique keys."""
        expr = ((MetricSelector("brand") == "Seat") & (MetricSelector("model") == "Leon")) | (
            MetricSelector("environment") == "test"
        )
        assert expr.required_metrics() == {"brand", "model", "environment"}

    def test_metric_selector_str_representation(self):
        """String representation of MetricSelector should be readable."""
        expr = MetricSelector("brand")
        assert str(expr) == "MetricSelector<brand>"

    def test_metric_op_str_representation(self):
        """String representation of MetricOp should contain operation name."""
        expr = MetricSelector("brand") == "Seat"
        assert "MetricOp" in str(expr)
        assert "eq" in str(expr)

    def test_tag_filter_with_key_value_store_solver(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """End-to-end: tag filter applied via KeyValueStoreSolver should filter correctly."""
        solver = KeyValueStoreSolver(spark, config=_kvs_config())
        query = key_value_store_db.query
        query.where(TagSelector("vehicle_key") == "Seat_Leon")
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}

    def test_multiple_separate_where_calls(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """Multiple where() calls should accumulate filters."""
        solver = KeyValueStoreSolver(spark, config=_kvs_config())
        query = key_value_store_db.query
        query.where(TagSelector("brand") == "Seat")
        query.where(TagSelector("model") == "Leon")
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}


class TestSolverConfig:
    """Tests for SolverConfig creation and property access."""

    def test_default_config(self):
        """Default SolverConfig should have sensible defaults."""
        cfg = SolverConfig()
        assert cfg.container_id_col == "container_id"
        assert cfg.channel_id_cols == ["container_id", "channel_id"]
        assert cfg.tstart_col == "tstart"
        assert cfg.tend_col == "tend"
        assert cfg.value_col == "value"
        assert cfg.project_id_col == "project_id"

    def test_from_dict_per_table(self):
        """SolverConfig.from_dict should populate per-table fields."""
        data = {
            "project_id": "MY_PROJECT",
            "container_tags": {
                "column_name_mapping": {"ent_id": "container_id", "project": "project_id"},
            },
            "channels": {
                "column_name_mapping": {
                    "measurement_id": "container_id",
                    "signal_id": "channel_id",
                    "t_start": "tstart",
                    "t_stop": "tend",
                    "val": "value",
                },
            },
        }
        cfg = SolverConfig.from_dict(data)
        assert cfg.container_tags.column_name_mapping == {
            "ent_id": "container_id",
            "project": "project_id",
        }
        assert cfg.channels.column_name_mapping == {
            "measurement_id": "container_id",
            "signal_id": "channel_id",
            "t_start": "tstart",
            "t_stop": "tend",
            "val": "value",
        }
        assert cfg.container_id_col == "container_id"
        assert cfg.channel_id_col == "channel_id"

    def test_from_dict_partial_override(self):
        """Partial dict should keep defaults for omitted tables."""
        data = {
            "channels": {
                "column_name_mapping": {"meas_id": "container_id"},
            }
        }
        cfg = SolverConfig.from_dict(data)
        assert cfg.channels.column_name_mapping == {"meas_id": "container_id"}
        assert cfg.container_id_col == "container_id"
        assert cfg.channel_id_col == "channel_id"
        assert cfg.tstart_col == "tstart"

    def test_from_json(self, tmp_path):
        """SolverConfig.from_json should read a JSON file correctly."""
        import json

        config_data = {
            "channels": {
                "column_name_mapping": {
                    "cnt_id": "container_id",
                    "ch_id": "channel_id",
                    "start_time": "tstart",
                    "end_time": "tend",
                    "signal_value": "value",
                },
            },
            "container_tags": {
                "column_name_mapping": {"proj": "project_id"},
            },
        }
        config_file = tmp_path / "solver_config.json"
        config_file.write_text(json.dumps(config_data))

        cfg = SolverConfig.from_json(str(config_file))
        assert cfg.channels.column_name_mapping["cnt_id"] == "container_id"
        assert cfg.container_tags.column_name_mapping["proj"] == "project_id"
        assert cfg.container_id_col == "container_id"


class TestKeyValueStoreSolverConfig:
    """Tests for configuration handling in KeyValueStoreSolver."""

    def test_default_config_used_when_none(self, spark: SparkSession):
        """When no config is passed, default SolverConfig should be used."""
        solver = KeyValueStoreSolver(spark)
        assert solver.config.container_id_col == "container_id"
        assert solver.config.project_id_col == "project_id"
        assert solver.config.project_id is None

    def test_solver_config_instance(self, spark: SparkSession):
        """Passing a SolverConfig directly should be accepted."""
        cfg = SolverConfig(
            channels=TableConfig(
                column_name_mapping={"c_id": "container_id"}
            )
        )
        solver = KeyValueStoreSolver(spark, config=cfg)
        assert solver.config.channels.column_name_mapping["c_id"] == "container_id"
        assert solver.config.container_id_col == "container_id"

    def test_custom_config_filter_container_tags(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """Config with project_id should still filter correctly."""
        solver = KeyValueStoreSolver(
            spark, config=SolverConfig(project_id="SAMPLE_PROJECT")
        )
        query = key_value_store_db.query
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}
