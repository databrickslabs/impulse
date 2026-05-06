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
from tests.conftest import key_value_store_db, spark


def _default_cfg(project_id: str = "SAMPLE_PROJECT", **table_overrides) -> SolverConfig:
    """Build a SolverConfig with the standard element_id→key rename for the EAV table."""
    container_tags = table_overrides.pop(
        "container_tags",
        TableConfig(column_name_mapping={"element_id": "key"}),
    )
    return SolverConfig(project_id=project_id, container_tags=container_tags, **table_overrides)


class TestKeyValueStoreSolverFilterContainerTags:
    """Tests for KeyValueStoreSolver.filter_container_tags."""

    def test_no_filter_returns_all_containers(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """When no TagExpression filter is applied, all entity_ids are returned."""
        solver = KeyValueStoreSolver(spark, config=_default_cfg())
        query = key_value_store_db.query
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}

    def test_with_single_tag_filter(self, spark: SparkSession, key_value_store_db: MeasurementDB):
        """A single TagExpression filter should return matching containers."""
        solver = KeyValueStoreSolver(spark, config=_default_cfg())
        query = key_value_store_db.query
        query.where(TagSelector("brand") == "Seat")
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}

    def test_with_non_matching_tag_filter(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """A filter that matches no rows should return an empty result."""
        solver = KeyValueStoreSolver(spark, config=_default_cfg())
        query = key_value_store_db.query
        query.where(TagSelector("brand") == "NonExistentBrand")
        result = solver.filter_container_tags(spark, query)
        assert result.count() == 0

    def test_with_and_combined_tag_filters(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """AND-combined TagExpression filters should narrow results correctly."""
        solver = KeyValueStoreSolver(spark, config=_default_cfg())
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
        solver = KeyValueStoreSolver(spark, config=_default_cfg())
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
        solver = KeyValueStoreSolver(spark, config=_default_cfg("NON_EXISTENT_PROJECT"))
        query = key_value_store_db.query
        result = solver.filter_container_tags(spark, query)
        assert result.count() == 0

    def test_with_matching_parent_id(self, spark: SparkSession, key_value_store_db: MeasurementDB):
        """When parent_id matches, all matching containers are returned."""
        solver = KeyValueStoreSolver(
            spark,
            config=_default_cfg(
                container_tags=TableConfig(
                    column_name_mapping={"element_id": "key"},
                    filters={"parent_id": "container_concept"},
                ),
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
            config=_default_cfg(
                container_tags=TableConfig(
                    column_name_mapping={"element_id": "key"},
                    filters={"parent_id": "non_existent_parent"},
                ),
            ),
        )
        query = key_value_store_db.query
        result = solver.filter_container_tags(spark, query)
        assert result.count() == 0

    def test_no_parent_id_skips_filter(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """When no parent_id filter is configured, all containers are returned."""
        cfg = _default_cfg()
        solver = KeyValueStoreSolver(spark, config=cfg)
        assert "parent_id" not in cfg.container_tags.filters
        query = key_value_store_db.query
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}

    def test_pivot_creates_correct_columns(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """Pivot should produce columns matching the required element_ids."""
        query = key_value_store_db.query
        brand_filter = TagSelector("brand") == "Seat"
        model_filter = TagSelector("model") == "Leon"
        query.where(brand_filter & model_filter)

        tags = query.db.container_tags(spark)
        tags = tags.where(tags.project_id == "SAMPLE_PROJECT")
        tags = tags.where(tags.element_id.isin(["brand", "model"]))
        tags = (
            tags.groupBy("container_id")
            .pivot("element_id", ["brand", "model"])
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
        solver = KeyValueStoreSolver(spark, config=_default_cfg())
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
        solver = KeyValueStoreSolver(spark, config=_default_cfg())
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
        solver = KeyValueStoreSolver(spark, config=_default_cfg())
        query = key_value_store_db.query
        empty_filter = TagSelector("") == "some_value"
        query.where(empty_filter)
        result = solver.filter_container_tags(spark, query)
        assert result.count() == 0

    def test_empty_value_selector(self, spark: SparkSession, key_value_store_db: MeasurementDB):
        """Filtering for an empty string value should not crash."""
        solver = KeyValueStoreSolver(spark, config=_default_cfg())
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
        solver = KeyValueStoreSolver(spark, config=_default_cfg())
        query = key_value_store_db.query
        query.where(TagSelector("vehicle_key") == "Seat_Leon")
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}

    def test_multiple_separate_where_calls(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """Multiple where() calls should accumulate filters."""
        solver = KeyValueStoreSolver(spark, config=_default_cfg())
        query = key_value_store_db.query
        query.where(TagSelector("brand") == "Seat")
        query.where(TagSelector("model") == "Leon")
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}


class TestSolverConfig:
    """Tests for SolverConfig creation and property access."""

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


class TestKeyValueStoreSolverConfig:
    """Tests for configuration handling in KeyValueStoreSolver."""

    def test_default_config_used_when_none(self, spark: SparkSession):
        """A SolverConfig with only project_id and tag-key rename should construct cleanly."""
        solver = KeyValueStoreSolver(spark, config=_default_cfg())
        assert solver.config.container_id_col == "container_id"
        assert solver.config.project_id_col == "project_id"
        # Verify no redundant instance attributes (PR rework item #2)
        assert not hasattr(solver, "cid_col")
        assert not hasattr(solver, "ch_col")
        assert not hasattr(solver, "ts_col")
        assert not hasattr(solver, "te_col")
        assert not hasattr(solver, "val_col")

    def test_custom_config_filter_container_tags(
        self, spark: SparkSession, key_value_store_db: MeasurementDB
    ):
        """A SolverConfig with explicit (no-op) mapping should behave like the default."""
        # The test data already uses 'container_id' / 'project_id' — the mapping is a no-op.
        cfg = SolverConfig(
            project_id="SAMPLE_PROJECT",
            container_tags=TableConfig(
                column_name_mapping={
                    "container_id": "container_id",
                    "project_id": "project_id",
                    "element_id": "key",
                },
            ),
        )
        solver = KeyValueStoreSolver(spark, config=cfg)
        query = key_value_store_db.query
        result = solver.filter_container_tags(spark, query)
        container_ids = {row.container_id for row in result.collect()}
        assert container_ids == {1, 2, 3}
