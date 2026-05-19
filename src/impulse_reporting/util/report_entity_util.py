import pyspark.sql.functions as f
from pyspark.sql import Column
from pyspark.sql.types import IntegerType

from impulse_query_engine.analyze.metadata.metric_expression import MetricExpression
from impulse_query_engine.analyze.metadata.tag_expression import TagExpression
from impulse_query_engine.analyze.query.query_builder import QueryBuilder
from impulse_reporting.aggregations.aggregation import Aggregation
from impulse_reporting.config.config_parser import Comparator, MetricFilter, TagFilter
from impulse_reporting.events.event import Event


class ReportEntityUtil:
    """Utility class for report entities."""

    @staticmethod
    def get_event_id_column(
        elements: list[Aggregation] | list[Event], element_name: str
    ) -> Column:
        """
        Generates a Spark SQL expression that maps event or aggregation names to their corresponding IDs.

        Parameters
        ----------
        elements : list of Aggregation or list of Event
            List of Aggregation or Event objects to map from.
        element_name : str
            The name of the column containing event or aggregation names.

        Returns
        -------
        pyspark.sql.Column
            A Spark SQL Column expression mapping names to IDs, or None if no match is found.
        """

        name_to_id = ReportEntityUtil._get_name_to_id_mapping(elements)
        col_expr = None
        for name, event_id in name_to_id.items():
            if event_id is None:
                continue
            elif col_expr is None:
                col_expr = f.when(f.col(element_name) == f.lit(name), f.lit(event_id))
            else:
                col_expr = col_expr.when(f.col(element_name) == f.lit(name), f.lit(event_id))

        return (
            col_expr.otherwise(None) if col_expr is not None else f.lit(None).cast(IntegerType())
        )

    @staticmethod
    def _get_name_to_id_mapping(elements: list[Aggregation] | list[Event]) -> dict:
        """
        Creates a mapping from names to IDs for a given list of Aggregation or Event objects.

        Parameters
        ----------
        elements : list of Aggregation or list of Event
            List of Aggregation or Event objects to map from.

        Returns
        -------
        dict
            Dictionary mapping each object's name to its ID.
        """
        if all(isinstance(x, Aggregation) for x in elements):
            name_to_id = {}
            for hist in elements:
                if hist and hist.get_event():
                    name_to_id[hist.get_name()] = hist.get_event().get_id()

        elif all(isinstance(x, Event) for x in elements):
            name_to_id = {
                element.get_name(): element.get_id() if element else None for element in elements
            }
        else:
            raise TypeError("elements must be a list of Aggregation or Event objects")

        return name_to_id

    @staticmethod
    def _apply_comparator(expression, comparator: Comparator, value):
        """
        Apply a comparison operator to an expression.

        Parameters
        ----------
        expression : TagExpression or MetricExpression
            The left-hand side selector expression.
        comparator : Comparator
            The comparison operator to apply.
        value : str | int | float
            The right-hand side value.

        Returns
        -------
        TagOp or MetricOp
            The resulting comparison expression.
        """
        match comparator:
            case Comparator.EQ:
                return expression == value
            case Comparator.NE:
                return expression != value
            case Comparator.GT:
                return expression > value
            case Comparator.GE:
                return expression >= value
            case Comparator.LT:
                return expression < value
            case Comparator.LE:
                return expression <= value

    @staticmethod
    def generate_tag_filters(
        query: QueryBuilder, tag_filter_groups: list[list[TagFilter]]
    ) -> TagExpression | None:
        """
        Build a TagExpression filter from groups of TagFilter objects (OR of ANDs).

        Parameters
        ----------
        query : QueryBuilder
            The query builder instance used to create tag selectors.
        tag_filter_groups : list[list[TagFilter]]
            Outer list elements are OR-combined; inner list elements are AND-combined.

        Returns
        -------
        TagExpression or None
            Combined filter expression, or None if no groups are provided.
        """
        if not tag_filter_groups:
            return None

        or_expression: TagExpression | None = None

        for group in tag_filter_groups:
            and_expression: TagExpression | None = None
            for tag_filter in group:
                selector = query.tag(tag_filter.tag_name, cast_type=tag_filter.cast_type.value)
                condition = ReportEntityUtil._apply_comparator(
                    selector, tag_filter.comparator, tag_filter.value
                )
                and_expression = (
                    and_expression & condition if and_expression is not None else condition
                )
            if and_expression is not None:
                or_expression = (
                    or_expression | and_expression if or_expression is not None else and_expression
                )

        return or_expression

    @staticmethod
    def generate_metric_filters(
        query: QueryBuilder, metric_filter_groups: list[list[MetricFilter]]
    ) -> MetricExpression | None:
        """
        Build a MetricExpression filter from groups of MetricFilter objects (OR of ANDs).

        Parameters
        ----------
        query : QueryBuilder
            The query builder instance used to create metric selectors.
        metric_filter_groups : list[list[MetricFilter]]
            Outer list elements are OR-combined; inner list elements are AND-combined.

        Returns
        -------
        MetricExpression or None
            Combined filter expression, or None if no groups are provided.
        """
        if not metric_filter_groups:
            return None

        or_expression: MetricExpression | None = None

        for group in metric_filter_groups:
            and_expression: MetricExpression | None = None
            for metric_filter in group:
                selector = query.metric(metric_filter.column_name)
                condition = ReportEntityUtil._apply_comparator(
                    selector, metric_filter.comparator, metric_filter.value
                )
                and_expression = (
                    and_expression & condition if and_expression is not None else condition
                )
            if and_expression is not None:
                or_expression = (
                    or_expression | and_expression if or_expression is not None else and_expression
                )

        return or_expression
