from __future__ import annotations

import abc
import operator

import pyspark.sql.functions as F
from pyspark.sql import Column


class MetricExpression(abc.ABC):
    def __eq__(self, other):
        """
        Return a MetricOp representing equality comparison.

        Parameters
        ----------
        other : MetricExpression or scalar
            The right-hand side of the equality comparison.

        Returns
        -------
        MetricOp
            Metric operation representing equality.
        """
        return MetricOp(operator.eq, self, other)

    def __ne__(self, other):
        """
        Return a MetricOp representing inequality comparison.

        Parameters
        ----------
        other : MetricExpression or scalar
            The right-hand side of the inequality comparison.

        Returns
        -------
        MetricOp
            Metric operation representing inequality.
        """
        return MetricOp(operator.ne, self, other)

    def __gt__(self, other):
        """
        Return a MetricOp representing greater-than comparison.

        Parameters
        ----------
        other : MetricExpression or scalar
           The right-hand side of the greater-than comparison.

        Returns
        -------
        MetricOp
           Metric operation representing greater-than.
        """
        return MetricOp(operator.gt, self, other)

    def __ge__(self, other):
        """
        Return a MetricOp representing greater-than-or-equal comparison.

        Parameters
        ----------
        other : MetricExpression or scalar
            The right-hand side of the comparison.

        Returns
        -------
        MetricOp
            Metric operation representing greater-than-or-equal.
        """
        return MetricOp(operator.ge, self, other)

    def __lt__(self, other):
        """
        Return a MetricOp representing less-than comparison.

        Parameters
        ----------
        other : MetricExpression or scalar
            The right-hand side of the less-than comparison.

        Returns
        -------
        MetricOp
            Metric operation representing less-than.
        """
        return MetricOp(operator.lt, self, other)

    def __le__(self, other):
        """
        Return a MetricOp representing less-than-or-equal comparison.

        Parameters
        ----------
        other : MetricExpression or scalar
            The right-hand side of the comparison.

        Returns
        -------
        MetricOp
            Metric operation representing less-than-or-equal.
        """
        return MetricOp(operator.le, self, other)

    def __or__(self, other):
        """
        Return a MetricOp representing logical OR operation.

        Parameters
        ----------
        other : MetricExpression
            The right-hand side of the OR operation.

        Returns
        -------
        MetricOp
            Metric operation representing logical OR.
        """
        return MetricOp(operator.or_, self, other)

    def __ror__(self, other):
        """
        Return a MetricOp representing logical OR operation (reversed operands).

        Parameters
        ----------
        other : MetricExpression
            The left-hand side of the OR operation.

        Returns
        -------
        MetricOp
            Metric operation representing logical OR.
        """
        return MetricOp(operator.or_, other, self)

    def __and__(self, other):
        """
        Return a MetricOp representing logical AND operation.

        Parameters
        ----------
        other : MetricExpression
            The right-hand side of the AND operation.

        Returns
        -------
        MetricOp
            Metric operation representing logical AND.
        """
        return MetricOp(operator.and_, self, other)

    def __rand__(self, other):
        """
        Return a MetricOp representing logical AND operation (reversed operands).

        Parameters
        ----------
        other : MetricExpression
            The left-hand side of the AND operation.

        Returns
        -------
        MetricOp
            Metric operation representing logical AND.
        """
        return MetricOp(operator.and_, other, self)

    def contains(self, value) -> "MetricOp":
        """
        Membership test for an ``array`` metric: keep rows whose array contains
        ``value``. Uses ``F.array_contains`` as the op, evaluated through the Spark
        ``get_selector_expr`` path — symmetric with the comparison ops (which use
        ``operator.eq`` etc.); ``F.array_contains`` fills the array-membership case
        that has no ``operator`` equivalent.

        Used for metrics whose values are stored as a plain array,
        e.g. ``q.metric("values").contains("ABCD-17")``.
        Can be used alongside the existing metric filters.

        Parameters
        ----------
        value
            The element to test for membership.

        Returns
        -------
        MetricOp
        """
        return MetricOp(F.array_contains, self, value)

    def contains_any(self, values: list) -> "MetricOp":
        """
        Array-membership OR: keep rows whose array shares **any** element with
        *values* (set intersection non-empty). This is the value-level OR that a
        single ``.contains`` can't express, e.g.
        ``q.metric("poi_defect_values").contains_any(["B1024-43", "U0046-13"])``
        keeps containers that saw *either* code. An empty *values* matches nothing.

        Parameters
        ----------
        values : list
            Elements to test for membership; matches if at least one is present.

        Returns
        -------
        MetricOp
        """

        def array_overlaps(arr: Column, vals: list) -> Column:
            # Called at get_selector_expr time (Spark active), so the literal
            # array is built lazily and the tree constructs without a
            # SparkSession. Empty values overlap nothing → False.
            if not vals:
                return F.lit(False)
            return F.arrays_overlap(arr, F.array(*[F.lit(v) for v in vals]))

        return MetricOp(array_overlaps, self, list(values))

    def contains_all(self, values: list) -> "MetricOp":
        """
        Array-membership AND: keep rows whose array contains **every** element of
        *values*, e.g. ``q.metric("poi_defect_values").contains_all(["B1024-43",
        "U0046-13"])`` keeps only containers that saw *both* codes. An empty
        *values* matches everything (vacuously true).

        Parameters
        ----------
        values : list
            Elements that must all be present.

        Returns
        -------
        MetricOp
        """

        def array_contains_all(arr: Column, vals: list) -> Column:
            # Built lazily at get_selector_expr time. Row survives when the
            # distinct intersection with arr covers all distinct target values.
            # Empty values is vacuously True.
            if not vals:
                return F.lit(True)
            target = F.array(*[F.lit(v) for v in vals])
            return F.size(F.array_intersect(target, arr)) == F.size(F.array_distinct(target))

        return MetricOp(array_contains_all, self, list(values))

    @abc.abstractmethod
    def get_selector_expr(self) -> Column:
        """
        Return a Spark SQL expression for selecting metrics.

        Returns
        -------
        pyspark.sql.Column
            Spark SQL column expression for metric selection.
        """
        pass

    @abc.abstractmethod
    def required_metrics(self) -> set[str]:
        """
        Return a set of required metric keys.

        Returns
        -------
        set of str
            Set of required metric keys.
        """
        pass


class MetricSelector(MetricExpression):
    def __init__(self, key: str):
        """
        Initialize a MetricSelector.

        Parameters
        ----------
        key : str
            The name of the metric to select.
        """
        self.key = key

    def get_selector_expr(self) -> Column:
        """
        Return a Spark SQL column expression for the selected metric.

        Returns
        -------
        pyspark.sql.Column
            Spark SQL column corresponding to the metric key.
        """
        return F.col(self.key)

    def __repr__(self):
        """
        Return the string representation of the MetricSelector.

        Returns
        -------
        str
            String representation.
        """
        return self.__str__()

    def __str__(self):
        """
        Return the string representation of the MetricSelector.

        Returns
        -------
        str
            String representation.
        """
        return f"MetricSelector<{self.key}>"

    def required_metrics(self) -> set[str]:
        """
        Return a set containing the metric key.

        Returns
        -------
        set of str
            Set containing the metric key.
        """
        return set([self.key])


class MetricOp(MetricExpression):
    def __init__(self, operation, *args, **kwargs):
        """
        Initialize a MetricOp.

        Parameters
        ----------
        operation : callable
            The operation to apply.
        *args
            Arguments like MetricExpressions for the operation.
        **kwargs
            Keyword arguments like MetricExpressions for the operation.
        """
        self.operation = operation
        self.args = args
        self.kwargs = kwargs

    def get_selector_expr(self) -> Column:
        """
        Build a Spark SQL expression for the metric selection.

        Returns
        -------
        pyspark.sql.Column
            Spark SQL column representing the metric operation.
        """
        argsb = [
            a.get_selector_expr() if isinstance(a, MetricExpression) else a for a in self.args
        ]
        kwargsb = {
            k: a.get_selector_expr() if isinstance(a, MetricExpression) else a
            for k, a in self.kwargs.items()
        }
        return self.operation(*argsb, **kwargsb)

    def __repr__(self):
        """
        Return the string representation of the MetricOp.

        Returns
        -------
        str
            String representation.
        """
        return self.__str__()

    def __str__(self):
        """
        Return the string representation of the MetricOp.

        Returns
        -------
        str
            String representation.
        """
        args_s = ",".join([str(arg) for arg in self.args])
        kwargs_s = ",".join([str(key) + "=" + str(value) for key, value in self.kwargs])
        if len(kwargs_s) == 0:
            return f"MetricOp<{self.operation.__name__}({args_s})>"
        else:
            return f"MetricOp<{self.operation.__name__}({args_s}, {kwargs_s})>"

    def required_metrics(self) -> set[str]:
        """
        Return a set of required metric keys for the operation.

        Returns
        -------
        set of str
            Set of required metric keys.
        """
        metrics = list()
        for arg in self.args:
            if hasattr(arg, "required_metrics"):
                metrics.extend(arg.required_metrics())
        for kwarg in self.kwargs.values():
            if hasattr(kwarg, "required_metrics"):
                metrics.extend(kwarg.required_metrics())
        return set(metrics)
