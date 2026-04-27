import operator
import abc

import pandas as pd
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
    def build_pandas(self, df: pd.DataFrame) -> pd.Series:
        """
        Build a pandas Series based on the metric expression from the given DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame containing metric data.

        Returns
        -------
        pandas.Series
            Series representing the metric expression.
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

    def build_pandas(self, df) -> pd.Series:
        """
        Return a pandas Series for the selected metric from the DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame containing metric data.

        Returns
        -------
        pandas.Series
            Series corresponding to the metric key.
        """
        return df[self.key]

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

    def build_pandas(self, df) -> pd.Series:
        """
        Build a pandas Series for the metric operation from the given DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame containing metric data.

        Returns
        -------
        pandas.Series
            Series representing the metric operation.
        """
        argsb = [a.build_pandas(df) if isinstance(a, MetricExpression) else a for a in self.args]
        kwargsb = {
            k: a.build_pandas(df) if isinstance(a, MetricExpression) else a
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
