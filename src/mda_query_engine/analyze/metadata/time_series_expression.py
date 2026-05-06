from __future__ import annotations

import abc
import operator
import zlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import pyspark.sql.types as T

import mda_query_engine.util as U
from mda_query_engine.analyze.metadata.tag_expression import TagExpression
from mda_query_engine.model.series.sample_series import SampleSeries

if TYPE_CHECKING:
    from mda_query_engine.analyze.query.solvers.series_cache import SeriesCache


class RequiresDeserialization:
    pass


class TimeSeriesExpression(abc.ABC):
    def __init__(self, alias: str = "", is_single_signal: bool = True, requires_udf: bool = False):
        """
        Initialize a TimeSeriesExpression.

        Parameters
        ----------
        alias : str, optional
            Alias for the expression.
        is_single_signal : bool, optional
            Whether the expression refers to a single signal.
        requires_udf : bool, optional
            Whether the expression requires a user-defined function.
        """
        self._alias = alias
        self.is_single_signal = is_single_signal
        self.requires_udf = requires_udf

    @property
    def requires_sync(self):
        """
        Whether synchronization is required.

        Returns
        -------
        bool
            True if synchronization is required, False otherwise.
        """
        return not self.is_single_signal

    def dtype(self):
        """
        Get the default Spark data type.

        Returns
        -------
        pyspark.sql.types.DataType
            Default data type (DoubleType).
        """
        return T.DoubleType()

    def __getstate__(self):  # overwrite to avoid errors from __getattr__
        return self.__dict__

    def __setstate__(self, obj):  # overwrite to avoid errors from __getattr__
        self.__dict__ = obj

    def __add__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.add, "builtin", self, other)

    def __radd__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.add, "builtin", other, self)

    def __sub__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.sub, "builtin", self, other)

    def __rsub__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.sub, "builtin", other, self)

    def __mul__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.mul, "builtin", self, other)

    def __rmul__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.mul, "builtin", other, self)

    def __truediv__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.truediv, "builtin", self, other)

    def __rtruediv__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.truediv, "builtin", other, self)

    def __mod__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.mod, "builtin", self, other)

    def __rmod__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.mod, "builtin", other, self)

    def __eq__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.eq, "builtin", self, other)

    def __ne__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.ne, "builtin", self, other)

    def __gt__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.gt, "builtin", self, other)

    def __ge__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.ge, "builtin", self, other)

    def __lt__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.lt, "builtin", self, other)

    def __le__(self, other: TimeSeriesExpression | float | int) -> TimeSeriesOp:
        return TimeSeriesOp(operator.le, "builtin", self, other)

    def __or__(self, other: TimeSeriesExpression | bool) -> TimeSeriesOp:
        return TimeSeriesOp(operator.or_, "builtin", self, other)

    def __ror__(self, other: TimeSeriesExpression | bool) -> TimeSeriesOp:
        return TimeSeriesOp(operator.or_, "builtin", other, self)

    def __and__(self, other: TimeSeriesExpression | bool) -> TimeSeriesOp:
        return TimeSeriesOp(operator.and_, "builtin", self, other)

    def __rand__(self, other: TimeSeriesExpression | bool) -> TimeSeriesOp:
        return TimeSeriesOp(operator.and_, "builtin", other, self)

    @abc.abstractmethod
    def build(self, cache: SeriesCache) -> Any:
        """
        Build the time series from the cache.

        Parameters
        ----------
        cache : SeriesCache
            Cache containing time series data.

        Returns
        -------
        Any
            Built time series object.
        """
        pass

    @abc.abstractmethod
    def get_required_tag_exprs(self) -> set[TagExpression]:
        """
        Get required tag expressions.

        Returns
        -------
        set of TagExpression
            Required tag expressions.
        """
        pass

    @abc.abstractmethod
    def required_tags(self) -> set[str]:
        """
        Get required tag keys.

        Returns
        -------
        set of str
            Required tag keys.
        """
        pass

    @abc.abstractmethod
    def get_selector_expr(self):
        """
        Get the selector expression.

        Returns
        -------
        Any
            Selector expression.
        """
        pass

    @abc.abstractmethod
    def get_selectors(self) -> list["TimeSeriesSelector"]:
        """
        Return all leaf :class:`TimeSeriesSelector` nodes reachable from
        this expression.

        The returned list may contain duplicates when the same selector
        appears in multiple branches of the expression tree.
        Callers are responsible for deduplication if needed.

        Returns
        -------
        list of TimeSeriesSelector
            Leaf selectors.
        """
        pass

    @abc.abstractmethod
    def __str__(self) -> str:
        """
        Get the string representation.

        Returns
        -------
        str
            String representation.
        """
        pass

    def sum(self) -> TimeSeriesOp:
        """
        Calculate the sum of all values (normalized by duration).

        Returns
        -------
        TimeSeriesOp
            Sum operation.
        """
        return TimeSeriesOp("sum", "cls", self)

    def min(self) -> TimeSeriesOp:
        """
        Calculate the minimum value.

        Returns
        -------
        TimeSeriesOp
            Min operation.
        """
        return TimeSeriesOp("min", "cls", self)

    def max(self) -> TimeSeriesOp:
        """
        Calculate the maximum value.

        Returns
        -------
        TimeSeriesOp
            Max operation.
        """
        return TimeSeriesOp("max", "cls", self)

    def mean(self) -> TimeSeriesOp:
        """
        Calculate the mean value.

        Returns
        -------
        TimeSeriesOp
            Mean operation.
        """
        return TimeSeriesOp("mean", "cls", self)

    def where(self, other: TimeSeriesExpression) -> TimeSeriesExpression:
        """
        Filter the series by another expression.

        Parameters
        ----------
        other : TimeSeriesExpression
            Expression to filter by.

        Returns
        -------
        TimeSeriesExpression
            Filtered expression.
        """
        return TimeSeriesOp("where", "cls", self, other)

    def __getattr__(self, attr) -> Callable[[], TimeSeriesOp]:
        """
        Dynamically create a TimeSeriesOp for the given attribute.

        Parameters
        ----------
        attr : str
            Attribute name.

        Returns
        -------
        Callable
            Wrapper function returning a TimeSeriesOp.
        """

        def __getattr__wrapper(*args, **kwargs):
            return TimeSeriesOp(attr, "cls", self, *args, **kwargs)

        return __getattr__wrapper

    def alias(self, alias_name: str) -> TimeSeriesExpression:
        """
        Set the alias for this expression.

        Parameters
        ----------
        alias_name : str
            Alias name.

        Returns
        -------
        TimeSeriesExpression
            Expression with alias set.
        """
        self._alias = alias_name
        return self

    def histogram(self, bins: list[float]):
        """
        Create a histogram with given bins.

        Parameters
        ----------
        bins : list of float
            Bin edges for the histogram.

        Returns
        -------
        Histogram
            Histogram aggregation object.
        """
        from mda_query_engine.analyze.query.aggregations.histogram import (
            HistogramDuration,
        )

        return HistogramDuration(self, bins)

    def histogram_custom_weights(
        self,
        bins: list[float],
        weights,
        channel_interp_kind="previous",
        weights_interp_kind="previous",
        math_fct_for_weights=None,
        math_fct_kwargs=None,
        weight_type=None,
    ):
        """
        Create a histogram with custom weights applied to bins.

        Parameters
        ----------
        bins : list of float
            Bin edges for the histogram.
        weights : TimeSeriesExpression
            Weights expression to apply to the histogram instead of duration.
        channel_interp_kind : str, optional
            Interpolation method for channel data (default: 'previous').
        weights_interp_kind : str, optional
            Interpolation method for weights data (default: 'previous').
        math_fct_for_weights : callable, optional
            Mathematical function to apply to weights (default: None).
        math_fct_kwargs : dict, optional
            Keyword arguments for math_fct_for_weights (default: {}).
        weight_type : str, optional
            Type of weighting to use. Options:
            - None (default): Use custom weights only
            - 'time': Multiply custom weights by sample duration

        Returns
        -------
        HistogramCustomWeights
            Histogram aggregation object with custom weights applied.
        """
        from mda_query_engine.analyze.query.aggregations.histogram import (
            HistogramCustomWeights,
        )

        return HistogramCustomWeights(
            self,
            weights=weights,
            bins=bins,
            channel_interp_kind=channel_interp_kind,
            weights_interp_kind=weights_interp_kind,
            math_fct_for_weights=math_fct_for_weights,
            math_fct_kwargs=math_fct_kwargs,
            weight_type=weight_type,
        )

    def histogram2d(self, y_selection, x_bins: list[float], y_bins: list[float]):
        """
        Create a bi-dimensional histogram with given bins.

        Parameters
        ----------
        y_selection : TimeSeriesExpression
            Expression for selecting y-axis time series.
        x_bins : list of float
            Bin edges for the x-axis.
        y_bins : list of float
            Bin edges for the y-axis.

        Returns
        -------
        Histogram2D
            2D histogram aggregation object.
        """
        from mda_query_engine.analyze.query.aggregations.histogram2d import (
            Histogram2DDuration,
        )

        return Histogram2DDuration(self, y_selection, x_bins, y_bins)

    def histogram2d_custom_weights(
        self,
        y_selection,
        weights_selection,
        x_bins: list[float],
        y_bins: list[float],
        channel_interp_kind="previous",
        weights_interp_kind="previous",
        math_fct_for_weights=None,
        math_fct_kwargs=None,
        weight_type=None,
    ):
        """
        Create a bi-dimensional histogram with given bins and custom weights.

        Parameters
        ----------
        y_selection : TimeSeriesExpression
            Expression for selecting y-axis time series.
        weights_selection : TimeSeriesExpression
            Expression for selecting weights time series.
        x_bins : list of float
            Bin edges for the x-axis.
        y_bins : list of float
            Bin edges for the y-axis.
        channel_interp_kind : str, optional
            Interpolation method for channel data (default: 'previous').
        weights_interp_kind : str, optional
            Interpolation method for weights data (default: 'previous').
        math_fct_for_weights : callable, optional
            Mathematical function to apply to weights (default: None).
        math_fct_kwargs : dict, optional
            Keyword arguments for math_fct_for_weights (default: {}).
        weight_type : str, optional
            Type of weighting to use. Options:
            - None (default): Use custom weights only
            - 'time': Multiply custom weights by sample duration

        Returns
        -------
        Histogram2DCustomWeights
            2D histogram aggregation object with custom weights.
        """
        from mda_query_engine.analyze.query.aggregations.histogram2d import (
            Histogram2DCustomWeights,
        )

        return Histogram2DCustomWeights(
            self,
            y_selection,
            weights_selection,
            x_bins,
            y_bins,
            channel_interp_kind=channel_interp_kind,
            weights_interp_kind=weights_interp_kind,
            math_fct_for_weights=math_fct_for_weights,
            math_fct_kwargs=math_fct_kwargs,
            weight_type=weight_type,
        )

    def apply(self, func) -> TimeSeriesUDF:
        """
        Apply a function to the expression.

        Parameters
        ----------
        func : callable
            Function to apply.

        Returns
        -------
        TimeSeriesUDF
            UDF-wrapped expression.
        """
        return TimeSeriesUDF(func, self)

    @staticmethod
    def udf(func: Callable) -> CallableTimeSeriesExpression:
        """
        Wrap a function as a CallableTimeSeriesExpression.

        Parameters
        ----------
        func : callable
            Function to wrap.

        Returns
        -------
        CallableTimeSeriesExpression
            Callable wrapper.
        """
        return CallableTimeSeriesExpression(func)

    def as_dict(self) -> dict[str, Any]:
        """
        Return a dictionary representation of the expression.

        Returns
        -------
        dict
            Dictionary representation.
        """
        return {"alias": self._alias}

    @staticmethod
    def from_dict(obj: dict) -> TimeSeriesExpression:
        """
        Construct a TimeSeriesExpression from a dictionary.

        Parameters
        ----------
        obj : dict
            Dictionary containing expression data.

        Returns
        -------
        TimeSeriesExpression
            Expression instance.
        """
        if not isinstance(obj, dict):
            return obj
        if "type" not in obj.keys():
            return obj
        cls = U.resolve_cls(obj["type"])
        return cls.from_dict(obj)


class TimeSeriesSelector(TimeSeriesExpression, RequiresDeserialization):
    def __init__(self, expr, uses_alias: bool = False):
        """
        Initialize a TimeSeriesSelector.

        Parameters
        ----------
        expr : TagExpression
            Tag expression to select.
        """
        self._expr = expr
        self._uses_alias = uses_alias
        TimeSeriesExpression.__init__(self, is_single_signal=True)

    @property
    def uses_alias(self) -> bool:
        return self._uses_alias

    @property
    def selector_id(self) -> int:
        return zlib.crc32(str(self._expr).encode())

    def dtype(self):
        """
        Returns the Spark data type.

        Returns
        -------
        pyspark.sql.types.DataType
            Data type (BinaryType).
        """
        return T.BinaryType()

    def deserialize(self, d):
        """
        Deserialize sample series after collection/toPandas.

        Parameters
        ----------
        d : Any
            Data to deserialize.

        Returns
        -------
        SampleSeries
            Deserialized sample series.
        """
        return SampleSeries.deserialize(d)

    def build(self, cache: SeriesCache) -> SampleSeries:
        """
        Instantiate a SampleSeries from given cache data.

        Parameters
        ----------
        cache : SeriesCache
            Cache containing time series data.

        Returns
        -------
        SampleSeries
            Built sample series.
        """
        candidates = cache.resolve(self)
        if len(candidates) == 0:
            return SampleSeries.empty()
        # TODO: select candidate
        mid = candidates.container_id.iloc[0]
        cid = candidates.channel_id.iloc[0]
        return cache.load_blob(mid, cid)

    def get_required_tag_exprs(self) -> set[TagExpression]:
        """
        Get required tag expressions.

        Returns
        -------
        set of TagExpression
            Required tag expressions.
        """
        return set([self._expr])

    def required_tags(self) -> set[str]:
        """
        Get required tag keys.

        Returns
        -------
        set of str
            Required tag keys.
        """
        return self._expr.required_tags()

    def get_selector_expr(self):
        """
        Get selector expression.

        Returns
        -------
        Any
            Selector expression.
        """
        return self._expr.get_selector_expr()

    def get_selectors(self) -> list["TimeSeriesSelector"]:
        return [self]

    def with_alias(self, *args):
        """
        Create an alias selector.

        Parameters
        ----------
        *args
            Aliases to use.

        Returns
        -------
        TimeSeriesAliasSelector
            Alias selector.
        """
        return TimeSeriesAliasSelector(*([self] + list(args)))

    def __str__(self):
        """
        String representation.

        Returns
        -------
        str
            String representation.
        """
        return f"TimeSeriesSelector<{self._expr}>"

    def as_dict(self) -> dict[str, Any]:
        """
        Dictionary representation.

        Returns
        -------
        dict
            Dictionary representation.
        """
        obj = TimeSeriesExpression.as_dict(self)
        obj["type"] = U.name_of(TimeSeriesSelector)
        obj["expr"] = self._expr.as_dict()
        obj["uses_alias"] = self._uses_alias
        return obj

    @staticmethod
    def from_dict(obj: dict):
        """
        Construct from dictionary.

        Parameters
        ----------
        obj : dict
            Dictionary containing selector data.

        Returns
        -------
        TimeSeriesSelector
            Selector instance.
        """
        expr = TimeSeriesExpression.from_dict(obj["expr"])
        m = TimeSeriesSelector(expr, uses_alias=obj.get("uses_alias", False))
        if "alias" in obj and obj["alias"] is not None:
            m.alias(obj["alias"])
        return m


class TimeSeriesAliasSelector(TimeSeriesExpression):
    def __init__(self, *aliases):
        """
        Initialize a TimeSeriesAliasSelector.

        Parameters
        ----------
        *aliases : TimeSeriesSelector
            Aliases to select.
        """
        self._aliases = aliases
        TimeSeriesExpression.__init__(self, is_single_signal=True)

    def dtype(self):
        """
        Returns the Spark data type.

        Returns
        -------
        pyspark.sql.types.DataType
            Data type (BinaryType).
        """
        return T.BinaryType()

    def build(self, cache: SeriesCache) -> SampleSeries:
        """
        Build the time series from cache.

        Parameters
        ----------
        cache : SeriesCache
            Cache containing time series data.

        Returns
        -------
        SampleSeries
            Built sample series.
        """
        candidates = [alias.build(cache) for alias in self._aliases]
        # TODO: propery select best candidate
        return candidates[0]

    def get_required_tag_exprs(self) -> set[TagExpression]:
        """
        Get required tag expressions.

        Returns
        -------
        set of TagExpression
            Required tag expressions.
        """
        tags = set()
        for alias in self._aliases:
            tags = tags.union(alias.get_required_tag_exprs())
        return tags

    def required_tags(self) -> set[str]:
        """
        Get required tag keys.

        Returns
        -------
        set of str
            Required tag keys.
        """
        tags = list()
        for alias in self._aliases:
            tags.extend(alias.required_tags())
        return set(tags)

    def get_selector_expr(self):
        """
        Get selector expression.

        Returns
        -------
        Any
            Selector expression.
        """
        expr = None
        for alias in self._aliases:
            if expr is None:
                expr = alias.get_selector_expr()
            else:
                expr = expr | alias.get_selector_expr()
        return expr

    def get_selectors(self) -> list["TimeSeriesSelector"]:
        result: list[TimeSeriesSelector] = []
        for alias in self._aliases:
            result.extend(alias.get_selectors())
        return result

    def __str__(self):
        """
        String representation.

        Returns
        -------
        str
            String representation.
        """
        return f"TimeSeriesAliasSelector<{', '.join([str(a) for a in self._aliases])}>"


class TimeSeriesOp(TimeSeriesExpression):
    def __init__(self, operation, optype, *args, **kwargs):
        """
        Initialize a TimeSeriesOp.

        Parameters
        ----------
        operation : callable
            The operation to apply.
        optype : str
            Type of operation.
        *args
            Arguments (like (TimeSeriesSelector<TagOp<eq(TagSelector<channel_name>,Vehicle Speed Sensor)>>, 1))
            for the operation.
        **kwargs
            Keyword arguments for the operation.
        """
        self.operation = operation
        self.args = args
        self.kwargs = kwargs
        self.optype = optype
        is_single_signal = len(self.get_required_tag_exprs()) == 1
        TimeSeriesExpression.__init__(self, is_single_signal=is_single_signal)

    def get_required_tag_exprs(self) -> set[TagExpression]:
        """
        Get required tag expressions.

        Returns
        -------
        set of TagExpression
            Required tag expressions.
        """
        tags = set()
        for arg in self.args:
            if not hasattr(arg, "get_required_tag_exprs"):
                continue
            tags = tags.union(arg.get_required_tag_exprs())
        for kwarg in self.kwargs.values():
            if not hasattr(kwarg, "get_required_tag_exprs"):
                continue
            tags = tags.union(kwarg.get_required_tag_exprs())
        return tags

    def required_tags(self) -> set[str]:
        """
        Get required tag keys.

        Returns
        -------
        set of str
            Required tag keys.
        """
        tags = list()
        for arg in self.args:
            if hasattr(arg, "required_tags"):
                tags.extend(arg.required_tags())
        for kwarg in self.kwargs.values():
            if hasattr(kwarg, "required_tags"):
                tags.extend(kwarg.required_tags())
        return set(tags)

    def get_selector_expr(self):
        """
        Get selector expression.

        Returns
        -------
        Any
            Selector expression.
        """
        expr = None
        for arg in self.args:
            if hasattr(arg, "get_selector_expr"):
                arg_e = arg.get_selector_expr()
                if expr is None:
                    expr = arg_e
                else:
                    expr = expr | arg_e
        for kwarg in self.kwargs.values():
            if hasattr(kwarg, "get_selector_expr"):
                kwarg_e = kwarg.get_selector_expr()
                if expr is None:
                    expr = kwarg_e
                else:
                    expr = expr | kwarg_e
        return expr

    def get_selectors(self) -> list["TimeSeriesSelector"]:
        result: list[TimeSeriesSelector] = []
        for arg in self.args:
            if isinstance(arg, TimeSeriesExpression):
                result.extend(arg.get_selectors())
        for kwarg in self.kwargs.values():
            if isinstance(kwarg, TimeSeriesExpression):
                result.extend(kwarg.get_selectors())
        return result

    def build(self, cache: SeriesCache):
        """
        Build the time series from cache.

        Parameters
        ----------
        cache : SeriesCache
            Cache containing time series data.

        Returns
        -------
        Any
            Built time series object.
        """
        argsb = [a.build(cache) if isinstance(a, TimeSeriesExpression) else a for a in self.args]
        kwargsb = {
            k: a.build(cache) if isinstance(a, TimeSeriesExpression) else a
            for k, a in self.kwargs.items()
        }
        if self.optype == "cls":
            op = getattr(argsb[0], self.operation)
            return op(*argsb[1:], **kwargsb)
        elif self.optype == "builtin":
            return self.operation(*argsb, **kwargsb)
        elif self.optype == "python":
            return self.operation(*argsb, **kwargsb)
        # unknown case
        return self.operation(*argsb, **kwargsb)

    def __str__(self):
        """
        String representation.

        Returns
        -------
        str
            String representation.
        """
        args_s = ", ".join([str(arg) for arg in self.args])
        kwargs_s = ", ".join([str(key) + "=" + str(value) for key, value in self.kwargs.items()])
        opname = self.operation if isinstance(self.operation, str) else self.operation.__name__
        if len(kwargs_s) == 0:
            return f"TimeSeriesOp<{opname}({args_s})>"
        return f"TimeSeriesOp<{opname}({args_s}, {kwargs_s})>"

    def as_dict(self) -> dict[str, Any]:
        """
        Dictionary representation.

        Returns
        -------
        dict
            Dictionary representation.
        """
        obj = TimeSeriesExpression.as_dict(self)
        obj["type"] = U.name_of(TimeSeriesOp)
        obj["args"] = [a.as_dict() if hasattr(a, "as_dict") else a for a in self.args]
        obj["kwargs"] = {
            k: v.as_dict() if hasattr(v, "as_dict") else v for k, v in self.kwargs.items()
        }
        if self.optype == "builtin":
            obj["op"] = U.name_of(self.operation)
        elif self.optype == "cls":
            obj["op"] = self.operation
        else:  # todo: ??? other cases ???
            obj["op"] = U.name_of(self.operation)
        obj["optype"] = self.optype
        return obj

    @staticmethod
    def from_dict(obj):
        """
        Construct from dictionary.

        Parameters
        ----------
        obj : dict
            Dictionary containing operation data.

        Returns
        -------
        TimeSeriesOp
            Operation instance.
        """
        op = U.resolve_fn(obj["op"])
        args = [
            TimeSeriesExpression.from_dict(a) if isinstance(a, dict) else a for a in obj["args"]
        ]
        kwargs = {
            k: TimeSeriesExpression.from_dict(v) if isinstance(v, dict) else v
            for k, v in obj["kwargs"].items()
        }
        return TimeSeriesOp(op, obj["optype"], *args, **kwargs)


class TimeSeriesUDF(TimeSeriesOp):
    def __init__(self, func, *args, **kwargs):
        """
        Initialize a TimeSeriesUDF.

        Parameters
        ----------
        func : callable
            The user-defined function to apply.
        *args
            Arguments for the UDF.
        **kwargs
            Keyword arguments for the UDF.
        """
        TimeSeriesOp.__init__(self, func, *args, **kwargs)
        self.func = func
        # todo: properly check args + kwargs
        self.requires_udf = True
        # todo: check if func is a DF function somehow?
        self.is_single_signal = True
        self.args = args
        self.kwargs = kwargs

    def build(self, cache: SeriesCache):
        """
        Build the time series from cache using the UDF.

        Parameters
        ----------
        cache : SeriesCache
            Cache containing time series data.

        Returns
        -------
        Any
            Result of applying the UDF to the built arguments.
        """
        argsb = [a.build(cache) if isinstance(a, TimeSeriesExpression) else a for a in self.args]
        kwargsb = {
            k: a.build(cache) if isinstance(a, TimeSeriesExpression) else a
            for k, a in self.kwargs.items()
        }
        if isinstance(self.operation, str):
            op = getattr(argsb[0], self.operation)
            return op(*argsb[1:], **kwargsb)
        else:
            return self.operation(*argsb, **kwargsb)

    def __str__(self):
        """
        Return the string representation of the TimeSeriesUDF.

        Returns
        -------
        str
            String representation.
        """
        args_s = ", ".join([str(arg) for arg in self.args])
        kwargs_s = ", ".join([str(key) + "=" + str(value) for key, value in self.kwargs.items()])
        opname = self.operation if isinstance(self.operation, str) else self.operation.__name__
        if len(kwargs_s) == 0:
            return f"TimeSeriesUDF<{opname}({args_s})>"
        return f"TimeSeriesUDF<{opname}({args_s}, {kwargs_s})>"


class CallableTimeSeriesExpression:
    def __init__(self, func):
        """
        Initialize a CallableTimeSeriesExpression.

        Parameters
        ----------
        func : callable
            Function to wrap.
        """
        self.func = func

    def __call__(self, *args, **kwargs):
        """
        Create a TimeSeriesUDF with the wrapped function.

        Parameters
        ----------
        *args
            Arguments for the function.
        **kwargs
            Keyword arguments for the function.

        Returns
        -------
        TimeSeriesUDF
            UDF-wrapped expression.
        """
        return TimeSeriesUDF(self.func, *args, **kwargs)
