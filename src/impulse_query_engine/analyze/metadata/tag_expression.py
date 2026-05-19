from __future__ import annotations

import abc
import operator

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Column

import impulse_query_engine.util as U


class TagExpression(abc.ABC):
    def __eq__(self, other):
        """
        Return a TagOp representing equality comparison.

        Parameters
        ----------
        other : TagExpression or scalar
            The right-hand side of the equality comparison.

        Returns
        -------
        TagOp
            Tag operation representing equality.
        """
        return TagOp(operator.eq, self, other)

    def __gt__(self, other):
        """
        Return a TagOp representing greater-than comparison.

        Parameters
        ----------
        other : TagExpression or scalar
            The right-hand side of the greater-than comparison.

        Returns
        -------
        TagOp
            Tag operation representing greater-than.
        """
        return TagOp(operator.gt, self, other)

    def __ne__(self, other):
        """
        Return a TagOp representing inequality comparison.

        Parameters
        ----------
        other : TagExpression or scalar
            The right-hand side of the inequality comparison.

        Returns
        -------
        TagOp
            Tag operation representing inequality.
        """
        return TagOp(operator.ne, self, other)

    def __ge__(self, other):
        """
        Return a TagOp representing greater-than-or-equal comparison.

        Parameters
        ----------
        other : TagExpression or scalar
            The right-hand side of the comparison.

        Returns
        -------
        TagOp
            Tag operation representing greater-than-or-equal.
        """
        return TagOp(operator.ge, self, other)

    def __lt__(self, other):
        """
        Return a TagOp representing less-than comparison.

        Parameters
        ----------
        other : TagExpression or scalar
            The right-hand side of the less-than comparison.

        Returns
        -------
        TagOp
            Tag operation representing less-than.
        """
        return TagOp(operator.lt, self, other)

    def __le__(self, other):
        """
        Return a TagOp representing less-than-or-equal comparison.

        Parameters
        ----------
        other : TagExpression or scalar
            The right-hand side of the comparison.

        Returns
        -------
        TagOp
            Tag operation representing less-than-or-equal.
        """
        return TagOp(operator.le, self, other)

    def __or__(self, other):
        """
        Return a TagOp representing logical OR operation.

        Parameters
        ----------
        other : TagExpression
            The right-hand side of the OR operation.

        Returns
        -------
        TagOp
            Tag operation representing logical OR.
        """
        return TagOp(operator.or_, self, other)

    def __ror__(self, other):
        """
        Return a TagOp representing logical OR operation (reversed operands).

        Parameters
        ----------
        other : TagExpression
            The left-hand side of the OR operation.

        Returns
        -------
        TagOp
            Tag operation representing logical OR.
        """
        return TagOp(operator.or_, other, self)

    def __and__(self, other):
        """
        Return a TagOp representing logical AND operation.

        Parameters
        ----------
        other : TagExpression
            The right-hand side of the AND operation.

        Returns
        -------
        TagOp
            Tag operation representing logical AND.
        """
        return TagOp(operator.and_, self, other)

    def __rand__(self, other):
        """
        Return a TagOp representing logical AND operation (reversed operands).

        Parameters
        ----------
        other : TagExpression
            The left-hand side of the AND operation.

        Returns
        -------
        TagOp
            Tag operation representing logical AND.
        """
        return TagOp(operator.and_, other, self)

    @abc.abstractmethod
    def get_selector_expr(self) -> Column:
        """
        Return a Spark SQL expression for selecting tags.

        Returns
        -------
        pyspark.sql.Column
            Spark SQL column expression for tag selection.
        """
        pass

    @abc.abstractmethod
    def required_tags(self) -> set[str]:
        """
        Return a set of required tag keys.

        Returns
        -------
        set of str
            Set of required tag keys.
        """
        pass

    @abc.abstractmethod
    def build_pandas(self, df: pd.DataFrame) -> pd.Series:
        """
        Build a pandas Series based on the tag expression from the given DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame containing tag data.

        Returns
        -------
        pandas.Series
            Series representing the tag expression.
        """
        pass

    @abc.abstractmethod
    def as_dict(self) -> dict:
        """
        Return a dictionary representation of the tag expression.

        Returns
        -------
        dict
            Dictionary representation.
        """
        return {}

    @staticmethod
    def from_dict(obj: dict) -> TagExpression:
        """
        Construct a TagExpression from a dictionary.

        Parameters
        ----------
        obj : dict
            Dictionary containing tag expression data.

        Returns
        -------
        TagExpression
            TagExpression instance.
        """
        if not isinstance(obj, dict):
            return obj
        if "type" not in obj.keys():
            return obj
        cls = U.resolve_cls(obj["type"])
        return cls.from_dict(obj)


class TagSelector(TagExpression):

    _PANDAS_CAST_MAP = {
        "int": int,
        "long": int,
        "double": float,
        "float": float,
        "string": str,
    }

    def __init__(self, key: str, cast_type: str | None = None):
        """
        Initialize a TagSelector.

        Parameters
        ----------
        key : str
            The name of the tag to select.
        cast_type : str or None, optional
            Spark type to cast the column to before comparison
            (e.g. ``"int"``, ``"double"``, ``"string"``, ``"timestamp"``).
            When *None* (default) no casting is applied.
        """
        self.key = key
        self.cast_type = cast_type

    def get_selector_expr(self) -> Column:
        """
        Return a Spark SQL column expression for the selected tag.

        Returns
        -------
        pyspark.sql.Column
            Spark SQL column corresponding to the tag key, cast to
            ``cast_type`` when one is configured.
        """
        col = F.col(self.key)
        if self.cast_type is not None:
            col = col.cast(self.cast_type)
        return col

    def build_pandas(self, df: pd.DataFrame) -> pd.Series:
        """
        Return a pandas Series for the selected tag from the DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame containing tag data.

        Returns
        -------
        pandas.Series
            Series corresponding to the tag key, cast when configured.
        """
        series = df[f"ct_{self.key}"]
        if self.cast_type is not None:
            py_type = self._PANDAS_CAST_MAP.get(self.cast_type)
            if py_type is not None:
                series = series.astype(py_type)
        return series

    def required_tags(self) -> set[str]:
        """
        Return a set containing the tag key.

        Returns
        -------
        set of str
            Set containing the tag key.
        """
        return set([self.key])

    def __hash__(self):
        """
        Return the hash of the TagSelector.

        Returns
        -------
        int
            Hash value.
        """
        return hash((self.key, self.cast_type))

    def __repr__(self):
        """
        Return the string representation of the TagSelector.

        Returns
        -------
        str
            String representation.
        """
        return self.__str__()

    def __str__(self):
        """
        Return the string representation of the TagSelector.

        Returns
        -------
        str
            String representation.
        """
        if self.cast_type is not None:
            return f"TagSelector<{self.key}:{self.cast_type}>"
        return f"TagSelector<{self.key}>"

    def as_dict(self) -> dict:
        """
        Return a dictionary representation of the TagSelector.

        Returns
        -------
        dict
            Dictionary representation.
        """
        d = {"type": U.name_of(TagSelector), "key": self.key}
        if self.cast_type is not None:
            d["cast_type"] = self.cast_type
        return d

    @staticmethod
    def from_dict(obj: dict) -> TagSelector:
        """
        Construct a TagSelector from a dictionary.

        Parameters
        ----------
        obj : dict
            Dictionary containing tag selector data.

        Returns
        -------
        TagSelector
            TagSelector instance.
        """
        return TagSelector(obj["key"], cast_type=obj.get("cast_type"))


class TagOp(TagExpression):
    def __init__(self, operation, *args, **kwargs):
        """
        Initialize a TagOp.

        Parameters
        ----------
        operation : callable
            The operation to apply.
        *args
            Arguments like (TagSelector<channel_name>, 'Engine RPM') for the operation.
        **kwargs
            Keyword arguments for the operation.
        """
        self.operation = operation
        self.args = args
        self.kwargs = kwargs

    def get_selector_expr(self) -> Column:
        """
        Build a Spark SQL expression for the tag operation.

        Returns
        -------
        pyspark.sql.Column
            Spark SQL column representing the tag operation.
        """
        # build everything
        argsb = [a.get_selector_expr() if isinstance(a, TagExpression) else a for a in self.args]
        kwargsb = {
            k: a.get_selector_expr() if isinstance(a, TagExpression) else a
            for k, a in self.kwargs.items()
        }
        return self.operation(*argsb, **kwargsb)

    def build_pandas(self, df: pd.DataFrame) -> pd.Series:
        """
        Build a pandas Series for the tag operation from the given DataFrame.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame containing tag data.

        Returns
        -------
        pandas.Series
            Series representing the tag operation.
        """
        # build everything
        argsb = [a.build_pandas(df) if isinstance(a, TagExpression) else a for a in self.args]
        kwargsb = {
            k: a.build_pandas(df) if isinstance(a, TagExpression) else a
            for k, a in self.kwargs.items()
        }

        return self.operation(*argsb, **kwargsb)

    def required_tags(self) -> set[str]:
        """
        Return a set of required tag keys for the operation.

        Returns
        -------
        set of str
            Set of required tag keys.
        """
        tags = list()
        for arg in self.args:
            if hasattr(arg, "required_tags"):
                tags.extend(arg.required_tags())
        for kwarg in self.kwargs.values():
            if hasattr(kwarg, "required_tags"):
                tags.extend(kwarg.required_tags())
        return set(tags)

    def __hash__(self):
        """
        Return the hash of the TagOp.

        Returns
        -------
        int
            Hash value.
        """
        args_list = [hash(a) for a in self.args]
        args_hash = hash(tuple(args_list))
        kwargs_list = [hash((k, hash(a))) for k, a in self.kwargs]
        kwargs_hash = hash(tuple(kwargs_list))
        return hash((self.operation, args_hash, kwargs_hash))

    def __repr__(self):
        """
        Return the string representation of the TagOp.

        Returns
        -------
        str
            String representation.
        """
        return self.__str__()

    def __str__(self):
        """
        Return the string representation of the TagOp.

        Returns
        -------
        str
            String representation.
        """
        args_s = ",".join([str(arg) for arg in self.args])
        kwargs_s = ",".join([str(key) + "=" + str(value) for key, value in self.kwargs])
        if len(kwargs_s) == 0:
            return f"TagOp<{self.operation.__name__}({args_s})>"
        else:
            return f"TagOp<{self.operation.__name__}({args_s}, {kwargs_s})>"

    def as_dict(self) -> dict:
        """
        Return a dictionary representation of the TagOp.

        Returns
        -------
        dict
            Dictionary representation.
        """
        args_dicts = [a.as_dict() if hasattr(a, "as_dict") else a for a in self.args]
        kwargs_dicts = {
            k: v.as_dict() if hasattr(v, "as_dict") else v for k, v in self.kwargs.items()
        }
        return {
            "type": U.name_of(TagOp),
            "op": U.name_of(self.operation),
            "args": args_dicts,
            "kwargs": kwargs_dicts,
        }

    @staticmethod
    def from_dict(obj: dict) -> TagOp:
        """
        Construct a TagOp from a dictionary.

        Parameters
        ----------
        obj : dict
            Dictionary containing tag operation data.

        Returns
        -------
        TagOp
            TagOp instance.
        """
        op = U.resolve_fn(obj["op"])
        args = [TagExpression.from_dict(a) if isinstance(a, dict) else a for a in obj["args"]]
        kwargs = {
            k: TagExpression.from_dict(v) if isinstance(v, dict) else v
            for k, v in obj["kwargs"].items()
        }
        return TagOp(op, *args, **kwargs)
