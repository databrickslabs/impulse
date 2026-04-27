"""Basic Pipeline tests"""

# pylint: disable=missing-function-docstring, redefined-outer-name

import operator

from mda_query_engine.analyze.metadata.tag_expression import TagSelector
from mda_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesAliasSelector,
    TimeSeriesExpression,
    TimeSeriesOp,
    TimeSeriesSelector,
)


def test_where():
    expr = TimeSeriesSelector(TagSelector("name") == "test")
    expr2 = expr.where(expr == 1)
    assert isinstance(expr2, TimeSeriesOp)


def test_is_single_signal():
    expr1 = TimeSeriesSelector(TagSelector("name") == "test1")
    expr2 = TimeSeriesSelector(TagSelector("name") == "test2")
    assert expr1.is_single_signal
    assert expr2.is_single_signal
    expr3 = expr1 + 1
    assert expr3.is_single_signal
    expr4 = expr1 + expr2
    assert not expr4.is_single_signal


def test_requires_udf():
    expr1 = TimeSeriesSelector(TagSelector("name") == "test1")
    expr_t = expr1 + 2
    assert not expr_t.requires_udf
    expr_t = expr1 * 2
    assert not expr_t.requires_udf
    expr_t = expr1 / 2
    assert not expr_t.requires_udf
    expr_t = expr1 - 2
    assert not expr_t.requires_udf
    expr_t = expr1 % 2
    assert not expr_t.requires_udf
    expr_t = expr1.where(expr1 > 1)
    assert not expr_t.requires_udf
    expr_t = expr1.apply(lambda ts: ts * 2)
    assert expr_t.requires_udf


def test_apply_udf():
    expr1 = TimeSeriesSelector(TagSelector("name") == "test")
    expr_t = expr1.apply(lambda ts: ts * 2)


def test_create_udf():
    func = lambda ts, scalar: ts * scalar
    prepped_func = TimeSeriesExpression.udf(func)
    expr1 = TimeSeriesSelector(TagSelector("name") == "test1")
    expr2 = prepped_func(expr1, 1.5)
    assert expr2.is_single_signal
    assert expr2.requires_udf


def test_serialize_selector():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    obj = sel.as_dict()
    assert "type" in obj
    assert "expr" in obj
    assert "alias" in obj
    assert (
        obj["type"]
        == "mda_query_engine.analyze.metadata.time_series_expression.TimeSeriesSelector"
    )
    assert obj["alias"] == ""
    sel_deser = TimeSeriesExpression.from_dict(obj)
    assert sel._alias == sel_deser._alias


def test_serialize_op():
    op = TimeSeriesSelector(TagSelector("name") == "test") == 123
    obj = op.as_dict()
    assert "type" in obj
    assert "alias" in obj
    assert "args" in obj
    assert "kwargs" in obj
    assert "op" in obj
    assert "optype" in obj
    assert obj["type"] == "mda_query_engine.analyze.metadata.time_series_expression.TimeSeriesOp"
    assert obj["alias"] == ""
    assert len(obj["args"]) == 2
    assert len(obj["kwargs"]) == 0
    assert (
        obj["args"][0]["type"]
        == "mda_query_engine.analyze.metadata.time_series_expression.TimeSeriesSelector"
    )
    assert obj["args"][1] == 123
    op_deser = TimeSeriesExpression.from_dict(obj)
    assert op._alias == op_deser._alias


def test_serialize_op_cls():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    op = sel.where(sel == 123)
    obj = op.as_dict()
    assert "type" in obj
    assert "alias" in obj
    assert "args" in obj
    assert "kwargs" in obj
    assert "op" in obj
    assert "optype" in obj


def test_time_series_selector_str():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    s = str(sel)
    assert s == "TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>"


def test_time_series_alias_selector_str():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    alias_sel = TimeSeriesAliasSelector(sel, "my_alias")
    s = str(alias_sel)
    assert (
        s
        == "TimeSeriesAliasSelector<TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, my_alias>"
    )


def test_time_series_op_str():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    op = sel.where(sel == 123)
    s = str(op)
    assert s == (
        "TimeSeriesOp<where(TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, "
        "TimeSeriesOp<eq(TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, 123)>)>"
    )


def test_time_series_udf_str():
    func = lambda ts, scalar: ts * scalar
    prepped_func = TimeSeriesExpression.udf(func)
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    op = prepped_func(sel, 1.5)
    s = str(op)
    assert (
        s == "TimeSeriesUDF<<lambda>(TimeSeriesSelector<TagOp<eq(TagSelector<name>,test)>>, 1.5)>"
    )


def test_mod_returns_time_series_op():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    op = sel % 3
    assert isinstance(op, TimeSeriesOp)
    assert op.operation is operator.mod
    assert op.optype == "builtin"
    assert op.args[0] is sel
    assert op.args[1] == 3


def test_mod_expression_with_expression():
    sel1 = TimeSeriesSelector(TagSelector("name") == "test1")
    sel2 = TimeSeriesSelector(TagSelector("name") == "test2")
    op = sel1 % sel2
    assert isinstance(op, TimeSeriesOp)
    assert op.operation is operator.mod
    assert op.args[0] is sel1
    assert op.args[1] is sel2


def test_rmod_returns_time_series_op():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    op = 5 % sel
    assert isinstance(op, TimeSeriesOp)
    assert op.operation is operator.mod
    assert op.optype == "builtin"
    assert op.args[0] == 5
    assert op.args[1] is sel


def test_rmod_expression_with_float():
    sel = TimeSeriesSelector(TagSelector("name") == "test")
    op = 2.5 % sel
    assert isinstance(op, TimeSeriesOp)
    assert op.args[0] == 2.5
    assert op.args[1] is sel
