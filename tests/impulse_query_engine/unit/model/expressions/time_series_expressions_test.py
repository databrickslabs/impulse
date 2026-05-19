"""Basic Pipeline tests"""

# pylint: disable=missing-function-docstring, redefined-outer-name

import operator

from impulse_query_engine.analyze.metadata.tag_expression import TagSelector
from impulse_query_engine.analyze.metadata.time_series_expression import (
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
        == "impulse_query_engine.analyze.metadata.time_series_expression.TimeSeriesSelector"
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
    assert (
        obj["type"] == "impulse_query_engine.analyze.metadata.time_series_expression.TimeSeriesOp"
    )
    assert obj["alias"] == ""
    assert len(obj["args"]) == 2
    assert len(obj["kwargs"]) == 0
    assert (
        obj["args"][0]["type"]
        == "impulse_query_engine.analyze.metadata.time_series_expression.TimeSeriesSelector"
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


class TestGetSelectors:
    def test_selector_returns_self(self):
        sel = TimeSeriesSelector(TagSelector("name") == "test")
        result = sel.get_selectors()
        assert result == [sel]

    def test_selector_with_alias_flag(self):
        sel_direct = TimeSeriesSelector(TagSelector("name") == "a")
        sel_aliased = TimeSeriesSelector(TagSelector("alias") == "b", uses_alias=True)
        assert sel_direct.get_selectors() == [sel_direct]
        assert sel_aliased.get_selectors() == [sel_aliased]
        assert sel_direct.uses_alias is False
        assert sel_aliased.uses_alias is True

    def test_op_returns_all_leaf_selectors(self):
        sel_a = TimeSeriesSelector(TagSelector("name") == "a")
        sel_b = TimeSeriesSelector(TagSelector("name") == "b")
        op = sel_a + sel_b
        result = op.get_selectors()
        assert len(result) == 2
        assert sel_a in result
        assert sel_b in result

    def test_nested_op_returns_all_leaves(self):
        sel_a = TimeSeriesSelector(TagSelector("name") == "a")
        sel_b = TimeSeriesSelector(TagSelector("name") == "b")
        nested = (sel_a + sel_b).mean()
        result = nested.get_selectors()
        assert len(result) == 2
        assert sel_a in result
        assert sel_b in result

    def test_op_with_scalar_ignores_non_expressions(self):
        sel = TimeSeriesSelector(TagSelector("name") == "x")
        op = sel + 5
        result = op.get_selectors()
        assert result == [sel]

    def test_alias_selector_returns_all_aliases(self):
        sel_a = TimeSeriesSelector(TagSelector("name") == "a")
        sel_b = TimeSeriesSelector(TagSelector("name") == "b")
        alias_sel = TimeSeriesAliasSelector(sel_a, sel_b)
        result = alias_sel.get_selectors()
        assert len(result) == 2
        assert sel_a in result
        assert sel_b in result

    def test_udf_returns_leaf_selectors(self):
        sel = TimeSeriesSelector(TagSelector("name") == "test")
        udf_expr = sel.apply(lambda ts: ts * 2)
        result = udf_expr.get_selectors()
        assert result == [sel]

    def test_mixed_uses_alias_all_returned(self):
        sel_direct = TimeSeriesSelector(TagSelector("name") == "a")
        sel_aliased = TimeSeriesSelector(TagSelector("alias") == "b", uses_alias=True)
        op = sel_direct + sel_aliased
        result = op.get_selectors()
        assert len(result) == 2
        assert sel_direct in result
        assert sel_aliased in result

    def test_duplicate_selector_in_expression(self):
        sel = TimeSeriesSelector(TagSelector("name") == "x")
        op = sel.where(sel > 1)
        result = op.get_selectors()
        assert len(result) == 2
        assert all(s is sel for s in result)
