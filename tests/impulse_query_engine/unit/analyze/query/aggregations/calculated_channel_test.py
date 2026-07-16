# pylint: disable=missing-function-docstring
"""Unit tests for the CalculatedChannel aggregation class.

Covers construction (identity storage, the `_alias` rule, the channel_id
sentinel), the `canonical_identity` encoding, delegation of the expression
interface to the wrapped expression, and validation.
"""

import pyspark.sql.types as T
import pytest

from impulse_query_engine.analyze.metadata.time_series_expression import (
    TimeSeriesExpression,
)
from impulse_query_engine.analyze.query.aggregations.aggregation import Aggregation
from impulse_query_engine.analyze.query.aggregations.calculated_channel import (
    _AUTO,
    CalculatedChannel,
)
from impulse_query_engine.model.series.sample_series import SampleSeries


class _StubExpr(TimeSeriesExpression):
    """Minimal TimeSeriesExpression recording delegation and returning a series."""

    def __init__(self, series=None):
        super().__init__()
        self._series = series if series is not None else SampleSeries([0], [100], [1.0])
        self._selectors = ["sel"]

    def build(self, cache):
        return self._series

    def get_selectors(self):
        return self._selectors

    def get_selector_expr(self):
        return "selector_expr"

    def get_required_tag_exprs(self):
        return {"tag_expr"}

    def required_tags(self):
        return {"tag"}

    def __str__(self):
        return "<StubExpr>"


class TestConstruction:
    def test_is_aggregation(self):
        cc = CalculatedChannel(_StubExpr(), {"channel_name": "speed_kmh"})
        assert isinstance(cc, Aggregation)
        assert isinstance(cc, TimeSeriesExpression)

    def test_identity_stored(self):
        cc = CalculatedChannel(_StubExpr(), {"channel_name": "speed_kmh", "data_key": "CALC"})
        assert cc.identity == {"channel_name": "speed_kmh", "data_key": "CALC"}

    def test_alias_concatenates_identity_values(self):
        cc = CalculatedChannel(_StubExpr(), {"channel_name": "Eng_RPM", "data_key": "TM"})
        assert cc._alias == "Eng_RPM::TM"

    def test_alias_single_identity(self):
        cc = CalculatedChannel(_StubExpr(), {"channel_name": "speed_kmh"})
        assert cc._alias == "speed_kmh"

    def test_explicit_alias_overrides(self):
        cc = CalculatedChannel(_StubExpr(), {"channel_name": "speed_kmh"})
        cc.alias("renamed")
        assert cc._alias == "renamed"

    def test_channel_id_defaults_to_auto_sentinel(self):
        cc = CalculatedChannel(_StubExpr(), {"channel_name": "x"})
        assert cc._explicit_channel_id is _AUTO

    def test_explicit_channel_id_stored(self):
        cc = CalculatedChannel(_StubExpr(), {"channel_name": "x"}, channel_id=999)
        assert cc._explicit_channel_id == 999

    def test_explicit_none_channel_id_stored(self):
        cc = CalculatedChannel(_StubExpr(), {"channel_name": "x"}, channel_id=None)
        assert cc._explicit_channel_id is None
        assert cc._explicit_channel_id is not _AUTO

    def test_empty_identity_raises(self):
        with pytest.raises(ValueError, match="non-empty identity"):
            CalculatedChannel(_StubExpr(), {})


class TestCanonicalIdentity:
    def test_sorted_and_stable(self):
        cc1 = CalculatedChannel(_StubExpr(), {"channel_name": "s", "data_key": "CALC"})
        cc2 = CalculatedChannel(_StubExpr(), {"data_key": "CALC", "channel_name": "s"})
        assert cc1.canonical_identity() == "channel_name=s&data_key=CALC"
        # Order-independent: same identity, different key order → same encoding.
        assert cc1.canonical_identity() == cc2.canonical_identity()

    def test_distinct_identities_differ(self):
        a = CalculatedChannel(_StubExpr(), {"channel_name": "a"}).canonical_identity()
        b = CalculatedChannel(_StubExpr(), {"channel_name": "b"}).canonical_identity()
        assert a != b


class TestDelegation:
    def test_build_returns_wrapped_series(self):
        series = SampleSeries([0, 100], [100, 200], [3.0, 4.0])
        cc = CalculatedChannel(_StubExpr(series), {"channel_name": "x"})
        assert cc.build(cache=None) is series

    def test_get_selectors_delegates(self):
        expr = _StubExpr()
        cc = CalculatedChannel(expr, {"channel_name": "x"})
        assert cc.get_selectors() == expr._selectors

    def test_selector_interface_delegates(self):
        cc = CalculatedChannel(_StubExpr(), {"channel_name": "x"})
        assert cc.get_selector_expr() == "selector_expr"
        assert cc.get_required_tag_exprs() == {"tag_expr"}
        assert cc.required_tags() == {"tag"}

    def test_dtype_is_double(self):
        cc = CalculatedChannel(_StubExpr(), {"channel_name": "x"})
        assert cc.dtype() == T.DoubleType()

    def test_evaluation_type_is_sample_series(self):
        cc = CalculatedChannel(_StubExpr(), {"channel_name": "x"})
        assert cc.evaluation_type() is SampleSeries
