"""Unit tests for ``constant_map_lookup``."""

import pyspark.sql.types as T
import pytest

from impulse_reporting.util.constant_lookup import constant_map_lookup


def _lookup(spark, keys, mapping, value_type):
    df = spark.createDataFrame([(k,) for k in keys], "name string")
    return df.select(constant_map_lookup(mapping, "name", value_type).alias("v"))


def test_int_values_and_missing_key(spark):
    rows = _lookup(spark, ["a", "b", "zz"], {"a": 1, "b": 2_147_483_647}, T.IntegerType())
    assert [r["v"] for r in rows.collect()] == [1, 2_147_483_647, None]


def test_string_array_and_nested_map_values(spark):
    df = spark.createDataFrame([("a", "x"), ("b", "y"), ("c", "x")], "name string, label string")
    names = constant_map_lookup({"a": "ev_a", "b": None}, "name", T.StringType())
    arrays = constant_map_lookup({"a": ["c0", "c1"]}, "name", T.ArrayType(T.StringType()))
    nested = constant_map_lookup(
        {"a": {"x": "ax"}, "c": {"y": "cy"}},
        "name",
        T.MapType(T.StringType(), T.StringType()),
    )
    rows = df.select(
        names.alias("name_v"),
        arrays.alias("array_v"),
        nested[df["label"]].alias("nested_v"),
    ).collect()
    assert [r["name_v"] for r in rows] == ["ev_a", None, None]
    assert [r["array_v"] for r in rows] == [["c0", "c1"], None, None]
    assert [r["nested_v"] for r in rows] == ["ax", None, None]


@pytest.mark.parametrize("mapping", [{}, {None: 1}], ids=["empty", "only_none_key"])
def test_empty_mapping_is_typed_null(spark, mapping):
    rows = _lookup(spark, ["a", "null"], mapping, T.IntegerType())
    assert rows.schema["v"].dataType == T.IntegerType()
    assert [r["v"] for r in rows.collect()] == [None, None]


def test_special_character_keys_and_values(spark):
    keys = ['quo"te', "back\\slash", "umlaut äöü", "emoji \U0001f697", "it's"]
    mapping = {k: f"value of {k}" for k in keys}
    rows = _lookup(spark, keys, mapping, T.StringType()).collect()
    assert [r["v"] for r in rows] == [mapping[k] for k in keys]


def test_lookup_is_constant_folded(spark):
    """The JSON literal must be folded into a map literal, not parsed per row."""
    mapping = {f"agg_{i}": i for i in range(300)}
    df = _lookup(spark, ["agg_7"], mapping, T.IntegerType())
    plan = df._jdf.queryExecution().optimizedPlan().toString().lower()
    assert "from_json" not in plan
    assert "jsontostructs" not in plan
    assert df.collect()[0]["v"] == 7


def test_none_key_is_skipped(spark):
    """A ``None`` key must not become the JSON key ``"null"`` and match that string."""
    rows = _lookup(spark, ["null", "a"], {None: 1, "a": 2}, T.IntegerType())
    assert [r["v"] for r in rows.collect()] == [None, 2]


@pytest.mark.parametrize("bad_value", [2**31, "7"])
def test_value_type_mismatch_fails_instead_of_nulling_the_map(spark, bad_value):
    """A value that does not fit ``value_type`` must raise, not silently null every row."""
    df = _lookup(spark, ["a", "b"], {"a": 1, "b": bad_value}, T.IntegerType())
    with pytest.raises(Exception, match="(?i)malformed|failfast"):
        df.collect()
