"""Unit tests for ``constant_map_lookup``."""

import pyspark.sql.types as T

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


def test_empty_mapping_is_typed_null(spark):
    rows = _lookup(spark, ["a"], {}, T.IntegerType())
    assert rows.schema["v"].dataType == T.IntegerType()
    assert rows.collect()[0]["v"] is None


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
