"""Constant-map lookups for mapping report entity names to per-row metadata."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import pyspark.sql.functions as f
from pyspark.sql import Column
from pyspark.sql.types import DataType, MapType, StringType


def constant_map_lookup(
    mapping: Mapping[str, Any], key: Column | str, value_type: DataType
) -> Column:
    """
    Look up ``key`` in a constant ``{str: value}`` mapping.

    The mapping is serialised to JSON once and parsed with ``from_json`` from a
    string literal. The optimizer constant-folds that into a single map literal,
    so the column costs a fixed number of driver calls no matter how many entries
    the mapping has, and each row does one map lookup.

    Parameters
    ----------
    mapping : Mapping of str to Any
        The constant mapping. Values must be JSON-serialisable (str, int, bool,
        None, lists, or nested dicts with string keys) and match ``value_type``.
    key : pyspark.sql.Column or str
        The column (or column name) holding the lookup key.
    value_type : pyspark.sql.types.DataType
        Spark type of the mapping's values.

    Returns
    -------
    pyspark.sql.Column
        The value for ``key``, or NULL when ``key`` is not in the mapping (or the
        mapping is empty).
    """
    if not mapping:
        return f.lit(None).cast(value_type)
    key_col = f.col(key) if isinstance(key, str) else key
    map_type = MapType(StringType(), value_type)
    # ``[...]`` (GetMapValue) returns NULL for a missing key; ``element_at`` would
    # raise under ANSI mode.
    return f.from_json(f.lit(json.dumps(dict(mapping))), map_type)[key_col]
