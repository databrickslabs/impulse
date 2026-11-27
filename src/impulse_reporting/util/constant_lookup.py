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

    The mapping is parsed with ``from_json`` from one JSON string literal, which
    the optimizer constant-folds into a map literal. Building the column costs a
    fixed number of driver calls regardless of the mapping size (``create_map``
    would need one call per key and value). Per row, Spark scans the map's keys
    linearly.

    Parsing uses ``FAILFAST``: a value that does not match ``value_type`` raises
    when the plan is optimized instead of silently turning the whole map NULL.

    Parameters
    ----------
    mapping : Mapping of str to Any
        The constant mapping. Values must be JSON-serialisable (str, int, bool,
        None, lists, or nested dicts with string keys) and match ``value_type``.
        Entries whose key is ``None`` are skipped, since a NULL key never matches;
        other non-string keys are matched by their JSON string form.
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
    entries = {k: v for k, v in mapping.items() if k is not None}
    if not entries:
        return f.lit(None).cast(value_type)
    key_col = f.col(key) if isinstance(key, str) else key
    map_type = MapType(StringType(), value_type)
    lookup_map = f.from_json(f.lit(json.dumps(entries)), map_type, {"mode": "FAILFAST"})
    # ``[...]`` (GetMapValue) returns NULL for a missing key; ``element_at`` would
    # raise under ANSI mode.
    return lookup_map[key_col]
