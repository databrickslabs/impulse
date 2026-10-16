"""e2e step: drop the test schema."""

import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
args = parser.parse_args()

if not args.schema.startswith("e2e_"):
    raise ValueError(f"Refusing to drop non-e2e schema {args.schema!r}")
SparkSession.builder.getOrCreate().sql(
    f"DROP SCHEMA IF EXISTS `{args.catalog}`.`{args.schema}` CASCADE"
)
