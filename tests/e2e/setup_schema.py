"""e2e step: create the schema the report writes to."""

import argparse

from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", required=True)
parser.add_argument("--schema", required=True)
args = parser.parse_args()

SparkSession.builder.getOrCreate().sql(f"CREATE SCHEMA `{args.catalog}`.`{args.schema}`")
