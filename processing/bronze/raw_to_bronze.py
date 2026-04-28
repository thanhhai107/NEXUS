from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("nexus-raw-to-bronze")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def run(raw_path: str, bronze_table: str) -> None:
    """Convert raw JSONL envelopes into a Bronze Iceberg table.

    Bronze keeps source payloads plus ingestion metadata for traceability.
    """
    spark = build_spark()
    raw_df = spark.read.json(raw_path)

    bronze_df = (
        raw_df.withColumn("payload_struct", col("payload"))
        .withColumn("_nexus_bronze_loaded_at", current_timestamp())
    )

    bronze_df.writeTo(bronze_table).using("iceberg").createOrReplace()
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load raw JSONL data into Bronze Iceberg.")
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--bronze-table", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(raw_path=args.raw_path, bronze_table=args.bronze_table)
