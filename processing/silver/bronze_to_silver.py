from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, trim


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("nexus-bronze-to-silver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def run(bronze_table: str, silver_table: str) -> None:
    """Standardize Bronze records into a cleaner Silver table.

    Replace the sample flattening logic with dataset-specific normalization rules.
    """
    spark = build_spark()
    bronze_df = spark.table(bronze_table)

    payload_columns = [
        col(f"payload_struct.{field.name}").alias(field.name)
        for field in bronze_df.schema["payload_struct"].dataType.fields
    ]

    silver_df = bronze_df.select(*payload_columns, "_nexus_source", "_nexus_ingested_at")

    for field in silver_df.schema.fields:
        if field.dataType.simpleString() == "string":
            silver_df = silver_df.withColumn(field.name, trim(col(field.name)))

    silver_df = silver_df.dropDuplicates().withColumn("_nexus_silver_loaded_at", current_timestamp())
    silver_df.writeTo(silver_table).using("iceberg").createOrReplace()
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean Bronze data into Silver Iceberg.")
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--silver-table", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(bronze_table=args.bronze_table, silver_table=args.silver_table)
