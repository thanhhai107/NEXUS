from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp, sum as spark_sum


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("nexus-silver-to-gold")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def run(silver_table: str, gold_table: str, group_by: str = "department", metric_column: str = "amount") -> None:
    """Create a small analytical Gold aggregate.

    This is intentionally generic; real projects should add one job/model per domain use case.
    """
    spark = build_spark()
    silver_df = spark.table(silver_table)

    gold_df = (
        silver_df.groupBy(group_by)
        .agg(
            count("*").alias("record_count"),
            spark_sum(col(metric_column).cast("double")).alias(f"total_{metric_column}"),
        )
        .withColumn("_nexus_gold_loaded_at", current_timestamp())
    )

    gold_df.writeTo(gold_table).using("iceberg").createOrReplace()
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate Silver data into Gold Iceberg.")
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--gold-table", required=True)
    parser.add_argument("--group-by", default="department")
    parser.add_argument("--metric-column", default="amount")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        silver_table=args.silver_table,
        gold_table=args.gold_table,
        group_by=args.group_by,
        metric_column=args.metric_column,
    )
