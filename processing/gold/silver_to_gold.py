from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from processing.common.idempotency import parse_key_list, write_idempotent_iceberg


def build_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("nexus-silver-to-gold")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def run(
    silver_table: str,
    gold_table: str,
    group_by: str = "department",
    metric_column: str = "amount",
    *,
    event_time_column: str | None = None,
    watermark_delay: str = "2 hours",
    window_duration: str = "1 hour",
    write_mode: str = "merge",
) -> None:
    """Create a small analytical Gold aggregate.

    This is intentionally generic; real projects should add one job/model per domain use case.
    """
    from pyspark.sql.functions import col, count, current_timestamp, to_timestamp, window
    from pyspark.sql.functions import sum as spark_sum

    spark = build_spark()
    silver_df = spark.table(silver_table)

    group_columns = parse_key_list(group_by)
    if event_time_column:
        event_df = silver_df.withColumn("_nexus_event_timestamp", to_timestamp(col(event_time_column)))
        if watermark_delay:
            event_df = event_df.withWatermark("_nexus_event_timestamp", watermark_delay)
        grouped = event_df.groupBy(
            window(col("_nexus_event_timestamp"), window_duration).alias("event_window"),
            *[col(column) for column in group_columns],
        )
        gold_df = grouped.agg(
            count("*").alias("record_count"),
            spark_sum(col(metric_column).cast("double")).alias(f"total_{metric_column}"),
        ).select(
            col("event_window.start").alias("window_start"),
            col("event_window.end").alias("window_end"),
            *[col(column) for column in group_columns],
            "record_count",
            f"total_{metric_column}",
        )
        merge_keys = ["window_start", "window_end", *group_columns]
    else:
        gold_df = silver_df.groupBy(*group_columns).agg(
            count("*").alias("record_count"),
            spark_sum(col(metric_column).cast("double")).alias(f"total_{metric_column}"),
        )
        merge_keys = group_columns

    gold_df = gold_df.withColumn("_nexus_gold_loaded_at", current_timestamp())
    write_idempotent_iceberg(
        spark,
        gold_df,
        gold_table,
        preferred_keys=merge_keys,
        mode=write_mode,
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate Silver data into Gold Iceberg.")
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--gold-table", required=True)
    parser.add_argument("--group-by", default="department")
    parser.add_argument("--metric-column", default="amount")
    parser.add_argument("--event-time-column", help="Enable event-time/window aggregation using this timestamp column.")
    parser.add_argument("--watermark-delay", default="2 hours")
    parser.add_argument("--window-duration", default="1 hour")
    parser.add_argument("--write-mode", choices=["merge", "replace"], default="merge")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        silver_table=args.silver_table,
        gold_table=args.gold_table,
        group_by=args.group_by,
        metric_column=args.metric_column,
        event_time_column=args.event_time_column,
        watermark_delay=args.watermark_delay,
        window_duration=args.window_duration,
        write_mode=args.write_mode,
    )
