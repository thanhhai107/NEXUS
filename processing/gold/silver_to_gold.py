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
    dimensions: str | None = None,
    metrics: str | None = None,
) -> None:
    """Create a Gold analytical aggregate.

    Supports:
    - Simple GROUP BY on dimensions
    - Window-based aggregation with watermark for late data
    - Multiple metrics (COUNT, SUM, AVG, MIN, MAX)
    - Configurable dimensions via --dimensions (comma-separated)
    - Configurable metrics via --metrics (comma-separated: type:column)

    Dimensions default to --group-by value. Metrics default to count(*) + sum(--metric-column).
    """
    from pyspark.sql.functions import (
        avg, col, count, current_timestamp, max as spark_max,
        min as spark_min, to_timestamp, window,
    )
    from pyspark.sql.functions import sum as spark_sum

    METRIC_FUNCTIONS = {
        "count": lambda c: count("*").alias("record_count"),
        "sum": lambda c: spark_sum(col(c).cast("double")).alias(f"total_{c}"),
        "avg": lambda c: avg(col(c).cast("double")).alias(f"avg_{c}"),
        "min": lambda c: spark_min(col(c).cast("double")).alias(f"min_{c}"),
        "max": lambda c: spark_max(col(c).cast("double")).alias(f"max_{c}"),
    }

    spark = build_spark()
    silver_df = spark.table(silver_table)

    group_columns = parse_key_list(dimensions or group_by)
    metric_specs = _parse_metrics(metrics, metric_column)

    if event_time_column:
        event_df = silver_df.withColumn("_nexus_event_timestamp", to_timestamp(col(event_time_column)))
        if watermark_delay:
            event_df = event_df.withWatermark("_nexus_event_timestamp", watermark_delay)
        grouped = event_df.groupBy(
            window(col("_nexus_event_timestamp"), window_duration).alias("event_window"),
            *[col(column) for column in group_columns],
        )
        agg_exprs = []
        for metric_type, metric_col in metric_specs:
            fn = METRIC_FUNCTIONS.get(metric_type)
            if fn:
                agg_exprs.append(fn(metric_col))
        gold_df = grouped.agg(*agg_exprs).select(
            col("event_window.start").alias("window_start"),
            col("event_window.end").alias("window_end"),
            *[col(column) for column in group_columns],
            *[c for c in gold_df.columns if c not in ["window_start", "window_end"] + group_columns],
        )
        merge_keys = ["window_start", "window_end", *group_columns]
    else:
        gold_df = silver_df.groupBy(*group_columns).agg(
            *[METRIC_FUNCTIONS.get(mt, lambda c: count("*").alias("record_count"))(mc)
              for mt, mc in metric_specs]
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


def _parse_metrics(metrics_str: str | None, fallback_column: str) -> list[tuple[str, str]]:
    if not metrics_str:
        return [("count", "*"), ("sum", fallback_column)]
    result: list[tuple[str, str]] = []
    for spec in metrics_str.split(","):
        spec = spec.strip()
        if ":" in spec:
            metric_type, metric_col = spec.split(":", 1)
            result.append((metric_type.strip(), metric_col.strip()))
        else:
            result.append(("count", "*"))
    if not result:
        result = [("count", "*"), ("sum", fallback_column)]
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate Silver data into Gold Iceberg.")
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--gold-table", required=True)
    parser.add_argument("--group-by", default="department")
    parser.add_argument("--metric-column", default="amount")
    parser.add_argument("--dimensions", help="Comma-separated dimension columns (overrides --group-by)")
    parser.add_argument("--metrics", help="Comma-separated metric specs: count:*,sum:amount,avg:temperature")
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
        dimensions=args.dimensions,
        metrics=args.metrics,
        event_time_column=args.event_time_column,
        watermark_delay=args.watermark_delay,
        window_duration=args.window_duration,
        write_mode=args.write_mode,
    )
