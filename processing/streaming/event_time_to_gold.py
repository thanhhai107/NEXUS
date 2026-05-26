from __future__ import annotations

import argparse

from processing.common.idempotency import parse_key_list, write_idempotent_iceberg
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp, to_timestamp, window
from pyspark.sql.functions import sum as spark_sum


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("nexus-streaming-event-time-to-gold")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def run(
    *,
    silver_table: str,
    gold_table: str,
    checkpoint_location: str,
    event_time_column: str,
    group_by: list[str],
    metric_column: str,
    watermark_delay: str = "2 hours",
    window_duration: str = "1 hour",
    output_mode: str = "update",
    trigger_available_now: bool = False,
    processing_time: str | None = None,
    await_timeout_seconds: float | None = None,
    skip_overwrite_snapshots: bool = True,
    skip_delete_snapshots: bool = True,
) -> None:
    spark = build_spark()
    stream_reader = spark.readStream
    if skip_overwrite_snapshots:
        stream_reader = stream_reader.option("streaming-skip-overwrite-snapshots", "true")
    if skip_delete_snapshots:
        stream_reader = stream_reader.option("streaming-skip-delete-snapshots", "true")
    silver_stream = stream_reader.table(silver_table)
    event_stream = (
        silver_stream.withColumn("_nexus_event_timestamp", to_timestamp(col(event_time_column)))
        .withWatermark("_nexus_event_timestamp", watermark_delay)
    )

    gold_stream = (
        event_stream.groupBy(
            window(col("_nexus_event_timestamp"), window_duration).alias("event_window"),
            *[col(column) for column in group_by],
        )
        .agg(
            count("*").alias("record_count"),
            spark_sum(col(metric_column).cast("double")).alias(f"total_{metric_column}"),
        )
        .select(
            col("event_window.start").alias("window_start"),
            col("event_window.end").alias("window_end"),
            *[col(column) for column in group_by],
            "record_count",
            f"total_{metric_column}",
        )
        .withColumn("_nexus_gold_loaded_at", current_timestamp())
    )

    merge_keys = ["window_start", "window_end", *group_by]

    def write_batch(batch_df, batch_id: int) -> None:
        write_idempotent_iceberg(
            batch_df.sparkSession,
            batch_df,
            gold_table,
            preferred_keys=merge_keys,
            mode="merge",
        )

    writer = (
        gold_stream.writeStream.outputMode(output_mode)
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(write_batch)
    )
    if trigger_available_now:
        writer = writer.trigger(availableNow=True)
    elif processing_time:
        writer = writer.trigger(processingTime=processing_time)

    query = writer.start()
    try:
        if await_timeout_seconds is None:
            query.awaitTermination()
        elif not query.awaitTermination(await_timeout_seconds):
            query.stop()
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate Silver streaming records into Gold using event time and watermark."
    )
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--gold-table", required=True)
    parser.add_argument("--checkpoint-location", required=True)
    parser.add_argument("--event-time-column", required=True)
    parser.add_argument("--group-by", required=True, help="Comma-separated dimensions.")
    parser.add_argument("--metric-column", required=True)
    parser.add_argument("--watermark-delay", default="2 hours")
    parser.add_argument("--window-duration", default="1 hour")
    parser.add_argument("--output-mode", choices=["append", "update", "complete"], default="update")
    parser.add_argument(
        "--trigger-available-now",
        action="store_true",
        help="Process all currently available streaming input, then stop.",
    )
    parser.add_argument("--processing-time", help="Optional processing-time trigger, for example '1 minute'.")
    parser.add_argument("--await-timeout-seconds", type=float, help="Stop the query if it has not ended in time.")
    parser.add_argument(
        "--process-overwrite-snapshots",
        dest="skip_overwrite_snapshots",
        action="store_false",
        default=True,
        help="Fail on Iceberg overwrite snapshots instead of skipping them.",
    )
    parser.add_argument(
        "--process-delete-snapshots",
        dest="skip_delete_snapshots",
        action="store_false",
        default=True,
        help="Fail on Iceberg delete snapshots instead of skipping them.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        silver_table=args.silver_table,
        gold_table=args.gold_table,
        checkpoint_location=args.checkpoint_location,
        event_time_column=args.event_time_column,
        group_by=parse_key_list(args.group_by),
        metric_column=args.metric_column,
        watermark_delay=args.watermark_delay,
        window_duration=args.window_duration,
        output_mode=args.output_mode,
        trigger_available_now=args.trigger_available_now,
        processing_time=args.processing_time,
        await_timeout_seconds=args.await_timeout_seconds,
        skip_overwrite_snapshots=args.skip_overwrite_snapshots,
        skip_delete_snapshots=args.skip_delete_snapshots,
    )
