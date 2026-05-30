from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from processing.common.idempotency import parse_key_list, write_idempotent_iceberg


def build_spark():
    from pyspark.sql import SparkSession
    from common.config import get_spark_master_url, get_spark_config

    master = get_spark_master_url()
    spark_cfg = get_spark_config()

    builder = SparkSession.builder.appName("nexus-raw-to-bronze")

    # Set master URL
    if master.startswith("spark://") or master.startswith("k8s://"):
        builder = builder.master(master)

    # Apply configs
    for key, value in spark_cfg.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def run(
    raw_path: str,
    bronze_table: str,
    *,
    dedup_keys: list[str] | None = None,
    write_mode: str = "merge",
) -> None:
    """Convert raw JSONL envelopes into a Bronze Iceberg table.

    Bronze keeps source payloads plus ingestion metadata for traceability.
    """
    from pyspark.sql.functions import col, current_timestamp

    spark = build_spark()
    raw_df = spark.read.json(raw_path)

    bronze_df = (
        raw_df.withColumn("payload_struct", col("payload"))
        .withColumn("_nexus_bronze_loaded_at", current_timestamp())
    )

    write_idempotent_iceberg(
        spark,
        bronze_df,
        bronze_table,
        preferred_keys=dedup_keys or ["_nexus_record_id"],
        mode=write_mode,
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load raw JSONL data into Bronze Iceberg.")
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument(
        "--dedup-keys",
        default="_nexus_record_id",
        help="Comma-separated keys for idempotent Bronze MERGE.",
    )
    parser.add_argument("--write-mode", choices=["merge", "replace"], default="merge")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        raw_path=args.raw_path,
        bronze_table=args.bronze_table,
        dedup_keys=parse_key_list(args.dedup_keys),
        write_mode=args.write_mode,
    )
