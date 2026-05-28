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
        SparkSession.builder.appName("nexus-bronze-to-silver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def run(
    bronze_table: str,
    silver_table: str,
    *,
    dataset: str | None = None,
    dedup_keys: list[str] | None = None,
    write_mode: str = "merge",
) -> None:
    """Standardize Bronze records into a cleaner Silver table.

    Replace the sample flattening logic with dataset-specific normalization rules.
    """
    from pyspark.sql.functions import col, current_timestamp, trim

    spark = build_spark()
    bronze_df = spark.table(bronze_table)

    payload_columns = [
        col(f"payload_struct.{field.name}").alias(field.name)
        for field in bronze_df.schema["payload_struct"].dataType.fields
    ]
    metadata_columns = [
        column for column in (
            "_nexus_record_id",
            "_nexus_event_time",
            "_nexus_ingested_at",
            "_nexus_run_id",
            "_nexus_chunk_id",
            "_nexus_source",
            "_nexus_dataset",
        )
        if column in bronze_df.columns
    ]

    silver_df = bronze_df.select(*payload_columns, *metadata_columns)

    for field in silver_df.schema.fields:
        if field.dataType.simpleString() == "string":
            silver_df = silver_df.withColumn(field.name, trim(col(field.name)))

    if dataset and not dedup_keys:
        from common.data_contract import load_data_contract

        dedup_keys = list(load_data_contract(dataset).semantic_dedup_keys)
    silver_df = silver_df.withColumn("_nexus_silver_loaded_at", current_timestamp())
    write_idempotent_iceberg(
        spark,
        silver_df,
        silver_table,
        preferred_keys=dedup_keys or ["_nexus_record_id"],
        mode=write_mode,
    )
    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clean Bronze data into Silver Iceberg.")
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--dataset", help="Dataset name used to load semantic dedup keys from the data contract.")
    parser.add_argument("--dedup-keys", help="Comma-separated semantic dedup keys. Overrides --dataset.")
    parser.add_argument("--write-mode", choices=["merge", "replace"], default="merge")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        bronze_table=args.bronze_table,
        silver_table=args.silver_table,
        dataset=args.dataset,
        dedup_keys=parse_key_list(args.dedup_keys),
        write_mode=args.write_mode,
    )
