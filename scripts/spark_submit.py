#!/usr/bin/env python3
"""Spark cluster submission script for NEXUS processing.

Submits Spark jobs to a remote cluster (standalone, YARN, or K8s).

Usage:
    python scripts/spark_submit.py bronze --raw-path s3a://bucket/raw/tpcds_store_sales
    python scripts/spark_submit.py silver --bronze-table nexus.bronze.tpcds_store_sales
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(description="Submit NEXUS Spark jobs to cluster")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Bronze: raw to bronze
    bronze_parser = subparsers.add_parser("bronze", help="Run raw-to-bronze processing")
    bronze_parser.add_argument("--raw-path", required=True, help="S3A path to raw data")
    bronze_parser.add_argument("--bronze-table", required=True, help="Target bronze table name")
    bronze_parser.add_argument("--dedup-keys", default="_nexus_record_id", help="Comma-separated dedup keys")
    bronze_parser.add_argument("--write-mode", default="merge", choices=["merge", "replace"])

    # Silver: bronze to silver
    silver_parser = subparsers.add_parser("silver", help="Run bronze-to-silver processing")
    silver_parser.add_argument("--bronze-table", required=True, help="Source bronze table")
    silver_parser.add_argument("--silver-table", required=True, help="Target silver table")
    silver_parser.add_argument("--dataset", help="Dataset name")
    silver_parser.add_argument("--dedup-keys", help="Comma-separated dedup keys")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Import and run
    if args.command == "bronze":
        from processing.bronze.raw_to_bronze import run as bronze_run
        from processing.common.idempotency import parse_key_list

        bronze_run(
            raw_path=args.raw_path,
            bronze_table=args.bronze_table,
            dedup_keys=parse_key_list(args.dedup_keys),
            write_mode=args.write_mode,
        )

    elif args.command == "silver":
        from processing.silver.bronze_to_silver import run as silver_run
        from processing.common.idempotency import parse_key_list

        silver_run(
            bronze_table=args.bronze_table,
            silver_table=args.silver_table,
            dataset=args.dataset,
            dedup_keys=parse_key_list(args.dedup_keys) if args.dedup_keys else None,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
