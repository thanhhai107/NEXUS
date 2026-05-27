"""
Batch Ingestion Pipeline for NEXUS.

Provides batch processing capabilities:
- REST API ingestion with pagination
- CSV file ingestion
- CSV download from URLs
- Common utilities for writing raw data
"""

from ingestion.batch.api_ingestion import (
    batch_api_source_run,
    extract_records,
    ingest_api_records,
)
from ingestion.batch.common import (
    clean_col_name,
    read_csv_records,
    raw_dataset_dir,
    write_jsonl,
)

__all__ = [
    # API ingestion
    "batch_api_source_run",
    "extract_records",
    "ingest_api_records",
    # CSV utilities
    "clean_col_name",
    "read_csv_records",
    "raw_dataset_dir",
    "write_jsonl",
]
