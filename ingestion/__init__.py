"""
NEXUS Ingestion Framework — TPC-DS SF=1 only.

Architecture:
├── base/        - Shared infrastructure (HTTP client, core classes, contracts)
├── batch/       - Batch processing (REST API, CSV, CSV download, Parquet)
└── data_caterer/ - TPC-DS data generation via Data Caterer
"""

from ingestion.base.core import DownloadContext, SourceFailure, SourceRun
from ingestion.batch.common import write_jsonl
from ingestion.batch.parquet_ingestion import ingest_parquet, ingest_parquet_download

__all__ = [
    "DownloadContext",
    "SourceFailure",
    "SourceRun",
    "write_jsonl",
    "ingest_parquet",
    "ingest_parquet_download",
]

__version__ = "1.0.0"
