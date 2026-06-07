"""
NEXUS Ingestion Framework — TPC-DI (DIGen-based).

Architecture:
├── base/        - Shared infrastructure (HTTP client, core classes, contracts)
├── batch/       - Batch processing (REST API, CSV, CSV download, Parquet)
└── data_caterer/ - (deprecated) legacy Data Caterer integration
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
