"""
NEXUS Ingestion Framework

Architecture:
├── base/        - Shared infrastructure (HTTP client, core classes, contracts)
├── batch/       - Batch processing (REST API, CSV, CSV download, Parquet)
├── streaming/   - Streaming (Kafka, API polling, GTFS Realtime)
├── sources/     - Source adapters (London data sources)
└── canonical/  - Data contracts (envelope format, writer)
"""

from ingestion.base.core import DownloadContext, SourceFailure, SourceRun
from ingestion.batch.common import write_jsonl
from ingestion.batch.parquet_ingestion import ingest_parquet, ingest_parquet_download
from ingestion.streaming import (
    KafkaConfig,
    ProducerConfig,
    ConsumerConfig,
    StreamSourceConfig,
    STREAM_TOPICS,
    run_producer,
    consume_events,
)
from ingestion.streaming.api_stream import ApiStreamConfig, run_api_stream

__all__ = [
    # Base
    "DownloadContext",
    "SourceFailure",
    "SourceRun",
    "write_jsonl",
    # Batch
    "ingest_parquet",
    "ingest_parquet_download",
    # Streaming
    "KafkaConfig",
    "ProducerConfig",
    "ConsumerConfig",
    "StreamSourceConfig",
    "STREAM_TOPICS",
    "run_producer",
    "consume_events",
    "ApiStreamConfig",
    "run_api_stream",
]

__version__ = "1.0.0"
