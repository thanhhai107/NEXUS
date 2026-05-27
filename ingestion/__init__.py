"""
NEXUS Ingestion Framework

Architecture:
├── base/        - Shared infrastructure (HTTP client, core classes, contracts)
├── batch/       - Batch processing (REST API, CSV, CSV download)
├── streaming/  - Kafka streaming (real-time data pipeline)
├── sources/     - Source adapters (London data sources)
└── canonical/  - Data contracts (envelope format, writer)
"""

from ingestion.base.core import DownloadContext, SourceFailure, SourceRun
from ingestion.canonical.writer import write_jsonl
from ingestion.streaming import (
    KafkaConfig,
    ProducerConfig,
    ConsumerConfig,
    StreamSourceConfig,
    STREAM_TOPICS,
    run_producer,
    consume_events,
)

__all__ = [
    # Base
    "DownloadContext",
    "SourceFailure",
    "SourceRun",
    "write_jsonl",
    # Streaming
    "KafkaConfig",
    "ProducerConfig",
    "ConsumerConfig",
    "StreamSourceConfig",
    "STREAM_TOPICS",
    "run_producer",
    "consume_events",
]

__version__ = "1.0.0"
