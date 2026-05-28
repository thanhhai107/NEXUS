"""
Dataclasses for NEXUS ingestion contracts.

Provides type-safe contracts for:
- Source specifications
- Retry, timeout, rate-limit policies
- Chunk and run manifests
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# Chunk status constants
CHUNK_SUCCESS = "success"
CHUNK_FAILED = "failed"
CHUNK_SKIPPED = "skipped"

# Coverage status constants
COVERAGE_COMPLETE = "complete"
COVERAGE_PARTIAL = "partial"
COVERAGE_FAILED = "failed"

# Publish status constants
PUBLISH_UNPUBLISHED = "unpublished"
PUBLISH_PUBLISHED = "published"
PUBLISH_WITH_WARNING = "published_with_warning"


class RetryStyle(Enum):
    NONE = "none"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 5
    retryable_status_codes: tuple[int, ...] = (429, 500, 502, 503, 504)
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    jitter_seconds: float = 0.5
    retry_style: RetryStyle = RetryStyle.EXPONENTIAL


@dataclass(frozen=True)
class TimeoutPolicy:
    connect_timeout_seconds: float = 10.0
    read_timeout_seconds: float = 60.0
    total_timeout_seconds: float | None = None

    @property
    def requests_timeout(self) -> tuple[float, float]:
        """Return (connect_timeout, read_timeout) for requests library."""
        return (self.connect_timeout_seconds, self.read_timeout_seconds)


@dataclass(frozen=True)
class RateLimitPolicy:
    delay_seconds: float = 0.0
    min_delay_on_429_seconds: float = 2.0
    max_concurrency: int = 1


@dataclass(frozen=True)
class CoveragePolicy:
    min_success_ratio: float = 1.0
    allow_publish_with_warnings: bool = False
    required_chunks: tuple[str, ...] = field(default_factory=tuple)


@dataclass
class SourceSpec:
    source_id: str
    source_key: str
    source_name: str
    dataset_name: str
    api_url_template: str | None = None
    auth_style: str = "bearer"
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    timeout_policy: TimeoutPolicy = field(default_factory=TimeoutPolicy)
    rate_limit_policy: RateLimitPolicy = field(default_factory=RateLimitPolicy)
    # Extended fields for downloader
    description: str | None = None
    func: Any = None
    required_env: tuple[str, ...] = ()
    realtime: bool = False


@dataclass
class LogicalWindow:
    start: str
    end: str
    chunk_key: str = ""


@dataclass
class DownloadChunk:
    chunk_id: str
    relative_path: str
    logical_window: LogicalWindow
    required: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DownloadPlan:
    chunks: list[DownloadChunk] = field(default_factory=list)
    total_records: int = 0


@dataclass
class ChunkResult:
    chunk_id: str
    status: str
    required: bool = True
    paths: tuple[str, ...] = field(default_factory=tuple)
    checksums: dict[str, str] = field(default_factory=dict)
    record_count: int = 0
    quarantine_count: int = 0
    attempts: int = 1
    error: str | None = None
    first_attempt_at: str | None = None
    finished_at: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class RunManifest:
    source_id: str
    dataset_name: str
    run_id: str
    expected_chunks: int = 0
    successful_chunks: int = 0
    failed_chunks: int = 0
    skipped_chunks: int = 0
    coverage_ratio: float = 0.0
    coverage_status: str = COVERAGE_COMPLETE
    publish_status: str = PUBLISH_UNPUBLISHED
    chunks: tuple[ChunkResult, ...] = field(default_factory=tuple)
    started_at: str = ""
    updated_at: str = ""
    finished_at: str | None = None
    raw_dir: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    downstream_raw_path: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "dataset_name": self.dataset_name,
            "run_id": self.run_id,
            "expected_chunks": self.expected_chunks,
            "successful_chunks": self.successful_chunks,
            "failed_chunks": self.failed_chunks,
            "skipped_chunks": self.skipped_chunks,
            "coverage_ratio": self.coverage_ratio,
            "coverage_status": self.coverage_status,
            "publish_status": self.publish_status,
            "chunks": [
                {
                    "chunk_id": c.chunk_id,
                    "status": c.status,
                    "required": c.required,
                    "paths": list(c.paths),
                    "checksums": c.checksums,
                    "record_count": c.record_count,
                    "quarantine_count": c.quarantine_count,
                    "attempts": c.attempts,
                    "error": c.error,
                    "first_attempt_at": c.first_attempt_at,
                    "finished_at": c.finished_at,
                }
                for c in self.chunks
            ],
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "finished_at": self.finished_at,
            "raw_dir": self.raw_dir,
            "details": self.details,
            "downstream_raw_path": self.downstream_raw_path,
        }


@dataclass
class PublishedManifest:
    source_id: str
    dataset_name: str
    run_id: str
    published_at: str
    coverage_status: str
    publish_status: str
    chunks: tuple[ChunkResult, ...]
    raw_dir: str
    source_key: str
    downstream_raw_path: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "dataset_name": self.dataset_name,
            "run_id": self.run_id,
            "published_at": self.published_at,
            "coverage_status": self.coverage_status,
            "publish_status": self.publish_status,
            "chunks": [
                {
                    "chunk_id": c.chunk_id,
                    "status": c.status,
                    "required": c.required,
                    "paths": list(c.paths),
                    "checksums": c.checksums,
                    "record_count": c.record_count,
                }
                for c in self.chunks
            ],
            "raw_dir": self.raw_dir,
            "source_key": self.source_key,
            "downstream_raw_path": self.downstream_raw_path,
        }
