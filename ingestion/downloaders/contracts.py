from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping


CHUNK_SUCCESS = "success"
CHUNK_FAILED = "failed"
CHUNK_SKIPPED = "skipped"

COVERAGE_COMPLETE = "complete"
COVERAGE_PARTIAL = "partial"
COVERAGE_FAILED = "failed"

PUBLISH_UNPUBLISHED = "unpublished"
PUBLISH_PUBLISHED = "published"
PUBLISH_WITH_WARNING = "published_with_warning"


@dataclass(frozen=True)
class SourceSpec:
    """Metadata contract for a source adapter.

    Existing adapters still receive ``SourceRun`` and ``DownloadContext``. The
    extra fields let the runtime stay config-first without hard-coding source
    behavior into shared execution logic.
    """

    key: str
    source_id: str
    description: str
    func: Callable[[Any, Any], None]
    dataset_name: str | None = None
    required_env: tuple[str, ...] = ()
    realtime: bool = False
    policies: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 6
    retryable_status_codes: tuple[int, ...] = (429, 500, 502, 503, 504)
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    jitter_seconds: float = 0.5


@dataclass(frozen=True)
class TimeoutPolicy:
    connect_timeout_seconds: float = 10.0
    read_timeout_seconds: float = 60.0
    total_timeout_seconds: float | None = None

    @property
    def requests_timeout(self) -> tuple[float, float]:
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
    required_chunks: tuple[str, ...] = ()


@dataclass(frozen=True)
class DownloadChunk:
    chunk_id: str
    required: bool = True
    logical_window: Mapping[str, Any] = field(default_factory=dict)
    relative_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "required": self.required,
            "logical_window": dict(self.logical_window),
            "relative_path": self.relative_path,
        }


@dataclass(frozen=True)
class DownloadPlan:
    source_id: str
    dataset_name: str
    run_id: str
    chunks: tuple[DownloadChunk, ...]
    policies: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "dataset_name": self.dataset_name,
            "run_id": self.run_id,
            "chunk_count": len(self.chunks),
            "chunks": [chunk.to_dict() for chunk in self.chunks],
            "policies": dict(self.policies),
        }


@dataclass(frozen=True)
class ChunkResult:
    chunk_id: str
    status: str
    required: bool = True
    paths: tuple[str, ...] = ()
    checksums: Mapping[str, str] = field(default_factory=dict)
    record_count: int = 0
    quarantine_count: int = 0
    attempts: int = 0
    error: str | None = None
    first_attempt_at: str | None = None
    finished_at: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "status": self.status,
            "required": self.required,
            "paths": list(self.paths),
            "checksums": dict(self.checksums),
            "record_count": self.record_count,
            "quarantine_count": self.quarantine_count,
            "attempts": self.attempts,
            "error": self.error,
            "first_attempt_at": self.first_attempt_at,
            "finished_at": self.finished_at,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class RunManifest:
    source_id: str
    dataset_name: str
    run_id: str
    expected_chunks: int
    successful_chunks: int
    failed_chunks: int
    skipped_chunks: int
    coverage_ratio: float
    coverage_status: str
    publish_status: str
    chunks: tuple[ChunkResult, ...]
    started_at: str
    updated_at: str
    finished_at: str | None = None
    raw_dir: str | None = None
    downstream_raw_path: str | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

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
            "chunks": [chunk.to_dict() for chunk in self.chunks],
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "finished_at": self.finished_at,
            "raw_dir": self.raw_dir,
            "downstream_raw_path": self.downstream_raw_path,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class PublishedManifest:
    source_id: str
    dataset_name: str
    run_id: str
    published_at: str
    coverage_status: str
    publish_status: str
    chunks: tuple[ChunkResult, ...]
    raw_dir: str
    source_key: str | None = None
    downstream_raw_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        result = {
            "source_id": self.source_id,
            "dataset_name": self.dataset_name,
            "run_id": self.run_id,
            "published_at": self.published_at,
            "coverage_status": self.coverage_status,
            "publish_status": self.publish_status,
            "chunks": [chunk.to_dict() for chunk in self.chunks],
            "raw_dir": self.raw_dir,
            "source_key": self.source_key,
            "downstream_raw_path": self.downstream_raw_path,
        }
        return result
