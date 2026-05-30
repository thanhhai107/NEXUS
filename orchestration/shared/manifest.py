"""Manifest Management for Orchestration.

Provides manifest reading/writing operations.
Extracted from ingestion/base/core.py for orchestration use.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from common.config import BRONZE_DIR, RUNTIME_DIR


class CoverageStatus(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    FAILED = "failed"


class PublishStatus(str, Enum):
    PUBLISHED = "published"
    PUBLISHED_WITH_WARNING = "published_with_warning"
    UNPUBLISHED = "unpublished"


@dataclass
class ChunkResult:
    """Result of processing a single chunk."""
    chunk_id: str
    status: str  # "success", "failed", "skipped"
    required: bool = True
    paths: tuple[str, ...] = field(default_factory=tuple)
    checksums: dict[str, str] = field(default_factory=dict)
    record_count: int = 0
    error: str | None = None


@dataclass
class RunManifest:
    """Manifest for a complete run."""
    source_id: str
    dataset_name: str
    run_id: str
    expected_chunks: int = 0
    successful_chunks: int = 0
    failed_chunks: int = 0
    skipped_chunks: int = 0
    coverage_ratio: float = 0.0
    coverage_status: str = CoverageStatus.FAILED
    publish_status: str = PublishStatus.UNPUBLISHED
    chunks: tuple[ChunkResult, ...] = field(default_factory=tuple)
    started_at: str = ""
    updated_at: str = ""
    finished_at: str = ""
    raw_dir: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for JSON serialization."""
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
                    "error": c.error,
                }
                for c in self.chunks
            ],
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "finished_at": self.finished_at,
            "raw_dir": self.raw_dir,
            "details": self.details,
        }


@dataclass
class PublishedManifest:
    """Manifest for published (ready for downstream) runs."""
    source_id: str
    dataset_name: str
    run_id: str
    published_at: str
    coverage_status: str
    publish_status: str
    chunks: tuple[dict[str, Any], ...]
    raw_dir: str
    source_key: str
    downstream_raw_path: str | None = None


def get_manifest_path(run_id: str, dataset: str, published: bool = False) -> Path:
    """Get path to manifest file.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
        published: If True, return published manifest path
    
    Returns:
        Path to manifest.json
    """
    base = BRONZE_DIR / dataset / f"run_id={run_id}"
    
    if published:
        return base / "published" / "published_manifest.json"
    
    return base / "metadata" / "run_manifest.json"


def read_manifest(run_id: str, dataset: str) -> RunManifest | None:
    """Read run manifest.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
    
    Returns:
        RunManifest or None if not found
    """
    path = get_manifest_path(run_id, dataset)
    
    if not path.exists():
        return None
    
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return _dict_to_manifest(data)
    except (json.JSONDecodeError, KeyError):
        return None


def read_published_manifest(run_id: str, dataset: str) -> PublishedManifest | None:
    """Read published manifest.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
    
    Returns:
        PublishedManifest or None if not found
    """
    path = get_manifest_path(run_id, dataset, published=True)
    
    if not path.exists():
        return None
    
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return PublishedManifest(
            source_id=data.get("source_id", ""),
            dataset_name=data.get("dataset_name", ""),
            run_id=data.get("run_id", ""),
            published_at=data.get("published_at", ""),
            coverage_status=data.get("coverage_status", ""),
            publish_status=data.get("publish_status", ""),
            chunks=tuple(data.get("chunks", [])),
            raw_dir=data.get("raw_dir", ""),
            source_key=data.get("source_key", ""),
            downstream_raw_path=data.get("downstream_raw_path"),
        )
    except (json.JSONDecodeError, KeyError):
        return None


def write_manifest(run_id: str, dataset: str, manifest: RunManifest) -> Path:
    """Write run manifest.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
        manifest: Manifest to write
    
    Returns:
        Path to written manifest
    """
    path = get_manifest_path(run_id, dataset)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    path.write_text(
        json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    
    return path


def is_published(run_id: str, dataset: str) -> bool:
    """Check if a run is published.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
    
    Returns:
        True if published
    """
    manifest = read_published_manifest(run_id, dataset)
    
    if manifest is None:
        return False
    
    return manifest.publish_status in (
        PublishStatus.PUBLISHED,
        PublishStatus.PUBLISHED_WITH_WARNING,
    )


def assert_published(run_id: str, dataset: str) -> None:
    """Assert that a run is published.
    
    Args:
        run_id: Run identifier
        dataset: Dataset name
    
    Raises:
        FileNotFoundError: If manifest not found
        ValueError: If not published
    """
    if not is_published(run_id, dataset):
        raise ValueError(
            f"Run {run_id} for dataset {dataset} is not published. "
            "Ensure all required chunks are complete."
        )


def _dict_to_manifest(data: dict[str, Any]) -> RunManifest:
    """Convert dict to RunManifest."""
    chunks = []
    for chunk_data in data.get("chunks", []):
        chunks.append(ChunkResult(
            chunk_id=chunk_data.get("chunk_id", ""),
            status=chunk_data.get("status", "failed"),
            required=chunk_data.get("required", True),
            paths=tuple(chunk_data.get("paths", [])),
            checksums=chunk_data.get("checksums", {}),
            record_count=chunk_data.get("record_count", 0),
            error=chunk_data.get("error"),
        ))
    
    return RunManifest(
        source_id=data.get("source_id", ""),
        dataset_name=data.get("dataset_name", ""),
        run_id=data.get("run_id", ""),
        expected_chunks=data.get("expected_chunks", 0),
        successful_chunks=data.get("successful_chunks", 0),
        failed_chunks=data.get("failed_chunks", 0),
        skipped_chunks=data.get("skipped_chunks", 0),
        coverage_ratio=data.get("coverage_ratio", 0.0),
        coverage_status=data.get("coverage_status", CoverageStatus.FAILED),
        publish_status=data.get("publish_status", PublishStatus.UNPUBLISHED),
        chunks=tuple(chunks),
        started_at=data.get("started_at", ""),
        updated_at=data.get("updated_at", ""),
        finished_at=data.get("finished_at", ""),
        raw_dir=data.get("raw_dir", ""),
        details=data.get("details", {}),
    )
