"""Semantic Annotation Cache.

Extracted from ingestion/semantic/cache.py for governance service.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class CachedAnnotations:
    """Cached semantic annotations for a source."""
    source_id: str
    version: str
    schema_hash: str
    annotations: dict[str, dict]
    annotated_at: str
    field_count: int = 0

    def __post_init__(self):
        if self.field_count == 0:
            self.field_count = len(self.annotations)


class SemanticCache:
    """Semantic annotation cache system."""

    def __init__(self, cache_dir: Path | str):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get(self, source_id: str) -> CachedAnnotations | None:
        """Get cached annotations for a source."""
        source_dir = self.cache_dir / source_id
        if not source_dir.exists():
            return None

        versions = sorted(
            [d for d in source_dir.iterdir() if d.is_dir() and d.name.startswith("v")],
            reverse=True
        )

        if not versions:
            return None

        latest = versions[0]
        cache_file = latest / "annotations.json"

        if not cache_file.exists():
            return None

        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))

            annotations = {
                k: v for k, v in data.items()
                if not k.startswith("_")
            }

            return CachedAnnotations(
                source_id=source_id,
                version=latest.name,
                schema_hash=data.get("_schema_hash", ""),
                annotations=annotations,
                annotated_at=data.get("_annotated_at", ""),
                field_count=data.get("_field_count", len(annotations)),
            )
        except (json.JSONDecodeError, KeyError):
            return None

    def set(
        self,
        source_id: str,
        annotations: dict[str, dict],
        schema_hash: str,
        annotated_by: str = "llm",
    ) -> str:
        """Save annotations to cache."""
        source_dir = self.cache_dir / source_id
        source_dir.mkdir(parents=True, exist_ok=True)

        version = f"v{schema_hash[:4]}"
        version_dir = source_dir / version
        version_dir.mkdir(exist_ok=True)

        now = datetime.now(timezone.utc).isoformat()
        cache_data = {
            **annotations,
            "_schema_hash": schema_hash,
            "_annotated_at": now,
            "_annotated_by": annotated_by,
            "_field_count": len(annotations),
        }

        (version_dir / "annotations.json").write_text(
            json.dumps(cache_data, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )

        return version

    def approve(
        self,
        source_id: str,
        version: str | None = None,
        approved_by: str = "human",
    ) -> bool:
        """Mark annotations as approved."""
        source_dir = self.cache_dir / source_id
        if not source_dir.exists():
            return False

        if version is None:
            versions = sorted(
                [d for d in source_dir.iterdir() if d.is_dir() and d.name.startswith("v")],
                reverse=True
            )
            if not versions:
                return False
            version_dir = versions[0]
        else:
            version_dir = source_dir / version
            if not version_dir.exists():
                return False

        now = datetime.now(timezone.utc).isoformat()

        approved_data = {
            "approved": True,
            "approved_at": now,
            "approved_by": approved_by,
        }

        (version_dir / "approved.json").write_text(
            json.dumps(approved_data, indent=2),
            encoding="utf-8"
        )

        return True

    def is_approved(self, source_id: str, version: str | None = None) -> bool:
        """Check if annotations are approved."""
        source_dir = self.cache_dir / source_id
        if not source_dir.exists():
            return False

        if version is None:
            versions = sorted(
                [d for d in source_dir.iterdir() if d.is_dir() and d.name.startswith("v")],
                reverse=True
            )
            if not versions:
                return False
            version_dir = versions[0]
        else:
            version_dir = source_dir / version
            if not version_dir.exists():
                return False

        approved_file = version_dir / "approved.json"
        if not approved_file.exists():
            return False

        try:
            data = json.loads(approved_file.read_text(encoding="utf-8"))
            return data.get("approved", False)
        except json.JSONDecodeError:
            return False

    def get_status(self) -> dict[str, dict[str, Any]]:
        """Get status of all cached annotations."""
        status = {}

        for source_id in self.list_sources():
            versions = self.list_versions(source_id)
            latest = self.get(source_id)
            approved = self.is_approved(source_id)

            status[source_id] = {
                "version_count": len(versions),
                "latest_version": versions[0] if versions else None,
                "field_count": latest.field_count if latest else 0,
                "annotated_at": latest.annotated_at if latest else None,
                "approved": approved,
            }

        return status

    def list_versions(self, source_id: str) -> list[str]:
        """List all versions for a source."""
        source_dir = self.cache_dir / source_id
        if not source_dir.exists():
            return []

        return sorted([
            d.name for d in source_dir.iterdir()
            if d.is_dir() and d.name.startswith("v")
        ], reverse=True)

    def list_sources(self) -> list[str]:
        """List all sources with cached annotations."""
        return sorted([
            d.name for d in self.cache_dir.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ])
