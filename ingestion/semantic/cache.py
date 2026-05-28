"""
Semantic Annotation Cache.

Stores and retrieves semantic annotations for sources.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
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


@dataclass
class AnnotationMetadata:
    """Metadata for a cached annotation."""
    
    source_id: str
    version: str
    schema_hash: str
    field_count: int
    annotated_at: str
    annotated_by: str = "llm"  # "llm", "template", "manual"
    approved: bool = False
    approved_at: str | None = None
    approved_by: str | None = None
    notes: str = ""


class SemanticCache:
    """
    Semantic annotation cache system.
    
    Stores annotations per source, versioned by schema hash.
    
    Directory structure:
        semantic_cache/
        ├── tfl_arrivals/
        │   ├── v1a3f/
        │   │   ├── annotations.json
        │   │   ├── metadata.json
        │   │   └── approved.json (optional)
        │   └── v2b7c/
        │       └── ...
    
    Usage:
        cache = SemanticCache("semantic_cache")
        
        # Get cached annotations
        cached = cache.get("tfl_arrivals")
        if cached:
            print(f"Found {len(cached.annotations)} annotations")
        
        # Save annotations
        cache.set("tfl_arrivals", annotations, schema_hash="v1a3f")
    """
    
    def __init__(self, cache_dir: Path | str):
        """
        Initialize semantic cache.
        
        Args:
            cache_dir: Path to cache directory
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get(self, source_id: str) -> CachedAnnotations | None:
        """
        Get cached annotations for a source.
        
        Gets the most recent version if exists.
        
        Args:
            source_id: Source identifier
        
        Returns:
            CachedAnnotations if exists, None otherwise
        """
        source_dir = self.cache_dir / source_id
        if not source_dir.exists():
            return None
        
        # Find versions sorted by name (should follow v* pattern)
        versions = sorted(
            [d for d in source_dir.iterdir() if d.is_dir() and d.name.startswith("v")],
            reverse=True
        )
        
        if not versions:
            return None
        
        # Get latest version
        latest = versions[0]
        cache_file = latest / "annotations.json"
        
        if not cache_file.exists():
            return None
        
        try:
            data = json.loads(cache_file.read_text(encoding="utf-8"))
            
            # Separate metadata keys
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
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Warning: Failed to load cache for {source_id}: {e}")
            return None
    
    def get_version(self, source_id: str, version: str) -> CachedAnnotations | None:
        """
        Get cached annotations for a specific version.
        
        Args:
            source_id: Source identifier
            version: Version string (e.g., "v1a3f")
        
        Returns:
            CachedAnnotations if exists, None otherwise
        """
        source_dir = self.cache_dir / source_id
        version_dir = source_dir / version
        
        if not version_dir.exists():
            return None
        
        cache_file = version_dir / "annotations.json"
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
                version=version,
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
        """
        Save annotations to cache.
        
        Creates a new version based on schema hash.
        
        Args:
            source_id: Source identifier
            annotations: Dict of field_name -> annotation
            schema_hash: Hash of the schema for versioning
            annotated_by: Who/what annotated ("llm", "template", "manual")
        
        Returns:
            Version string (e.g., "v1a3f")
        """
        source_dir = self.cache_dir / source_id
        source_dir.mkdir(parents=True, exist_ok=True)
        
        # Create version based on schema hash
        version = f"v{schema_hash[:4]}"
        version_dir = source_dir / version
        version_dir.mkdir(exist_ok=True)
        
        # Save annotations
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
        
        # Save metadata
        metadata = AnnotationMetadata(
            source_id=source_id,
            version=version,
            schema_hash=schema_hash,
            field_count=len(annotations),
            annotated_at=now,
            annotated_by=annotated_by,
        )
        
        (version_dir / "metadata.json").write_text(
            json.dumps({
                "source_id": metadata.source_id,
                "version": metadata.version,
                "schema_hash": metadata.schema_hash,
                "field_count": metadata.field_count,
                "annotated_at": metadata.annotated_at,
                "annotated_by": metadata.annotated_by,
                "approved": metadata.approved,
            }, indent=2),
            encoding="utf-8"
        )
        
        return version
    
    def approve(
        self,
        source_id: str,
        version: str | None = None,
        approved_by: str = "human",
        notes: str = "",
    ) -> bool:
        """
        Mark annotations as approved (human review).
        
        Args:
            source_id: Source identifier
            version: Version to approve, or latest if None
            approved_by: Who approved
            notes: Optional notes
        
        Returns:
            True if approved successfully
        """
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
            "notes": notes,
        }
        
        (version_dir / "approved.json").write_text(
            json.dumps(approved_data, indent=2),
            encoding="utf-8"
        )
        
        # Update metadata
        metadata_file = version_dir / "metadata.json"
        if metadata_file.exists():
            try:
                metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
                metadata.update(approved_data)
                metadata_file.write_text(
                    json.dumps(metadata, indent=2),
                    encoding="utf-8"
                )
            except json.JSONDecodeError:
                pass
        
        return True
    
    def is_approved(self, source_id: str, version: str | None = None) -> bool:
        """
        Check if annotations are approved.
        
        Args:
            source_id: Source identifier
            version: Version to check, or latest if None
        
        Returns:
            True if approved
        """
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
    
    def list_versions(self, source_id: str) -> list[str]:
        """
        List all versions for a source.
        
        Args:
            source_id: Source identifier
        
        Returns:
            List of version strings
        """
        source_dir = self.cache_dir / source_id
        if not source_dir.exists():
            return []
        
        return sorted([
            d.name for d in source_dir.iterdir()
            if d.is_dir() and d.name.startswith("v")
        ], reverse=True)
    
    def list_sources(self) -> list[str]:
        """
        List all sources with cached annotations.
        
        Returns:
            List of source IDs
        """
        return sorted([
            d.name for d in self.cache_dir.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ])
    
    def delete(self, source_id: str, version: str | None = None) -> bool:
        """
        Delete cached annotations.
        
        Args:
            source_id: Source identifier
            version: Specific version to delete, or all if None
        
        Returns:
            True if deleted
        """
        source_dir = self.cache_dir / source_id
        if not source_dir.exists():
            return False
        
        if version is None:
            # Delete all versions
            for v_dir in source_dir.iterdir():
                if v_dir.is_dir() and v_dir.name.startswith("v"):
                    for f in v_dir.iterdir():
                        f.unlink()
                    v_dir.rmdir()
            source_dir.rmdir()
        else:
            # Delete specific version
            version_dir = source_dir / version
            if version_dir.exists():
                for f in version_dir.iterdir():
                    f.unlink()
                version_dir.rmdir()
        
        return True
    
    def get_status(self) -> dict[str, dict[str, Any]]:
        """
        Get status of all cached annotations.
        
        Returns:
            Dict of source_id -> status info
        """
        status = {}
        
        for source_id in self.list_sources():
            versions = self.list_versions(source_id)
            latest = self.get(source_id) if versions else None
            
            approved = self.is_approved(source_id)
            
            status[source_id] = {
                "version_count": len(versions),
                "latest_version": versions[0] if versions else None,
                "field_count": latest.field_count if latest else 0,
                "annotated_at": latest.annotated_at if latest else None,
                "approved": approved,
            }
        
        return status
