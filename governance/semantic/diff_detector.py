"""Schema Diff Detector.

Detects changes between schema versions.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from governance.schema.inference import InferredSchema


@dataclass
class DiffResult:
    """Result of schema diff."""
    schema_hash: str
    has_changes: bool
    is_new_source: bool
    new_fields: list[str]
    removed_fields: list[str]
    changed_fields: list[str]


class SchemaDiffDetector:
    """Detects changes in schema."""
    
    def __init__(self, cache_dir: Path | str):
        self.cache_dir = Path(cache_dir)
    
    def detect_changes(
        self,
        source_id: str,
        schema: InferredSchema,
    ) -> DiffResult:
        """Detect changes between current schema and cached version."""
        new_hash = self._compute_hash(schema)
        
        # Check if source exists
        source_dir = self.cache_dir / source_id
        if not source_dir.exists():
            return DiffResult(
                schema_hash=new_hash,
                has_changes=True,
                is_new_source=True,
                new_fields=list(schema.fields.keys()),
                removed_fields=[],
                changed_fields=[],
            )
        
        # Find latest version
        versions = sorted(
            [d for d in source_dir.iterdir() if d.is_dir() and d.name.startswith("v")],
            reverse=True
        )
        
        if not versions:
            return DiffResult(
                schema_hash=new_hash,
                has_changes=True,
                is_new_source=True,
                new_fields=list(schema.fields.keys()),
                removed_fields=[],
                changed_fields=[],
            )
        
        # Load cached schema
        latest = versions[0]
        cached_file = latest / "annotations.json"
        
        if not cached_file.exists():
            return DiffResult(
                schema_hash=new_hash,
                has_changes=True,
                is_new_source=False,
                new_fields=list(schema.fields.keys()),
                removed_fields=[],
                changed_fields=[],
            )
        
        cached_data = json.loads(cached_file.read_text(encoding="utf-8"))
        cached_hash = cached_data.get("_schema_hash", "")
        
        if cached_hash == new_hash:
            return DiffResult(
                schema_hash=new_hash,
                has_changes=False,
                is_new_source=False,
                new_fields=[],
                removed_fields=[],
                changed_fields=[],
            )
        
        # Compute diff
        current_fields = set(schema.fields.keys())
        cached_fields = set(k for k in cached_data.keys() if not k.startswith("_"))
        
        new_fields = sorted(current_fields - cached_fields)
        removed_fields = sorted(cached_fields - current_fields)
        changed_fields = []  # Simplified - would need type comparison
        
        should_reannotate = len(new_fields) >= 10
        
        return DiffResult(
            schema_hash=new_hash,
            has_changes=True,
            is_new_source=False,
            new_fields=new_fields,
            removed_fields=removed_fields,
            changed_fields=changed_fields,
        )
    
    def _compute_hash(self, schema: InferredSchema) -> str:
        """Compute hash of schema."""
        schema_dict = schema.to_dict()
        schema_str = json.dumps(schema_dict, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:12]
    
    @property
    def should_reannotate(self) -> bool:
        """Check if should reannotate entire source."""
        return False  # Simplified


def create_diff_detector(cache_dir: str = "governance/semantic") -> SchemaDiffDetector:
    """Create a schema diff detector."""
    return SchemaDiffDetector(cache_dir)
