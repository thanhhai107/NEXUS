"""
Schema Diff Detector.

Compares current inferred schema with cached annotations to detect changes.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from governance.schema.inference import InferredSchema


@dataclass
class DiffResult:
    """Result of schema diff comparison."""
    
    is_new_source: bool = False
    new_fields: list[str] = field(default_factory=list)
    changed_fields: list[str] = field(default_factory=list)
    stable_fields: list[str] = field(default_factory=list)
    should_reannotate: bool = False
    schema_hash: str = ""

    @property
    def has_changes(self) -> bool:
        """Check if there are any changes."""
        return bool(self.new_fields or self.changed_fields)

    @property
    def total_new_or_changed(self) -> int:
        """Total number of new or changed fields."""
        return len(self.new_fields) + len(self.changed_fields)


class SchemaDiffDetector:
    """
    Detects changes between current schema and cached annotations.
    
    Usage:
        detector = SchemaDiffDetector(cache_dir="semantic_cache")
        diff = detector.detect_changes("tpcds_store_sales", inferred_schema)
        
        if diff.should_reannotate:
            # Call LLM
        else:
            # Use cached
    """
    
    def __init__(self, cache_dir: Path | str):
        """
        Initialize diff detector.
        
        Args:
            cache_dir: Path to semantic cache directory
        """
        self.cache_dir = Path(cache_dir)
    
    def detect_changes(
        self,
        source_id: str,
        current_schema: InferredSchema,
    ) -> DiffResult:
        """
        Detect changes between current schema and cached version.
        
        Args:
            source_id: Source identifier
            current_schema: Current inferred schema
        
        Returns:
            DiffResult with information about new/changed/stable fields
        """
        from ingestion.semantic.cache import SemanticCache
        
        cache = SemanticCache(self.cache_dir)
        cached = cache.get(source_id)
        
        if cached is None:
            # New source - all fields are new
            schema_hash = self._compute_schema_hash(current_schema)
            return DiffResult(
                is_new_source=True,
                new_fields=list(current_schema.fields.keys()),
                changed_fields=[],
                stable_fields=[],
                should_reannotate=True,
                schema_hash=schema_hash,
            )
        
        # Compare fields
        new_fields = []
        changed_fields = []
        stable_fields = []
        
        for field_name, current_field in current_schema.fields.items():
            if field_name not in cached.annotations:
                new_fields.append(field_name)
            elif self._field_changed(current_field, cached.annotations[field_name]):
                changed_fields.append(field_name)
            else:
                stable_fields.append(field_name)
        
        # Decision: re-annotate if many changes
        total_fields = len(current_schema.fields)
        new_ratio = len(new_fields) / total_fields if total_fields > 0 else 0
        
        should_reannotate = (
            len(new_fields) >= 3 or           # >= 3 new fields
            new_ratio > 0.3 or               # > 30% new fields
            len(changed_fields) >= 5 or        # >= 5 changed fields
            cached.schema_hash != self._compute_schema_hash(current_schema)
        )
        
        return DiffResult(
            is_new_source=False,
            new_fields=new_fields if not should_reannotate else list(current_schema.fields.keys()),
            changed_fields=changed_fields if not should_reannotate else [],
            stable_fields=stable_fields,
            should_reannotate=should_reannotate,
            schema_hash=self._compute_schema_hash(current_schema),
        )
    
    def _field_changed(
        self,
        current_field,
        cached_annotation: dict,
    ) -> bool:
        """
        Check if a field has changed significantly.
        
        Args:
            current_field: Current FieldSchema
            cached_annotation: Cached annotation dict
        
        Returns:
            True if field has changed
        """
        # Type changed
        if current_field.inferred_type != cached_annotation.get("inferred_type"):
            return True
        
        # Nullable changed
        if current_field.nullable != cached_annotation.get("nullable"):
            return True
        
        # Pattern changed
        if current_field.pattern != cached_annotation.get("pattern"):
            return True
        
        return False
    
    def _compute_schema_hash(self, schema: InferredSchema) -> str:
        """
        Compute hash of schema for version tracking.
        
        Args:
            schema: InferredSchema to hash
        
        Returns:
            8-character hash string
        """
        # Create stable representation of schema
        schema_data = {
            "fields": {
                name: {
                    "type": f.inferred_type,
                    "nullable": f.nullable,
                    "pattern": f.pattern,
                }
                for name, f in sorted(schema.fields.items())
            },
            "record_count": schema.record_count,
        }
        
        hash_input = json.dumps(schema_data, sort_keys=True)
        return hashlib.sha256(hash_input.encode()).hexdigest()[:8]
