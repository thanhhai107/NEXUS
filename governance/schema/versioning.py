"""Schema Versioning Module.

Provides schema registry and versioning for tracking schema evolution.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common.config import RUNTIME_DIR


SCHEMA_REGISTRY_DIR = RUNTIME_DIR / "catalog" / "schemas"


@dataclass
class SchemaVersion:
    """A versioned schema entry."""
    version: str
    source_id: str
    schema_hash: str
    created_at: str
    field_count: int
    record_count: int
    path: str
    approved: bool = False


class SchemaRegistry:
    """Registry for managing schema versions."""

    def __init__(self, registry_dir: Path | None = None):
        """Initialize registry.
        
        Args:
            registry_dir: Directory for schema storage (defaults to catalog/schemas)
        """
        self.registry_dir = registry_dir or SCHEMA_REGISTRY_DIR
        self.registry_dir.mkdir(parents=True, exist_ok=True)
        self.index_path = self.registry_dir / "_index.json"

    def _load_index(self) -> dict[str, Any]:
        """Load the registry index."""
        if not self.index_path.exists():
            return {"schemas": {}, "latest": {}}
        
        try:
            return json.loads(self.index_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"schemas": {}, "latest": {}}

    def _save_index(self, index: dict[str, Any]) -> None:
        """Save the registry index."""
        self.index_path.write_text(
            json.dumps(index, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def register(
        self,
        source_id: str,
        schema_path: Path,
        record_count: int = 0,
    ) -> SchemaVersion:
        """Register a new schema version.
        
        Args:
            source_id: Dataset/source identifier
            schema_path: Path to schema JSON file
            record_count: Number of records used for inference
        
        Returns:
            SchemaVersion entry
        """
        # Read schema and compute hash
        schema_data = json.loads(schema_path.read_text(encoding="utf-8"))
        schema_hash = hashlib.sha256(
            json.dumps(schema_data, sort_keys=True).encode()
        ).hexdigest()[:12]
        
        # Create version directory
        version = f"v{len(self.list_versions(source_id)) + 1}_{schema_hash}"
        version_dir = self.registry_dir / source_id / version
        version_dir.mkdir(parents=True, exist_ok=True)
        
        # Copy schema to version directory
        import shutil
        shutil.copy2(schema_path, version_dir / "schema.json")
        
        # Create version entry
        version_entry = SchemaVersion(
            version=version,
            source_id=source_id,
            schema_hash=schema_hash,
            created_at=datetime.now(timezone.utc).isoformat(),
            field_count=len(schema_data.get("properties", {})),
            record_count=record_count,
            path=str(version_dir / "schema.json"),
        )
        
        # Update index
        index = self._load_index()
        
        if source_id not in index["schemas"]:
            index["schemas"][source_id] = {}
        
        index["schemas"][source_id][version] = {
            "schema_hash": schema_hash,
            "created_at": version_entry.created_at,
            "field_count": version_entry.field_count,
            "record_count": version_entry.record_count,
            "path": version_entry.path,
        }
        
        index["latest"][source_id] = version
        
        self._save_index(index)
        
        return version_entry

    def get_version(self, source_id: str, version: str) -> SchemaVersion | None:
        """Get a specific schema version."""
        index = self._load_index()
        
        schemas = index.get("schemas", {}).get(source_id, {})
        if version not in schemas:
            return None
        
        entry = schemas[version]
        return SchemaVersion(
            version=version,
            source_id=source_id,
            schema_hash=entry["schema_hash"],
            created_at=entry["created_at"],
            field_count=entry["field_count"],
            record_count=entry["record_count"],
            path=entry["path"],
        )

    def get_latest(self, source_id: str) -> SchemaVersion | None:
        """Get the latest schema version for a source."""
        index = self._load_index()
        
        latest_version = index.get("latest", {}).get(source_id)
        if not latest_version:
            return None
        
        return self.get_version(source_id, latest_version)

    def list_versions(self, source_id: str) -> list[str]:
        """List all versions for a source."""
        index = self._load_index()
        schemas = index.get("schemas", {}).get(source_id, {})
        return sorted(schemas.keys(), reverse=True)

    def list_sources(self) -> list[str]:
        """List all registered sources."""
        index = self._load_index()
        return sorted(index.get("schemas", {}).keys())

    def approve_version(self, source_id: str, version: str) -> bool:
        """Mark a schema version as approved."""
        index = self._load_index()
        
        if source_id not in index["schemas"] or version not in index["schemas"][source_id]:
            return False
        
        # Create approval marker
        source_dir = self.registry_dir / source_id / version
        approval_file = source_dir / "approved.json"
        approval_file.write_text(json.dumps({
            "approved": True,
            "approved_at": datetime.now(timezone.utc).isoformat(),
        }, indent=2))
        
        index["schemas"][source_id][version]["approved"] = True
        self._save_index(index)
        
        return True


def register_schema(
    source_id: str,
    schema_path: Path,
    record_count: int = 0,
) -> SchemaVersion:
    """Register a schema in the global registry."""
    registry = SchemaRegistry()
    return registry.register(source_id, schema_path, record_count)


def get_latest_schema(source_id: str) -> SchemaVersion | None:
    """Get the latest schema for a source."""
    registry = SchemaRegistry()
    return registry.get_latest(source_id)


def compare_schemas(source_id: str, version_a: str, version_b: str) -> dict[str, Any]:
    """Compare two schema versions.
    
    Args:
        source_id: Source/dataset identifier
        version_a: First version to compare
        version_b: Second version to compare
    
    Returns:
        Dict with added, removed, and changed fields
    """
    registry = SchemaRegistry()
    
    schema_a = registry.get_version(source_id, version_a)
    schema_b = registry.get_version(source_id, version_b)
    
    if not schema_a or not schema_b:
        return {"error": "One or both versions not found"}
    
    # Load schema data
    data_a = json.loads(Path(schema_a.path).read_text(encoding="utf-8"))
    data_b = json.loads(Path(schema_b.path).read_text(encoding="utf-8"))
    
    fields_a = set(data_a.get("properties", {}).keys())
    fields_b = set(data_b.get("properties", {}).keys())
    
    return {
        "version_a": version_a,
        "version_b": version_b,
        "added_fields": sorted(fields_b - fields_a),
        "removed_fields": sorted(fields_a - fields_b),
        "common_fields": sorted(fields_a & fields_b),
        "field_count_a": len(fields_a),
        "field_count_b": len(fields_b),
    }
