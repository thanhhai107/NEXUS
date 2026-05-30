"""Dataset Registry Module.

Provides dataset metadata catalog and lineage tracking.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common.config import RUNTIME_DIR


CATALOG_DIR = RUNTIME_DIR / "catalog" / "datasets"


@dataclass
class DatasetMetadata:
    """Metadata for a dataset."""
    dataset_id: str
    domain: str
    description: str = ""
    owner: str = ""
    steward: str = ""
    sensitivity: str = "public"
    source_type: str = ""
    velocity: str = ""  # "batch", "streaming", "polling"
    created_at: str = ""
    updated_at: str = ""
    tags: list[str] = field(default_factory=list)
    links: dict[str, str] = field(default_factory=dict)
    schema_version: str | None = None


class DatasetRegistry:
    """Registry for managing dataset metadata."""

    def __init__(self, catalog_dir: Path | None = None):
        """Initialize registry.
        
        Args:
            catalog_dir: Directory for catalog storage
        """
        self.catalog_dir = catalog_dir or CATALOG_DIR
        self.catalog_dir.mkdir(parents=True, exist_ok=True)

    def register(
        self,
        dataset_id: str,
        domain: str,
        description: str = "",
        **kwargs,
    ) -> DatasetMetadata:
        """Register a new dataset.
        
        Args:
            dataset_id: Unique dataset identifier
            domain: Domain (e.g., 'transport', 'environment')
            description: Dataset description
            **kwargs: Additional metadata fields
        
        Returns:
            DatasetMetadata entry
        """
        metadata = DatasetMetadata(
            dataset_id=dataset_id,
            domain=domain,
            description=description,
            created_at=datetime.now(timezone.utc).isoformat(),
            updated_at=datetime.now(timezone.utc).isoformat(),
            **kwargs,
        )
        
        self._save(metadata)
        return metadata

    def get(self, dataset_id: str) -> DatasetMetadata | None:
        """Get dataset metadata."""
        path = self.catalog_dir / f"{dataset_id}.json"
        
        if not path.exists():
            return None
        
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return DatasetMetadata(**data)
        except (json.JSONDecodeError, TypeError):
            return None

    def update(self, dataset_id: str, **updates) -> DatasetMetadata | None:
        """Update dataset metadata."""
        metadata = self.get(dataset_id)
        
        if metadata is None:
            return None
        
        for key, value in updates.items():
            if hasattr(metadata, key):
                setattr(metadata, key, value)
        
        metadata.updated_at = datetime.now(timezone.utc).isoformat()
        self._save(metadata)
        
        return metadata

    def list_by_domain(self, domain: str) -> list[DatasetMetadata]:
        """List all datasets in a domain."""
        datasets = []
        
        for path in self.catalog_dir.glob("*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if data.get("domain") == domain:
                    datasets.append(DatasetMetadata(**data))
            except (json.JSONDecodeError, TypeError):
                continue
        
        return sorted(datasets, key=lambda d: d.dataset_id)

    def list_all(self) -> list[DatasetMetadata]:
        """List all registered datasets."""
        datasets = []
        
        for path in self.catalog_dir.glob("*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                datasets.append(DatasetMetadata(**data))
            except (json.JSONDecodeError, TypeError):
                continue
        
        return sorted(datasets, key=lambda d: d.dataset_id)

    def _save(self, metadata: DatasetMetadata) -> None:
        """Save dataset metadata to disk."""
        path = self.catalog_dir / f"{metadata.dataset_id}.json"
        path.write_text(
            json.dumps(metadata.__dict__, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )


def register_dataset(
    dataset_id: str,
    domain: str,
    description: str = "",
    **kwargs,
) -> DatasetMetadata:
    """Register a dataset in the global registry."""
    registry = DatasetRegistry()
    return registry.register(dataset_id, domain, description, **kwargs)


def get_dataset(dataset_id: str) -> DatasetMetadata | None:
    """Get dataset metadata."""
    registry = DatasetRegistry()
    return registry.get(dataset_id)


def list_datasets_by_domain(domain: str) -> list[DatasetMetadata]:
    """List datasets in a domain."""
    registry = DatasetRegistry()
    return registry.list_by_domain(domain)
