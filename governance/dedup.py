"""Record Deduplication Module.

Provides deduplication for records at the Bronze layer to prevent
duplicate writes when jobs fail and retry.
Supports both local filesystem and S3/MinIO storage.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from common.config import RUNTIME_DIR, is_vm_mode
from common.storage import get_governance_storage


DEDUP_DIR = RUNTIME_DIR / "dedup"


@dataclass
class DedupKey:
    """Deduplication key configuration for a source."""
    source_id: str
    key_fields: list[str]
    hash_algorithm: str = "sha256"
    
    def compute_key(self, record: dict[str, Any]) -> str:
        """Compute dedup key from record fields.
        
        Args:
            record: Data record
            
        Returns:
            Hash string of the key fields
        """
        key_values = []
        for field_name in self.key_fields:
            value = record.get(field_name)
            if value is not None:
                key_values.append(f"{field_name}={json.dumps(value, sort_keys=True)}")
        
        key_str = "|".join(key_values)
        
        if self.hash_algorithm == "sha256":
            return hashlib.sha256(key_str.encode()).hexdigest()[:16]
        elif self.hash_algorithm == "md5":
            return hashlib.md5(key_str.encode()).hexdigest()[:16]
        else:
            return key_str


@dataclass 
class DedupIndex:
    """Index of seen dedup keys for a source/run."""
    source_id: str
    run_id: str
    seen_keys: set[str] = field(default_factory=set)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    record_count: int = 0
    duplicate_count: int = 0
    
    def add(self, key: str) -> bool:
        """Add a key to the index.
        
        Args:
            key: Deduplication key
            
        Returns:
            True if key was new (not a duplicate), False if already seen
        """
        self.record_count += 1
        
        if key in self.seen_keys:
            self.duplicate_count += 1
            return False
        
        self.seen_keys.add(key)
        return True
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "run_id": self.run_id,
            "record_count": self.record_count,
            "duplicate_count": self.duplicate_count,
            "unique_count": len(self.seen_keys),
            "created_at": self.created_at,
            "dedup_ratio": round(self.duplicate_count / self.record_count, 4) if self.record_count > 0 else 0.0,
        }


class Deduplicator:
    """Manages record-level deduplication for Bronze layer."""
    
    # Default dedup keys by source type
    DEFAULT_KEYS: dict[str, list[str]] = {
        "tpcdi_dim_date": ["sk_date"],
        "tpcdi_dim_time": ["sk_time"],
        "tpcdi_dim_account": ["sk_accountid"],
        "tpcdi_dim_customer": ["sk_customerid"],
        "tpcdi_dim_broker": ["sk_brokerid"],
        "tpcdi_dim_security": ["sk_securityid"],
        "tpcdi_dim_company": ["sk_companyid"],
        "tpcdi_dim_trade": ["sk_tradeid"],
        "tpcdi_fact_cash_balances": ["sk_customerid", "sk_accountid", "sk_date"],
        "tpcdi_fact_holdings": ["sk_tradeid", "sk_customerid", "sk_securityid", "sk_closedate"],
        "tpcdi_fact_market_history": ["sk_securityid", "sk_date"],
        "tpcdi_fact_watches": ["sk_customerid", "sk_securityid"],
        "tpcdi_industry": ["in_id"],
        "tpcdi_status_type": ["st_id"],
        "tpcdi_tax_rate": ["tx_id"],
        "tpcdi_trade_type": ["tt_id"],
        "tpcdi_prospect": ["agencyid"],
    }
    
    def __init__(self, dedup_dir: Path | None = None):
        """Initialize deduplicator.
        
        Args:
            dedup_dir: Directory for dedup index storage
        """
        self.dedup_dir = dedup_dir or DEDUP_DIR
        self.dedup_dir.mkdir(parents=True, exist_ok=True)
        self._indices: dict[str, DedupIndex] = {}
    
    def get_dedup_key(self, source_id: str, record: dict[str, Any]) -> str | None:
        """Get deduplication key for a record.
        
        Args:
            source_id: Source identifier
            record: Data record
            
        Returns:
            Deduplication key string, or None if no key config
        """
        key_fields = self.DEFAULT_KEYS.get(source_id)
        
        if not key_fields:
            return None
        
        dedup_key = DedupKey(source_id=source_id, key_fields=key_fields)
        return dedup_key.compute_key(record)
    
    def filter_duplicates(
        self,
        source_id: str,
        run_id: str,
        records: Iterable[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Filter duplicate records.
        
        Args:
            source_id: Source identifier
            run_id: Run identifier for dedup index
            records: Input records
            
        Returns:
            Tuple of (unique_records, duplicate_records)
        """
        index = self._get_or_create_index(source_id, run_id)
        unique = []
        duplicates = []
        
        for record in records:
            key = self.get_dedup_key(source_id, record)
            
            if key is None:
                # No dedup config - keep all records
                unique.append(record)
                continue
            
            if index.add(key):
                unique.append(record)
            else:
                duplicates.append(record)
        
        return unique, duplicates
    
    def filter_duplicates_streaming(
        self,
        source_id: str,
        run_id: str,
        records: Iterable[dict[str, Any]],
    ) -> Iterable[dict[str, Any]]:
        """Streaming filter that yields only unique records.
        
        Args:
            source_id: Source identifier
            run_id: Run identifier
            records: Input records (generator)
            
        Yields:
            Only unique records
        """
        index = self._get_or_create_index(source_id, run_id)
        
        for record in records:
            key = self.get_dedup_key(source_id, record)
            
            if key is None or index.add(key):
                yield record
    
    def get_stats(self, source_id: str, run_id: str) -> dict[str, Any] | None:
        """Get deduplication statistics for a run.
        
        Args:
            source_id: Source identifier
            run_id: Run identifier
            
        Returns:
            Dedup stats dict or None if not found
        """
        index = self._get_index(source_id, run_id)
        return index.to_dict() if index else None
    
    def save_index(self, source_id: str, run_id: str) -> Path | str:
        """Save dedup index to disk or S3.
        
        Args:
            source_id: Source identifier
            run_id: Run identifier
            
        Returns:
            Path to saved index file (local path or S3 URL)
        """
        index = self._get_index(source_id, run_id)
        if not index:
            raise ValueError(f"No index found for {source_id}/{run_id}")
        
        data = index.to_dict()
        data["seen_keys"] = list(index.seen_keys)
        
        if is_vm_mode():
            # Use S3 storage
            storage = get_governance_storage()
            storage_path = f"dedup/{source_id}/{run_id}.dedup.json"
            return storage.write(storage_path, data, is_json=True)
        else:
            # Use local filesystem
            path = self.dedup_dir / source_id / f"{run_id}.dedup.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
            return path

    def load_index(self, source_id: str, run_id: str) -> DedupIndex | None:
        """Load dedup index from disk or S3.
        
        Args:
            source_id: Source identifier
            run_id: Run identifier
            
        Returns:
            DedupIndex or None if not found
        """
        if is_vm_mode():
            # Use S3 storage
            storage = get_governance_storage()
            storage_path = f"dedup/{source_id}/{run_id}.dedup.json"
            
            if not storage.exists(storage_path):
                return None
            
            try:
                data = storage.read(storage_path)
                seen_keys = set(data.pop("seen_keys", []))
                
                index = DedupIndex(
                    source_id=data["source_id"],
                    run_id=data["run_id"],
                    seen_keys=seen_keys,
                    created_at=data.get("created_at", ""),
                    record_count=data.get("record_count", 0),
                    duplicate_count=data.get("duplicate_count", 0),
                )
                
                self._indices[f"{source_id}/{run_id}"] = index
                return index
            except Exception:
                return None
        else:
            # Use local filesystem
            path = self.dedup_dir / source_id / f"{run_id}.dedup.json"
            
            if not path.exists():
                return None
            
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                seen_keys = set(data.pop("seen_keys", []))
                
                index = DedupIndex(
                    source_id=data["source_id"],
                    run_id=data["run_id"],
                    seen_keys=seen_keys,
                    created_at=data.get("created_at", ""),
                    record_count=data.get("record_count", 0),
                    duplicate_count=data.get("duplicate_count", 0),
                )
                
                self._indices[f"{source_id}/{run_id}"] = index
                return index
            except (json.JSONDecodeError, KeyError):
                return None
    
    def _get_index(self, source_id: str, run_id: str) -> DedupIndex | None:
        """Get index from memory."""
        key = f"{source_id}/{run_id}"
        return self._indices.get(key)
    
    def _get_or_create_index(self, source_id: str, run_id: str) -> DedupIndex:
        """Get existing or create new index."""
        key = f"{source_id}/{run_id}"
        
        if key not in self._indices:
            # Try to load from disk first
            loaded = self.load_index(source_id, run_id)
            if loaded:
                return loaded
            
            self._indices[key] = DedupIndex(
                source_id=source_id,
                run_id=run_id,
            )
        
        return self._indices[key]


# Global instance
_deduplicator: Deduplicator | None = None


def get_deduplicator() -> Deduplicator:
    """Get global deduplicator instance."""
    global _deduplicator
    if _deduplicator is None:
        _deduplicator = Deduplicator()
    return _deduplicator


def dedup_records(
    source_id: str,
    run_id: str,
    records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Convenience function for deduplication.
    
    Args:
        source_id: Source identifier
        run_id: Run identifier
        records: Input records
        
    Returns:
        Tuple of (unique_records, duplicate_records)
    """
    dedup = get_deduplicator()
    return dedup.filter_duplicates(source_id, run_id, records)


def dedup_streaming(
    source_id: str,
    run_id: str,
    records: Iterable[dict[str, Any]],
) -> Iterable[dict[str, Any]]:
    """Convenience function for streaming deduplication.
    
    Args:
        source_id: Source identifier
        run_id: Run identifier
        records: Input records
        
    Yields:
        Only unique records
    """
    dedup = get_deduplicator()
    return dedup.filter_duplicates_streaming(source_id, run_id, records)
