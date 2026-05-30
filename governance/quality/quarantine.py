"""Quarantine Module.

Writes invalid records to quarantine so they are not lost or silently loaded.
Supports local filesystem, PostgreSQL, and S3/MinIO storage.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from common.config import RUNTIME_DIR, is_vm_mode
from common.storage import get_storage
from governance.context import GovernanceContext, utc_now_iso
from governance.storage import append_governance_event, using_postgres_storage


DEFAULT_QUARANTINE_DIR = RUNTIME_DIR / "quarantine"


def _get_quarantine_storage_path(dataset: str) -> str:
    """Get S3 storage path for quarantine."""
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    return f"quarantine/{dataset}_{stamp}.jsonl"


def quarantine_records(
    dataset: str,
    invalid_records: Iterable[Mapping[str, object]],
    reason: str,
    batch_id: str | None = None,
    run_id: str | None = None,
    source_path: str | Path | None = None,
    actor: str | None = None,
    quarantine_dir: Path = DEFAULT_QUARANTINE_DIR,
    source_name: str | None = None,
    layer: str = "bronze",
) -> Path | str:
    """Write invalid records to quarantine.
    
    Args:
        dataset: Dataset name
        invalid_records: Records to quarantine
        reason: Reason for quarantine
        batch_id: Batch identifier
        run_id: Run identifier
        source_path: Source file path
        actor: Actor who detected
        quarantine_dir: Quarantine directory (local mode)
        source_name: Source name
        layer: Data layer (bronze/silver/gold)
        
    Returns:
        Path (local) or S3 URL
    """
    context = GovernanceContext.from_values(batch_id, run_id, source_path, actor)
    detected_at = utc_now_iso()
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    
    def build_envelope(item: Mapping[str, object]) -> dict[str, object]:
        item_dict = dict(item)
        issue_category = str(item_dict.get("issue_category", "data_quality"))
        issue_code = str(item_dict.get("issue_code", reason))
        severity = str(item_dict.get("severity", "high"))
        rule_id = str(item_dict.get("rule_id", "unknown_rule"))
        record_key = str(item_dict.get("record_key", item_dict.get("id", "unknown")))
        expected_value = item_dict.get("expected_value")
        actual_value = item_dict.get("actual_value")
        action_taken = str(item_dict.get("action_taken", "quarantined"))
        status = str(item_dict.get("status", "open"))
        return {
            "dataset": dataset,
            "dataset_name": dataset,
            "source_name": source_name,
            "layer": layer,
            "reason": reason,
            **context.to_event_fields(),
            "quarantined_at": detected_at,
            "detected_at": detected_at,
            "record_key": record_key,
            "issue_category": issue_category,
            "issue_code": issue_code,
            "severity": severity,
            "rule_id": rule_id,
            "column_name": item_dict.get("column_name"),
            "expected_value": expected_value,
            "actual_value": actual_value,
            "action_taken": action_taken,
            "status": status,
            "resolved_at": item_dict.get("resolved_at"),
            "resolver_note": item_dict.get("resolver_note"),
            "raw_payload": json.dumps(item_dict, ensure_ascii=False),
            "item": item_dict,
        }
    
    # Try PostgreSQL first
    if using_postgres_storage():
        for item in invalid_records:
            envelope = build_envelope(item)
            append_governance_event("quarantine", envelope)
        return str(quarantine_dir / f"{dataset}_{stamp}.jsonl")
    
    # Try S3 if in VM mode
    if is_vm_mode():
        storage = get_storage()
        storage_path = _get_quarantine_storage_path(dataset)
        
        # Read existing content or create new
        existing_content = b""
        if storage.exists(storage_path):
            existing_content = storage.read_bytes(storage_path)
        
        # Build all lines
        lines = []
        for item in invalid_records:
            envelope = build_envelope(item)
            lines.append(json.dumps(envelope, ensure_ascii=False))
        
        content = existing_content + "\n".join(lines).encode("utf-8") + b"\n"
        result = storage.write_bytes(storage_path, content)
        return result
    
    # Fall back to local filesystem
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    output_path = quarantine_dir / f"{dataset}_{stamp}.jsonl"
    
    with output_path.open("a", encoding="utf-8", newline="\n") as file:
        for item in invalid_records:
            envelope = build_envelope(item)
            file.write(json.dumps(envelope, ensure_ascii=False) + "\n")
    
    return output_path
