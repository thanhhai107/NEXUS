from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from common.config import RUNTIME_DIR
from governance.context import GovernanceContext, utc_now_iso
from governance.storage import append_governance_event, using_postgres_storage

DEFAULT_QUARANTINE_DIR = RUNTIME_DIR / "quarantine"


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
) -> Path:
    """Write invalid records to quarantine so they are not lost or silently loaded."""
    context = GovernanceContext.from_values(batch_id, run_id, source_path, actor)
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    output_path = quarantine_dir / f"{dataset}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.jsonl"

    detected_at = utc_now_iso()
    use_postgres = using_postgres_storage()

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

    if use_postgres:
        for item in invalid_records:
            envelope = build_envelope(item)
            append_governance_event("quarantine", envelope)
        return output_path

    with output_path.open("a", encoding="utf-8", newline="\n") as file:
        for item in invalid_records:
            envelope = build_envelope(item)
            file.write(json.dumps(envelope, ensure_ascii=False) + "\n")

    return output_path
