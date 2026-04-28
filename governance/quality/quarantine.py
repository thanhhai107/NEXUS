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
) -> Path:
    """Write invalid records to quarantine so they are not lost or silently loaded."""
    context = GovernanceContext.from_values(batch_id, run_id, source_path, actor)
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    output_path = quarantine_dir / f"{dataset}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.jsonl"

    if using_postgres_storage():
        for item in invalid_records:
            envelope = {
                "dataset": dataset,
                "reason": reason,
                **context.to_event_fields(),
                "quarantined_at": utc_now_iso(),
                "item": dict(item),
            }
            append_governance_event("quarantine", envelope)
        return output_path

    with output_path.open("w", encoding="utf-8", newline="\n") as file:
        for item in invalid_records:
            envelope = {
                "dataset": dataset,
                "reason": reason,
                **context.to_event_fields(),
                "quarantined_at": utc_now_iso(),
                "item": item,
            }
            file.write(json.dumps(envelope, ensure_ascii=False) + "\n")

    return output_path
