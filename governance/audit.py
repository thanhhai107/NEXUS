from __future__ import annotations

from pathlib import Path
from typing import Mapping

from common.config import LOGS_DIR
from governance.context import GovernanceContext, utc_now_iso
from governance.storage import append_governance_event

DEFAULT_AUDIT_LOG = LOGS_DIR / "audit.jsonl"


def write_audit_event(
    event_type: str,
    dataset: str,
    status: str,
    details: Mapping[str, object] | None = None,
    batch_id: str | None = None,
    run_id: str | None = None,
    source_path: str | Path | None = None,
    actor: str | None = None,
    audit_log: Path = DEFAULT_AUDIT_LOG,
) -> Path:
    """Append one governance audit event as JSONL."""
    context = GovernanceContext.from_values(batch_id, run_id, source_path, actor)
    event = {
        "event_type": event_type,
        "dataset": dataset,
        "status": status,
        "details": dict(details or {}),
        **context.to_event_fields(),
        "timestamp": utc_now_iso(),
    }
    append_governance_event("audit", event, audit_log)
    return audit_log
