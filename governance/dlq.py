from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from common.config import RUNTIME_DIR
from governance.context import GovernanceContext, utc_now_iso
from governance.storage import append_governance_event, using_postgres_storage

DEFAULT_DLQ_DIR = RUNTIME_DIR / "dlq"
DLQ_STREAM = "dlq"


def _dlq_file(category: str, dlq_dir: Path) -> Path:
    dlq_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return dlq_dir / f"{category}_{timestamp}.jsonl"


def record_dlq_event(
    category: str,
    payload: Mapping[str, Any],
    *,
    source: str,
    error: str,
    error_type: str | None = None,
    attempts: int | None = None,
    dataset: str | None = None,
    topic: str | None = None,
    batch_id: str | None = None,
    run_id: str | None = None,
    source_path: str | Path | None = None,
    actor: str | None = None,
    dlq_dir: Path | None = None,
) -> Path:
    dlq_dir = dlq_dir or DEFAULT_DLQ_DIR
    """Capture an operational failure (message/request/job) in the DLQ.

    Use this for failures that are NOT bad data records (those go to quarantine).
    Examples: Kafka publish failed after retries, downstream task crashed,
    consumer could not process a message, ingestion job timed out.
    """
    context = GovernanceContext.from_values(batch_id, run_id, source_path, actor)
    envelope = {
        "category": category,
        "source": source,
        "error": error,
        "error_type": error_type,
        "attempts": attempts,
        "dataset": dataset,
        "topic": topic,
        **context.to_event_fields(),
        "captured_at": utc_now_iso(),
        "payload": dict(payload),
    }

    if using_postgres_storage():
        append_governance_event(DLQ_STREAM, envelope)
        return dlq_dir

    output_path = _dlq_file(category, dlq_dir)
    with output_path.open("a", encoding="utf-8", newline="\n") as file:
        file.write(json.dumps(envelope, ensure_ascii=False) + "\n")
    return output_path


def list_dlq_events(dlq_dir: Path | None = None) -> list[dict[str, Any]]:
    dlq_dir = dlq_dir or DEFAULT_DLQ_DIR
    if not dlq_dir.exists():
        return []
    events: list[dict[str, Any]] = []
    for path in sorted(dlq_dir.glob("*.jsonl")):
        with path.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return events


def replay_dlq_events(
    handler,
    *,
    category: str | None = None,
    source: str | None = None,
    dataset: str | None = None,
    dlq_dir: Path | None = None,
) -> dict[str, Any]:
    dlq_dir = dlq_dir or DEFAULT_DLQ_DIR
    """Iterate DLQ events and call ``handler(event)`` for each match.

    The handler should return True when replay succeeded so it can be archived.
    """
    events = list_dlq_events(dlq_dir)
    matched = 0
    succeeded = 0
    failed: list[dict[str, Any]] = []
    for event in events:
        if category and event.get("category") != category:
            continue
        if source and event.get("source") != source:
            continue
        if dataset and event.get("dataset") != dataset:
            continue
        matched += 1
        try:
            ok = bool(handler(event))
        except Exception as exc:  # noqa: BLE001 - surface to caller
            ok = False
            failed.append({"event": event, "error": f"{type(exc).__name__}: {exc}"})
        if ok:
            succeeded += 1
        elif not failed or failed[-1]["event"] is not event:
            failed.append({"event": event, "error": "handler_returned_false"})
    return {"matched": matched, "succeeded": succeeded, "failed": failed}


__all__ = [
    "DEFAULT_DLQ_DIR",
    "DLQ_STREAM",
    "list_dlq_events",
    "record_dlq_event",
    "replay_dlq_events",
]