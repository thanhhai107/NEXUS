from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from common.config import LOGS_DIR, RUNTIME_DIR
from governance.agents.decision_schema import AgentDecision
from governance.quality.metrics import load_quality_history as read_quality_history
from governance.storage import read_governance_events, using_postgres_storage

LOG_DIR = LOGS_DIR
AUDIT_LOG = LOG_DIR / "audit.jsonl"
LINEAGE_LOG = LOG_DIR / "lineage.jsonl"
AGENT_LOG = LOG_DIR / "agent_decisions.jsonl"
QUARANTINE_DIR = RUNTIME_DIR / "quarantine"
SCHEMA_HISTORY_DIR = RUNTIME_DIR / "schemas" / "history"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if path == AUDIT_LOG:
        return read_governance_events("audit", path)
    if path == LINEAGE_LOG:
        return read_governance_events("lineage", path)
    if path == AGENT_LOG:
        return read_governance_events("agent_decisions", path)

    if not path.exists():
        return []

    events: list[dict[str, Any]] = []
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


def load_latest_audit_events(dataset_name: str) -> list[dict[str, Any]]:
    events = [
        event
        for event in _read_jsonl(AUDIT_LOG)
        if event.get("dataset") == dataset_name
    ]
    return events[-10:]


def load_quality_report(dataset_name: str) -> dict[str, Any]:
    for event in reversed(load_latest_audit_events(dataset_name)):
        if event.get("event_type") in {"quality_check", "streaming_quality_check"}:
            return {
                "status": event.get("status"),
                "timestamp": event.get("timestamp"),
                "details": event.get("details") or {},
            }
    return {"status": "unknown", "details": {}}


def load_quality_history(dataset_name: str, limit: int = 20) -> list[dict[str, Any]]:
    return read_quality_history(dataset_name, limit=limit)


def load_quarantine_summary(dataset_name: str) -> dict[str, Any]:
    if using_postgres_storage():
        items = [
            event
            for event in read_governance_events("quarantine")
            if event.get("dataset") == dataset_name
        ]
        reasons: Counter[str] = Counter(str(item.get("reason", "unknown")) for item in items)
        return {
            "dataset": dataset_name,
            "quarantine_count": len(items),
            "file_count": 0,
            "latest_file": None,
            "reasons": dict(reasons),
        }

    files = sorted(QUARANTINE_DIR.glob(f"{dataset_name}_*.jsonl"))
    reasons: Counter[str] = Counter()
    count = 0
    latest_file: str | None = None

    for path in files:
        latest_file = str(path)
        for item in _read_jsonl(path):
            count += 1
            reasons[str(item.get("reason", "unknown"))] += 1

    return {
        "dataset": dataset_name,
        "quarantine_count": count,
        "file_count": len(files),
        "latest_file": latest_file,
        "reasons": dict(reasons),
    }


def load_latest_schema_history(dataset_name: str) -> dict[str, Any]:
    if using_postgres_storage():
        snapshots = [
            event
            for event in read_governance_events("schema_history")
            if event.get("dataset") == dataset_name
        ]
        snapshots = sorted(snapshots, key=lambda item: str(item.get("captured_at", "")))
        return _schema_history_from_snapshots(dataset_name, snapshots)

    files = sorted(
        SCHEMA_HISTORY_DIR.glob(f"{dataset_name}_*.json"),
        key=lambda path: path.stat().st_mtime,
    )
    if not files:
        return {
            "dataset": dataset_name,
            "schema_history_count": 0,
            "latest": None,
            "previous": None,
            "breaking_changes": False,
            "changes": [],
        }

    def load(path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)

    snapshots = [load(path) for path in files]
    return _schema_history_from_snapshots(dataset_name, snapshots)


def _schema_history_from_snapshots(dataset_name: str, snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    if not snapshots:
        return {
            "dataset": dataset_name,
            "schema_history_count": 0,
            "latest": None,
            "previous": None,
            "breaking_changes": False,
            "changes": [],
        }

    latest = snapshots[-1]
    previous = snapshots[-2] if len(snapshots) > 1 else None
    changes = _schema_changes(previous, latest) if previous else []
    breaking = any(change.get("breaking") for change in changes)
    return {
        "dataset": dataset_name,
        "schema_history_count": len(snapshots),
        "latest": {
            "fingerprint": latest.get("fingerprint"),
            "captured_at": latest.get("captured_at"),
        },
        "previous": {
            "fingerprint": previous.get("fingerprint"),
            "captured_at": previous.get("captured_at"),
        } if previous else None,
        "breaking_changes": breaking,
        "changes": changes,
    }


def _schema_changes(previous: dict[str, Any] | None, latest: dict[str, Any]) -> list[dict[str, Any]]:
    if not previous:
        return []

    prev_schema = previous.get("schema") or {}
    latest_schema = latest.get("schema") or {}
    prev_required = set(prev_schema.get("required") or [])
    latest_required = set(latest_schema.get("required") or [])
    prev_props = set((prev_schema.get("properties") or {}).keys())
    latest_props = set((latest_schema.get("properties") or {}).keys())

    changes: list[dict[str, Any]] = []
    removed_required = sorted(prev_required - latest_required)
    added_required = sorted(latest_required - prev_required)
    removed_props = sorted(prev_props - latest_props)

    if removed_required:
        changes.append({"type": "removed_required_fields", "fields": removed_required, "breaking": True})
    if added_required:
        changes.append({"type": "added_required_fields", "fields": added_required, "breaking": True})
    if removed_props:
        changes.append({"type": "removed_properties", "fields": removed_props, "breaking": True})
    return changes


def load_lineage_events(dataset_name: str) -> list[dict[str, Any]]:
    events = []
    for event in _read_jsonl(LINEAGE_LOG):
        text = json.dumps(event, ensure_ascii=False)
        if dataset_name in text:
            events.append(event)
    return events[-10:]


def write_agent_decision(decision: AgentDecision) -> Path:
    from governance.storage import append_governance_event

    append_governance_event("agent_decisions", decision.to_dict(), AGENT_LOG)
    return AGENT_LOG


def load_agent_decisions(dataset_name: str | None = None) -> list[dict[str, Any]]:
    decisions = _read_jsonl(AGENT_LOG)
    if dataset_name:
        decisions = [
            item
            for item in decisions
            if item.get("dataset_name") == dataset_name
        ]
    return decisions
