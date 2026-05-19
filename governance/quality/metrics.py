from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from common.config import RUNTIME_DIR
from governance.context import GovernanceContext, utc_now_iso
from governance.storage import append_governance_event, read_governance_events

DEFAULT_QUALITY_METRICS_LOG = RUNTIME_DIR / "metrics" / "quality.jsonl"


def write_quality_metric(
    dataset: str,
    batch_id: str,
    status: str,
    quality: Mapping[str, object],
    event_type: str = "quality_check",
    auto_fix: Mapping[str, object] | None = None,
    schema_coercion: Mapping[str, object] | None = None,
    thresholds: Mapping[str, object] | None = None,
    threshold_violations: list[str] | None = None,
    run_id: str | None = None,
    source_path: str | Path | None = None,
    actor: str | None = None,
    metrics_log: Path = DEFAULT_QUALITY_METRICS_LOG,
) -> Path:
    context = GovernanceContext.from_values(batch_id, run_id, source_path, actor)
    payload = {
        "event_type": event_type,
        "dataset": dataset,
        "status": status,
        **context.to_event_fields(),
        "record_count": quality.get("record_count"),
        "readiness_score": quality.get("readiness_score"),
        "missing_ratio": quality.get("missing_ratio"),
        "duplicate_ratio": quality.get("duplicate_ratio"),
        "freshness_score": quality.get("freshness_score"),
        "schema_valid": quality.get("schema_valid"),
        "issues": list(quality.get("issues") or []),
        "auto_fix": dict(auto_fix or {}),
        "schema_coercion": dict(schema_coercion or {}),
        "thresholds": dict(thresholds or {}),
        "threshold_violations": list(threshold_violations or []),
        "timestamp": utc_now_iso(),
    }
    append_governance_event("quality_metrics", payload, metrics_log)
    return metrics_log


def load_quality_history(
    dataset: str,
    limit: int = 20,
    metrics_log: Path = DEFAULT_QUALITY_METRICS_LOG,
) -> list[dict[str, Any]]:
    events = [
        event
        for event in read_governance_events("quality_metrics", metrics_log)
        if event.get("dataset") == dataset
    ]
    return events[-limit:]
