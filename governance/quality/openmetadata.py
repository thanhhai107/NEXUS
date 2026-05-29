from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

from common.config import LOGS_DIR
from governance.context import GovernanceContext, utc_now_iso
from governance.storage import append_governance_event

DEFAULT_OPENMETADATA_DQ_LOG = LOGS_DIR / "openmetadata_dq.jsonl"


def build_quality_result_payload(
    *,
    dataset: str,
    status: str,
    quality: Mapping[str, Any],
    batch_id: str | None = None,
    run_id: str | None = None,
    source_path: str | Path | None = None,
    actor: str | None = None,
) -> dict[str, Any]:
    context = GovernanceContext.from_values(batch_id, run_id, source_path, actor)
    return {
        "tool": "OpenMetadata",
        "entity_type": "testCaseResult",
        "dataset": dataset,
        "status": status,
        **context.to_event_fields(),
        "timestamp": utc_now_iso(),
        "testCaseResult": {
            "testCaseName": f"{dataset}.data_quality_contract",
            "testCaseStatus": "Success" if status == "passed" else "Failed",
            "result": {
                "record_count": quality.get("record_count"),
                "readiness_score": quality.get("readiness_score"),
                "missing_ratio": quality.get("missing_ratio"),
                "duplicate_ratio": quality.get("duplicate_ratio"),
                "freshness_score": quality.get("freshness_score"),
                "schema_valid": quality.get("schema_valid"),
                "issues": list(quality.get("issues") or []),
                "gx_validation": dict(quality.get("gx_validation") or {}),
            },
        },
    }


def publish_quality_result(
    *,
    dataset: str,
    status: str,
    quality: Mapping[str, Any],
    batch_id: str | None = None,
    run_id: str | None = None,
    source_path: str | Path | None = None,
    actor: str | None = None,
    log_path: Path = DEFAULT_OPENMETADATA_DQ_LOG,
) -> dict[str, Any]:
    """Record an OpenMetadata-compatible DQ payload and optionally POST it."""
    payload = build_quality_result_payload(
        dataset=dataset,
        status=status,
        quality=quality,
        batch_id=batch_id,
        run_id=run_id,
        source_path=source_path,
        actor=actor,
    )
    append_governance_event("openmetadata_dq", payload, log_path)
    endpoint = os.getenv("OPENMETADATA_DQ_ENDPOINT", "").strip()
    if not endpoint:
        return {**payload, "published": False, "publish_target": "local_log"}

    try:
        import requests

        headers = {"Content-Type": "application/json"}
        token = os.getenv("OPENMETADATA_AUTH_TOKEN", "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        response = requests.post(endpoint, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
    except Exception as exc:
        return {
            **payload,
            "published": False,
            "publish_target": endpoint,
            "publish_error": f"{type(exc).__name__}: {exc}",
        }
    return {**payload, "published": True, "publish_target": endpoint}


__all__ = [
    "build_quality_result_payload",
    "publish_quality_result",
]
