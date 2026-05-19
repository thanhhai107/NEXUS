from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Header, HTTPException

from common.config import load_dataset_catalog
from governance.agents.tools import (
    load_agent_decisions as read_agent_decisions,
)
from governance.agents.tools import (
    load_latest_schema_history,
    load_lineage_events,
    load_quality_history,
    load_quality_report,
    load_quarantine_summary,
)
from governance.metadata import dataset_governance_metadata
from governance.policy import (
    evaluate_dataset_access,
    filter_datasets_for_role,
    normalize_role,
)

app = FastAPI(title="NEXUS Metadata API", version="0.1.0")


def load_datasets(role: str | None = None) -> dict[str, Any]:
    datasets = load_dataset_catalog().get("datasets", {})
    if role is None:
        return datasets
    return filter_datasets_for_role(datasets, role, surface="api")


def require_dataset_access(dataset_name: str, role: str) -> None:
    normalized_role = normalize_role(role)
    decision = evaluate_dataset_access(dataset_name, normalized_role, surface="api")
    if not decision.allowed:
        raise HTTPException(
            status_code=403 if decision.reason != "Unknown dataset." else 404,
            detail={
                "dataset": dataset_name,
                "role": normalized_role,
                "reason": decision.reason,
            },
        )


def require_governance_role(role: str) -> None:
    normalized_role = normalize_role(role)
    if normalized_role not in {"admin", "steward"}:
        raise HTTPException(
            status_code=403,
            detail={
                "role": normalized_role,
                "reason": "Governance endpoints require admin or steward role.",
            },
        )


def latest_quality(dataset_name: str) -> dict[str, Any] | None:
    report = load_quality_report(dataset_name)
    return None if report.get("status") == "unknown" else report


def governance_summary(dataset_name: str) -> dict[str, Any]:
    datasets_config = load_datasets(None)
    metadata = datasets_config.get(dataset_name, {})
    governance = dataset_governance_metadata(dataset_name)
    quality_event = latest_quality(dataset_name)
    quality = quality_event.get("details", {}) if quality_event else {}
    agent_decisions = read_agent_decisions(dataset_name)
    agent_decision = agent_decisions[-1] if agent_decisions else None
    quarantine = load_quarantine_summary(dataset_name)
    schema_history = load_latest_schema_history(dataset_name)
    lineage = load_lineage_events(dataset_name)

    return {
        "dataset": dataset_name,
        "domain": metadata.get("domain", "unknown"),
        "source_type": metadata.get("source_type", "unknown"),
        "governance": governance,
        "quality_status": quality_event.get("status") if quality_event else "unknown",
        "readiness_score": quality.get("readiness_score"),
        "missing_ratio": quality.get("missing_ratio"),
        "duplicate_ratio": quality.get("duplicate_ratio"),
        "freshness_score": quality.get("freshness_score"),
        "schema_valid": quality.get("schema_valid"),
        "quarantine_count": quarantine.get("quarantine_count", 0),
        "schema_breaking_changes": schema_history.get("breaking_changes", False),
        "lineage_event_count": len(lineage),
        "agent_decision": agent_decision.get("decision") if agent_decision else None,
        "agent_reason": agent_decision.get("reason") if agent_decision else None,
        "recommended_action": agent_decision.get("recommended_action") if agent_decision else None,
        "reprocess_required": agent_decision.get("reprocess_required") if agent_decision else False,
        "recommended_fixes": agent_decision.get("recommended_fixes", []) if agent_decision else [],
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "nexus-api"}


@app.get("/datasets")
def datasets(role: str = Header(default="public", alias="X-NEXUS-Role")) -> list[dict[str, Any]]:
    return [
        {"name": name, **metadata, "governance": dataset_governance_metadata(name)}
        for name, metadata in load_datasets(role).items()
    ]


@app.get("/datasets/{dataset_name}/quality")
def dataset_quality(
    dataset_name: str,
    role: str = Header(default="public", alias="X-NEXUS-Role"),
) -> dict[str, Any]:
    require_dataset_access(dataset_name, role)
    event = latest_quality(dataset_name)
    if not event:
        return {"dataset": dataset_name, "status": "unknown", "message": "No quality event recorded yet."}
    return {"dataset": dataset_name, "status": event["status"], "quality": event.get("details", {})}


@app.get("/datasets/{dataset_name}/readiness")
def dataset_readiness(
    dataset_name: str,
    role: str = Header(default="public", alias="X-NEXUS-Role"),
) -> dict[str, Any]:
    require_dataset_access(dataset_name, role)
    event = latest_quality(dataset_name)
    readiness = event.get("details", {}).get("readiness_score") if event else None
    return {"dataset": dataset_name, "readiness_score": readiness}


@app.get("/datasets/{dataset_name}/quality-history")
def dataset_quality_history(
    dataset_name: str,
    role: str = Header(default="public", alias="X-NEXUS-Role"),
) -> list[dict[str, Any]]:
    require_dataset_access(dataset_name, role)
    return load_quality_history(dataset_name)


@app.get("/agent/decisions")
def agent_decisions(role: str = Header(default="public", alias="X-NEXUS-Role")) -> list[dict[str, Any]]:
    require_governance_role(role)
    return read_agent_decisions()


@app.get("/datasets/{dataset_name}/agent-decision")
def dataset_agent_decision(
    dataset_name: str,
    role: str = Header(default="public", alias="X-NEXUS-Role"),
) -> dict[str, Any]:
    require_dataset_access(dataset_name, role)
    decisions = read_agent_decisions(dataset_name)
    if not decisions:
        return {"dataset": dataset_name, "status": "unknown", "message": "No agent decision recorded yet."}
    return decisions[-1]


@app.get("/datasets/{dataset_name}/remediation-plan")
def dataset_remediation_plan(
    dataset_name: str,
    role: str = Header(default="public", alias="X-NEXUS-Role"),
) -> dict[str, Any]:
    require_dataset_access(dataset_name, role)
    decisions = read_agent_decisions(dataset_name)
    if not decisions:
        return {"dataset": dataset_name, "status": "unknown", "message": "No agent decision recorded yet."}
    decision = decisions[-1]
    return {
        "dataset": dataset_name,
        "decision": decision.get("decision"),
        "issues": decision.get("issues", []),
        "root_causes": decision.get("root_causes", []),
        "recommended_fixes": decision.get("recommended_fixes", []),
        "reprocess_required": decision.get("reprocess_required", False),
    }


@app.get("/governance/summary")
def all_governance_summaries(
    role: str = Header(default="public", alias="X-NEXUS-Role"),
) -> list[dict[str, Any]]:
    return [
        governance_summary(dataset_name)
        for dataset_name in load_datasets(role)
    ]


@app.get("/datasets/{dataset_name}/governance-summary")
def dataset_governance_summary(
    dataset_name: str,
    role: str = Header(default="public", alias="X-NEXUS-Role"),
) -> dict[str, Any]:
    require_dataset_access(dataset_name, role)
    return governance_summary(dataset_name)
