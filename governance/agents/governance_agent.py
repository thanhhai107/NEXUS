from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import boto3

from common.semantic import load_semantic_contract
from governance.agents.decision_schema import AgentDecision
from governance.agents.prompts import build_prompt
from governance.agents.tools import (
    load_latest_audit_events,
    load_latest_schema_history,
    load_lineage_events,
    load_quality_history,
    load_quality_report,
    load_quarantine_summary,
    write_agent_decision,
)
from governance.quality.checks import detect_anomalies


def review_batch(dataset_name: str, batch_id: str) -> AgentDecision:
    evidence = _collect_evidence(dataset_name, batch_id)
    decision = _llm_decision(evidence) or _rule_decision(evidence)
    write_agent_decision(decision)
    return decision


def _collect_evidence(dataset_name: str, batch_id: str) -> dict[str, Any]:
    quality = load_quality_report(dataset_name)
    quarantine = load_quarantine_summary(dataset_name)
    schema = load_latest_schema_history(dataset_name)
    audit_events = load_latest_audit_events(dataset_name)
    lineage = load_lineage_events(dataset_name)
    history = load_quality_history(dataset_name)
    semantic_contract = load_semantic_contract(dataset_name).to_dict()
    readiness_history = [
        float(event["readiness_score"])
        for event in history
        if event.get("readiness_score") is not None
    ]

    return {
        "dataset_name": dataset_name,
        "batch_id": batch_id,
        "quality_report": quality,
        "quarantine_summary": quarantine,
        "schema_history": schema,
        "audit_events": audit_events,
        "lineage_events": lineage,
        "quality_history": history,
        "quality_anomalies": detect_anomalies(readiness_history),
        "semantic_contract": semantic_contract,
    }


def _llm_decision(evidence: dict[str, Any]) -> AgentDecision | None:
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    model = os.getenv("NEXUS_AGENT_MODEL", "amazon.nova-pro-v1:0")

    try:
        client = boto3.client("bedrock-runtime", region_name=region)
    except Exception as exc:
        import sys
        print(f"Warning: Bedrock client creation failed: {exc}", file=sys.stderr)
        return None

    system_prompt = "Return only valid JSON. Do not include markdown."
    user_prompt = build_prompt(evidence)

    try:
        response = client.converse(
            modelId=model,
            system=[{"text": system_prompt}],
            messages=[{"role": "user", "content": [{"text": user_prompt}]}],
            inferenceConfig={"temperature": 0},
        )
        content = response["output"]["message"]["content"][0]["text"]
        parsed = json.loads(content)
        parsed["dataset_name"] = evidence["dataset_name"]
        parsed["batch_id"] = evidence["batch_id"]
        parsed.setdefault("created_at", datetime.now(timezone.utc).isoformat())
        parsed.setdefault("evidence", _compact_evidence(evidence))
        remediation = _remediation_plan(evidence, str(parsed.get("decision", "WARNING")))
        parsed.setdefault("issues", remediation["issues"])
        parsed.setdefault("root_causes", remediation["root_causes"])
        parsed.setdefault("recommended_fixes", remediation["recommended_fixes"])
        parsed.setdefault("reprocess_required", remediation["reprocess_required"])
        return AgentDecision.from_dict(parsed)
    except Exception as exc:
        import sys
        print(f"Warning: LLM decision parsing failed: {exc}", file=sys.stderr)
        return None


def _rule_decision(evidence: dict[str, Any]) -> AgentDecision:
    quality = evidence["quality_report"]
    details = quality.get("details") or {}
    status = quality.get("status", "unknown")
    readiness = _score_100(details.get("readiness_score"))
    record_count = int(details.get("record_count") or 0)
    quarantine_count = int(evidence["quarantine_summary"].get("quarantine_count") or 0)
    schema_breaking = bool(evidence["schema_history"].get("breaking_changes"))

    decision = "PASS"
    confidence = 0.88
    reason = "Quality metadata is acceptable and no blocking governance risk was detected."
    action = "Continue the batch through Bronze, Silver, and Gold processing."

    if status == "failed" or readiness < 50:
        decision = "FAIL"
        confidence = 0.92
        reason = "Quality gate failed or readiness score is below 50."
        action = "Stop the batch and keep invalid data in quarantine for review."
    elif schema_breaking:
        decision = "FAIL"
        confidence = 0.90
        reason = "Schema history indicates breaking changes."
        action = "Stop the batch and review schema changes before loading trusted tables."
    elif readiness < 80:
        decision = "WARNING"
        confidence = 0.86
        reason = "Readiness score is between 50 and 80."
        action = "Continue the batch, but review quality issues before broad consumption."
    elif quarantine_count > 0 and record_count > quarantine_count:
        decision = "WARNING"
        confidence = 0.84
        reason = "Some records were quarantined, but valid records remain."
        action = "Continue the valid records and review quarantined records."
    elif quarantine_count > 0 and record_count <= quarantine_count:
        decision = "FAIL"
        confidence = 0.88
        reason = "All observed records appear to be quarantined."
        action = "Stop the batch and inspect the quarantined records."

    remediation = _remediation_plan(evidence, decision)
    return AgentDecision(
        dataset_name=evidence["dataset_name"],
        batch_id=evidence["batch_id"],
        decision=decision,
        confidence=confidence,
        reason=reason,
        recommended_action=action,
        issues=remediation["issues"],
        root_causes=remediation["root_causes"],
        recommended_fixes=remediation["recommended_fixes"],
        reprocess_required=remediation["reprocess_required"],
        evidence=_compact_evidence(evidence),
    )


def _score_100(value: Any) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return 0.0
    if score <= 1:
        return score * 100
    return score


def _compact_evidence(evidence: dict[str, Any]) -> dict[str, Any]:
    quality = evidence["quality_report"]
    details = quality.get("details") or {}
    quarantine = evidence["quarantine_summary"]
    schema = evidence["schema_history"]
    auto_fix = details.get("auto_fix") or {}
    semantic = evidence.get("semantic_contract") or {}
    semantic_rules = semantic.get("dataset_rules") or {}
    return {
        "audit_status": quality.get("status"),
        "readiness_score": details.get("readiness_score"),
        "missing_ratio": details.get("missing_ratio"),
        "duplicate_ratio": details.get("duplicate_ratio"),
        "freshness_score": details.get("freshness_score"),
        "schema_valid": details.get("schema_valid"),
        "auto_fix": auto_fix,
        "schema_coercion": details.get("schema_coercion") or {},
        "threshold_violations": details.get("threshold_violations") or [],
        "quality_history_count": len(evidence.get("quality_history") or []),
        "quality_anomalies": evidence.get("quality_anomalies") or [],
        "schema_breaking_changes": schema.get("breaking_changes", False),
        "quarantine_count": quarantine.get("quarantine_count", 0),
        "lineage_event_count": len(evidence.get("lineage_events") or []),
        "semantic_rules_present": bool(semantic_rules),
        "semantic_glossary": semantic_rules.get("glossary") or {},
        "semantic_grain": semantic_rules.get("grain") or {},
        "semantic_standards": semantic_rules.get("standards") or {},
    }


def _remediation_plan(evidence: dict[str, Any], decision: str) -> dict[str, Any]:
    quality = evidence["quality_report"]
    details = quality.get("details") or {}
    auto_fix = details.get("auto_fix") or {}
    quarantine = evidence["quarantine_summary"]
    schema = evidence["schema_history"]
    anomalies = evidence.get("quality_anomalies") or []
    threshold_violations = details.get("threshold_violations") or []

    issues: list[str] = []
    root_causes: list[str] = []
    fixes: list[str] = []

    if quality.get("status") == "unknown":
        issues.append("No quality check has been recorded for this dataset.")
        root_causes.append("Missing quality metadata.")
        fixes.append("Run the dataset quality check before agent review.")

    if not bool(details.get("schema_valid", True)):
        issues.append("Required columns are missing from the observed records.")
        root_causes.append("Source schema does not match the configured quality rules.")
        fixes.append("Update the source mapping or quality rules before loading trusted tables.")

    missing_ratio = float(details.get("missing_ratio") or 0)
    if missing_ratio > 0:
        issues.append(f"Missing value ratio is {missing_ratio:.2%}.")
        root_causes.append("Required fields contain null or empty values.")
        fixes.append("Fill missing values through auto_fix or quarantine invalid records.")

    duplicate_ratio = float(details.get("duplicate_ratio") or 0)
    if duplicate_ratio > 0:
        issues.append(f"Duplicate ratio is {duplicate_ratio:.2%}.")
        root_causes.append("Primary key values are not unique in the batch.")
        fixes.append("Deduplicate records before Silver or tighten primary key rules.")

    freshness = details.get("freshness_score")
    if freshness is not None and float(freshness) < 1:
        issues.append(f"Freshness score is {float(freshness):.2%}.")
        root_causes.append("Some records are older than the configured freshness window.")
        fixes.append("Reload from a fresher source or widen the freshness window only if justified.")

    quarantine_count = int(quarantine.get("quarantine_count") or 0)
    if quarantine_count > 0:
        issues.append(f"{quarantine_count} quarantined records exist for this dataset.")
        root_causes.append("Previous quality checks found invalid records.")
        fixes.append("Review quarantine reasons and patch ingestion or source validation.")

    if schema.get("breaking_changes"):
        issues.append("Schema history indicates breaking changes.")
        root_causes.append("The latest schema removed or newly requires fields.")
        fixes.append("Review schema evolution before continuing downstream loads.")

    for violation in threshold_violations:
        issues.append(str(violation))
        root_causes.append("Configured governance quality thresholds were not met.")
        fixes.append("Remediate source quality or update thresholds through governance review.")

    changed_records = int(auto_fix.get("changed_record_count") or 0)
    if changed_records > 0:
        issues.append(f"Auto-fix changed {changed_records} records.")
        root_causes.append("Incoming records required normalization or default values.")
        fixes.append("Review auto_fix summary and promote stable fixes into ingestion mapping.")

    for anomaly in anomalies:
        issues.append(anomaly)
        root_causes.append("Quality history shows a significant readiness score drop.")
        fixes.append("Compare this batch with the previous successful batch before broad consumption.")

    if not issues:
        issues.append("No remediation needed.")
        fixes.append("Continue the batch and monitor future quality history.")

    return {
        "issues": issues,
        "root_causes": _unique(root_causes),
        "recommended_fixes": _unique(fixes),
        "reprocess_required": decision == "FAIL" or bool(schema.get("breaking_changes")),
    }


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output
