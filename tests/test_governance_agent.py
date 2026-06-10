from __future__ import annotations

from governance.agents.governance_agent import _rule_decision


def make_evidence(
    status: str = "passed",
    missing: float = 0.0,
    duplicate: float = 0.0,
    freshness: float = 1.0,
    schema_valid: bool = True,
    violations: list[str] | None = None,
    quarantine: int = 0,
    schema_breaking: bool = False,
) -> dict:
    return {
        "dataset_name": "tpcdi_dim_trade",
        "batch_id": "batch-1",
        "quality_report": {
            "status": status,
            "details": {
                "record_count": 10,
                "readiness_score": 0.95,
                "missing_ratio": missing,
                "duplicate_ratio": duplicate,
                "freshness_score": freshness,
                "schema_valid": schema_valid,
                "threshold_violations": violations or [],
            },
        },
        "quarantine_summary": {"quarantine_count": quarantine},
        "schema_history": {"breaking_changes": schema_breaking},
        "audit_events": [],
        "lineage_events": [],
    }


def test_agent_pass_rule() -> None:
    decision = _rule_decision(make_evidence())
    assert decision.decision == "PASS"
    assert decision.reprocess_required is False


def test_agent_warn_rule() -> None:
    assert _rule_decision(make_evidence(missing=0.2)).decision == "WARNING"


def test_agent_fail_rule() -> None:
    assert _rule_decision(make_evidence(status="failed")).decision == "FAIL"


def test_agent_quarantine() -> None:
    decision = _rule_decision(make_evidence(quarantine=2))
    assert decision.decision == "WARNING"
    assert decision.recommended_fixes
