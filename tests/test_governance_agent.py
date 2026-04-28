from __future__ import annotations

from governance.agents.governance_agent import _rule_decision


def make_evidence(readiness: float, status: str = "passed", quarantine: int = 0) -> dict:
    return {
        "dataset_name": "demo",
        "batch_id": "batch-1",
        "quality_report": {
            "status": status,
            "details": {
                "record_count": 10,
                "readiness_score": readiness,
                "missing_ratio": 0.0,
                "duplicate_ratio": 0.0,
                "freshness_score": 1.0,
                "schema_valid": True,
            },
        },
        "quarantine_summary": {"quarantine_count": quarantine},
        "schema_history": {"breaking_changes": False},
        "audit_events": [],
        "lineage_events": [],
    }


def test_agent_pass_rule() -> None:
    decision = _rule_decision(make_evidence(0.95))
    assert decision.decision == "PASS"
    assert decision.reprocess_required is False


def test_agent_warn_rule() -> None:
    assert _rule_decision(make_evidence(0.75)).decision == "WARNING"


def test_agent_fail_rule() -> None:
    assert _rule_decision(make_evidence(0.40)).decision == "FAIL"


def test_agent_quarantine() -> None:
    decision = _rule_decision(make_evidence(0.95, quarantine=2))
    assert decision.decision == "WARNING"
    assert decision.recommended_fixes
