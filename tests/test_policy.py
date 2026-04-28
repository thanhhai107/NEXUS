from __future__ import annotations

from governance.policy import evaluate_dataset_access


def test_public_role_denied_for_internal_dataset() -> None:
    decision = evaluate_dataset_access("education_events", "public", surface="api")

    assert decision.allowed is False
    assert decision.reason == "Role is explicitly denied."


def test_analyst_role_allowed_for_internal_dataset() -> None:
    decision = evaluate_dataset_access("education_events", "analyst", surface="api")

    assert decision.allowed is True
