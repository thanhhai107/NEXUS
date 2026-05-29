from __future__ import annotations

from governance.schema_drift import compare_schema_drift


def test_schema_drift_detects_missing_new_rename_and_type_change() -> None:
    result = compare_schema_drift(
        {
            "required": ["id", "event_time"],
            "properties": {
                "id": {"type": "string"},
                "event_time": {"type": "string"},
                "speed": {"type": "number"},
                "status": {"type": "string"},
            },
        },
        [{"id": "evt-1", "event_timestamp": "2026-05-29T00:00:00Z", "speed": {"value": 10}, "new_col": "x"}],
        required_fields=["id", "event_time"],
        primary_keys=["id"],
        downstream_fields=["status"],
        alias_map={"event_time": "event_timestamp"},
    )

    payload = result.to_dict()

    assert payload["status"] == "failed"
    assert payload["should_quarantine"] is True
    assert "event_time" in payload["missing_fields"]
    assert "new_col" in payload["new_fields"]
    assert payload["rename_candidates"][0]["candidate_field"] == "event_timestamp"
    assert any(issue["issue_code"] == "field_type_change" for issue in payload["issues"])
    assert any(issue["issue_code"] == "dropped_downstream_field" for issue in payload["issues"])


def test_schema_drift_allows_warning_for_optional_new_fields() -> None:
    result = compare_schema_drift(
        {"properties": {"id": {"type": "string"}}},
        [{"id": "1", "extra": "preserve me"}],
    )

    assert result.status == "warning"
    assert result.should_quarantine is False
    assert result.issues[0].action == "preserve_in_bronze_and_review_before_promotion"
