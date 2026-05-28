from __future__ import annotations

import json

from governance.quality.quarantine import quarantine_records


def test_quarantine_records_writes_audit_fields(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NEXUS_GOVERNANCE_STORAGE", "local")
    records = [
        {
            "id": "evt-1",
            "issue_code": "missing_value",
            "severity": "critical",
            "rule_id": "expect_not_null:id",
            "column_name": "id",
            "expected_value": "non-null",
            "actual_value": None,
        }
    ]

    output = quarantine_records(
        dataset="transport_events",
        invalid_records=records,
        reason="invalid_data",
        quarantine_dir=tmp_path,
        source_name="tfl",
        layer="bronze",
    )

    assert output.exists()
    row = json.loads(output.read_text(encoding="utf-8").splitlines()[0])
    assert row["dataset"] == "transport_events"
    assert row["dataset_name"] == "transport_events"
    assert row["source_name"] == "tfl"
    assert row["layer"] == "bronze"
    assert row["issue_category"] == "data_quality"
    assert row["issue_code"] == "missing_value"
    assert row["severity"] == "critical"
    assert row["rule_id"] == "expect_not_null:id"
    assert row["column_name"] == "id"
    assert row["record_key"] == "evt-1"
    assert row["action_taken"] == "quarantined"
    assert row["status"] == "open"
    assert row["resolved_at"] is None
    assert row["resolver_note"] is None
    assert "raw_payload" in row
