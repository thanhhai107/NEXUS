from __future__ import annotations

from datetime import datetime, timezone

from governance.quality.checks import (
    detect_anomalies,
    duplicate_ratio,
    evaluate_quality_status,
    missing_value_ratio,
    readiness_score,
    run_quality_checks,
)
from governance.quality.schema import coerce_records_to_schema, normalize_json_schema


def test_missing_ratio() -> None:
    records = [{"id": "1", "name": "A"}, {"id": "2", "name": ""}]
    assert missing_value_ratio(records, ["id", "name"]) == 0.25


def test_dup_ratio() -> None:
    records = [{"id": "1"}, {"id": "1"}, {"id": "2"}]
    assert duplicate_ratio(records, ["id"]) == 1 / 3


def test_ready_score() -> None:
    assert readiness_score(0.0, 0.0, True, 1.0) == 1.0
    assert readiness_score(1.0, 1.0, False, 0.0) == 0.0


def test_quality_result() -> None:
    now = datetime.now(timezone.utc).isoformat()
    records = [{"id": "1", "name": "A", "updated_at": now}]

    result = run_quality_checks(
        dataset="tpcdi_dim_trade",
        records=records,
        required_columns=["id", "name", "updated_at"],
        primary_keys=["id"],
        freshness_column="updated_at",
        max_age_hours=1,
    )

    assert result.dataset == "tpcdi_dim_trade"
    assert result.schema_valid is True
    assert result.readiness_score == 1.0
    assert result.gx_validation["enabled"] is True
    assert result.gx_validation["success"] is True


def test_great_expectations_validation_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setenv("NEXUS_GX_ENABLED", "false")
    now = datetime.now(timezone.utc).isoformat()

    result = run_quality_checks(
        dataset="tpcdi_dim_trade",
        records=[{"id": "1", "name": "A", "updated_at": now}],
        required_columns=["id", "name", "updated_at"],
        primary_keys=["id"],
        freshness_column="updated_at",
        max_age_hours=1,
    )

    assert result.gx_validation["enabled"] is False
    assert result.gx_validation["success"] is None


def test_great_expectations_reports_failed_expectations(monkeypatch) -> None:
    monkeypatch.setenv("NEXUS_GX_ENABLED", "true")
    now = datetime.now(timezone.utc).isoformat()

    result = run_quality_checks(
        dataset="tpcdi_dim_trade",
        records=[
            {"id": "1", "name": "A", "updated_at": now},
            {"id": "1", "name": "B", "updated_at": now},
        ],
        required_columns=["id", "name", "updated_at"],
        primary_keys=["id"],
        freshness_column="updated_at",
        max_age_hours=1,
    )

    assert result.gx_validation["enabled"] is True
    assert result.gx_validation["success"] is False
    assert "expect_column_values_to_be_unique(column=id)" in result.gx_validation["failed_expectations"]


def test_json_schema_is_enforced_after_type_coercion() -> None:
    schema = {
        "type": "object",
        "required": ["id", "severity"],
        "properties": {
            "id": {"type": "string"},
            "severity": {"type": "integer"},
            "updated_at": {"type": "string", "format": "date-time"},
        },
    }
    now = datetime.now(timezone.utc).isoformat()
    coerced = coerce_records_to_schema(
        [{"id": "1", "severity": "2", "updated_at": now}],
        schema,
    )

    result = run_quality_checks(
        dataset="tpcdi_dim_trade",
        records=coerced.records,
        required_columns=["id", "severity"],
        primary_keys=["id"],
        freshness_column="updated_at",
        max_age_hours=1,
        json_schema=schema,
    )

    assert coerced.records[0]["severity"] == 2
    assert result.schema_valid is True


def test_json_schema_validation_failure_blocks_status() -> None:
    now = datetime.now(timezone.utc).isoformat()
    result = run_quality_checks(
        dataset="tpcdi_dim_trade",
        records=[{"id": "1", "severity": "high", "updated_at": now}],
        required_columns=["id", "severity"],
        primary_keys=["id"],
        freshness_column="updated_at",
        max_age_hours=1,
        json_schema={
            "type": "object",
            "required": ["id", "severity"],
            "properties": {"severity": {"type": "integer"}},
        },
    )
    status, violations = evaluate_quality_status(
        result,
        {
            "max_missing_ratio": 0.0,
            "max_duplicate_ratio": 0.0,
            "min_freshness_score": 1.0,
        },
    )

    assert result.schema_valid is False
    assert status == "failed"
    assert "Schema validation failed." in violations


def test_all_thresholds_are_enforced() -> None:
    result = run_quality_checks(
        dataset="tpcdi_dim_trade",
        records=[
            {"id": "1", "name": "", "updated_at": "2020-01-01T00:00:00+00:00"},
            {"id": "1", "name": "Ada", "updated_at": "2020-01-01T00:00:00+00:00"},
        ],
        required_columns=["id", "name"],
        primary_keys=["id"],
        freshness_column="updated_at",
        max_age_hours=1,
    )

    status, violations = evaluate_quality_status(
        result,
        {
            "max_missing_ratio": 0.0,
            "max_duplicate_ratio": 0.0,
            "min_freshness_score": 1.0,
        },
    )

    assert status == "failed"
    assert len(violations) == 3


def test_schema_normalization_matches_auto_fix_names() -> None:
    schema = {
        "required": ["Start_Time"],
        "properties": {"Start_Time": {"type": "string"}, "Distance(mi)": {"type": "number"}},
    }

    normalized = normalize_json_schema(schema, {"normalize_column_names": True})

    assert normalized["required"] == ["start_time"]
    assert "distance_mi" in normalized["properties"]


def test_anomaly_drop() -> None:
    assert detect_anomalies([0.98, 0.96, 0.60], threshold=0.20) == [
        "Quality score dropped by 0.36 at index 2"
    ]
