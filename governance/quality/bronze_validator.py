from __future__ import annotations

from pathlib import Path
from typing import Any

from common.data_contract import load_data_contract
from governance.quality.auto_fix import apply_auto_fix, normalize_field_name, normalize_field_names
from governance.quality.checks import evaluate_quality_status, run_quality_checks
from governance.quality.openmetadata import publish_quality_result
from governance.quality.quarantine import quarantine_records
from governance.quality.schema import coerce_records_to_schema, records_failing_json_schema
from governance.schema_drift import compare_schema_drift
from ingestion.batch.common import read_csv_records


def validate_bronze_file(
    *,
    dataset: str,
    source: Path,
    batch_id: str = "manual",
    run_id: str | None = None,
    actor: str | None = None,
    no_exit_on_fail: bool = True,
) -> dict[str, Any]:
    """Validate a Bronze/source file using the configured data contract."""
    contract = load_data_contract(dataset)
    records = read_csv_records(source)
    auto_fix_result = apply_auto_fix(records, contract.auto_fix)
    schema_coercion = coerce_records_to_schema(auto_fix_result.records, contract.schema)
    checked_records = schema_coercion.records
    required_columns = normalize_field_names(contract.required_columns, contract.auto_fix)
    primary_keys = normalize_field_names(contract.primary_keys, contract.auto_fix)
    freshness_column = normalize_field_name(contract.freshness_column or "", contract.auto_fix)

    quality = run_quality_checks(
        dataset=dataset,
        records=checked_records,
        required_columns=required_columns,
        primary_keys=primary_keys,
        freshness_column=freshness_column,
        max_age_hours=contract.max_age_hours,
        json_schema=contract.schema,
    )
    drift = compare_schema_drift(
        contract.schema,
        checked_records,
        required_fields=required_columns,
        primary_keys=primary_keys,
        downstream_fields=contract.semantic_dedup_keys,
    )
    status, threshold_violations = evaluate_quality_status(quality, contract.quality_thresholds)
    if drift.status == "failed":
        status = "failed"
        threshold_violations = [*threshold_violations, "Schema drift policy failed."]

    invalid_records = [
        record
        for record in checked_records
        if any(record.get(column) in (None, "") for column in required_columns)
    ]
    invalid_records.extend(records_failing_json_schema(checked_records, contract.schema))
    if drift.should_quarantine:
        invalid_records.extend(
            {
                "record_key": issue.field_name,
                "issue_category": "schema_drift",
                "issue_code": issue.issue_code,
                "severity": issue.severity,
                "rule_id": f"schema_drift:{issue.field_name}",
                "expected_value": issue.expected_type,
                "actual_value": issue.actual_type,
            }
            for issue in drift.issues
            if issue.action == "quarantine_record"
        )

    quarantine_path = None
    if invalid_records:
        quarantine_path = quarantine_records(
            dataset,
            invalid_records,
            reason="bronze_validation_failed",
            batch_id=batch_id,
            run_id=run_id,
            source_path=source,
            actor=actor,
            layer="bronze",
        )

    details = {
        **quality.__dict__,
        "auto_fix": auto_fix_result.summary,
        "schema_coercion": schema_coercion.summary,
        "schema_drift": drift.to_dict(),
        "threshold_violations": threshold_violations,
        "quarantine_path": str(quarantine_path) if quarantine_path else None,
    }
    openmetadata = publish_quality_result(
        dataset=dataset,
        status=status,
        quality=details,
        batch_id=batch_id,
        run_id=run_id,
        source_path=source,
        actor=actor,
    )
    return {
        "dataset": dataset,
        "source": str(source),
        "status": status,
        "details": details,
        "openmetadata": {
            "published": openmetadata.get("published"),
            "publish_target": openmetadata.get("publish_target"),
        },
        "exit_code": 0 if status == "passed" or no_exit_on_fail else 1,
    }


__all__ = ["validate_bronze_file"]
