"""
Error collector — gather detected errors from multiple pipeline sources.

Sources:
1. Bronze validation result (per-record errors)
2. Quarantine records (runtime/lake/quarantine/)
3. Correctness audit violations
4. Runner errors
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def collect_detected_errors(
    scenario_id: str,
    run_id: str,
    *,
    bronze_validation: dict[str, Any] | None = None,
    audit_results: list[dict[str, Any]] | None = None,
    quarantine_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Collect all detected errors from pipeline outputs.

    Returns list of detected_error records (see detected_errors.json schema).
    Each record has:
    - detected_error_id
    - error_type
    - source_name, batch_id, relative_file, physical_line_number
    - detected_stage: bronze_validation | quarantine | audit
    - mutation_id (nếu match được)
    """
    errors: list[dict[str, Any]] = []
    counter = 0

    # 1. Bronze validation per-record errors
    if bronze_validation:
        errors.extend(_parse_bronze_errors(bronze_validation, scenario_id, run_id))

    # 2. Quarantine records
    if quarantine_root:
        qpath = Path(quarantine_root)
        for f in sorted(qpath.rglob("*.jsonl")):
            for line in f.open(encoding="utf-8"):
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                counter += 1
                errors.append({
                    "detected_error_id": f"det-{counter:06d}",
                    "scenario_id": scenario_id,
                    "run_id": run_id,
                    "mutation_id": rec.get("mutation_id"),
                    "source_name": rec.get("source_name", ""),
                    "batch_id": rec.get("batch_id", ""),
                    "relative_file": rec.get("source_file", ""),
                    "physical_line_number": rec.get("record_number"),
                    "logical_record_number": rec.get("record_number"),
                    "error_type": rec.get("error_type") or rec.get("_parse_errors", "unknown"),
                    "field": rec.get("field", ""),
                    "original_value": rec.get("original_value", ""),
                    "detected_stage": "quarantine",
                })

    # 3. Audit violations
    if audit_results:
        for audit in audit_results:
            if audit.get("status") != "FAIL":
                continue
            audit_name = audit.get("audit") or audit.get("audit_name") or "audit_failure"

            # Map audit names to canonical error types
            if "pk_duplicate" in audit_name:
                error_type = "duplicate_primary_key"
            elif "row_count" in audit_name:
                error_type = "row_count_mismatch"
            elif "trade_holding" in audit_name:
                error_type = "trade_holding_mismatch"
            elif "prospect_customer" in audit_name:
                error_type = "prospect_customer_overlap"
            else:
                error_type = audit_name

            for violation in audit.get("violations", []):
                counter += 1
                errors.append({
                    "detected_error_id": f"det-{counter:06d}",
                    "scenario_id": scenario_id,
                    "run_id": run_id,
                    "mutation_id": violation.get("mutation_id"),
                    "source_name": violation.get("table", ""),
                    "batch_id": "batch1",
                    "relative_file": violation.get("table", ""),
                    "physical_line_number": violation.get("line_number") or violation.get("pk_value", ""),
                    "logical_record_number": None,
                    "error_type": error_type,
                    "field": violation.get("pk_column") or violation.get("issue", ""),
                    "original_value": str(violation.get("pk_value", "")),
                    "detected_stage": "audit",
                })

    return errors


def _parse_bronze_errors(
    result: dict[str, Any],
    scenario_id: str,
    run_id: str,
) -> list[dict[str, Any]]:
    """Extract per-record errors from bronze validation result.

    Maps aggregate counts from ``validate_bronze_tpcdi_file`` output to
    canonical detected_error records.  Covers Phase 1 and Phase 2 mutation
    types that produce coercion / field-count failures at bronze.

    Canonical error_type values and the mutations that produce them:
      field_count_mismatch — missing_field, extra_field, partial_file,
                             poison_record (after utf-8 replace)
      type_coercion_error  — type_error, null_required_field (\\N marker),
                             invalid_format
    """
    errors: list[dict[str, Any]] = []
    details = result.get("details", {})
    counter = 0
    source_name = details.get("source_name", "unknown")
    batch_id = details.get("batch_id", "batch1")

    fce = details.get("field_count_errors", 0)
    tce = details.get("type_coercion_errors", 0)

    for _ in range(fce):
        counter += 1
        errors.append({
            "detected_error_id": f"bronze-{counter:06d}",
            "scenario_id": scenario_id,
            "run_id": run_id,
            "source_name": source_name,
            "batch_id": batch_id,
            "relative_file": f"{source_name}.txt",
            "error_type": "field_count_mismatch",
            "detected_stage": "bronze_validation",
        })
    for _ in range(tce):
        counter += 1
        errors.append({
            "detected_error_id": f"bronze-{counter:06d}",
            "scenario_id": scenario_id,
            "run_id": run_id,
            "source_name": source_name,
            "batch_id": batch_id,
            "relative_file": f"{source_name}.txt",
            "error_type": "type_coercion_error",
            "detected_stage": "bronze_validation",
        })
    return errors


def write_detected_errors(errors: list[dict[str, Any]], path: Path) -> None:
    """Write detected_errors.json."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "errors": errors,
        "total": len(errors),
    }
    path.write_text(json.dumps(data, indent=2, default=str))
