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


def validate_bronze_tpcdi_file(
    *,
    source_name: str,
    batch_id: str = "batch1",
    chunk_size: int = 10000,
    no_exit_on_fail: bool = True,
    dataset: str | None = None,
) -> dict[str, Any]:
    """Validate a TPC-DI DIGen source file using streaming chunks.

    Performs per-chunk validation:
    1. File exists → row count
    2. Field count vs expected columns
    3. Type coercion errors (tracked via _parse_errors)
    4. Null ratio per column
    5. Quarantine malformed records

    Designed for Group 1 + Group 2 sources.  No FK/SCD checks yet.
    """
    from common.tpcdi_io import iter_tpcdi_chunks, count_tpcdi_records
    from common.tpcdi_sources import get_source_config, list_source_files, list_sources_for_batch
    from ingestion.tpcdi.parsers.reference import parse_reference, _ALLOWED_REFERENCE_SOURCES
    from ingestion.tpcdi.parsers.datetime_dim import parse_date_dim, parse_time_dim
    from ingestion.tpcdi.parsers.csv_pipe import (
        parse_hr, parse_daily_market, parse_prospect,
        parse_trade, parse_cash_transaction, parse_holding_history,
        parse_watch_history,
    )

    GROUP2_PARSERS = {
        "hr": parse_hr,
        "daily_market": parse_daily_market,
        "prospect": parse_prospect,
        "trade": parse_trade,
        "cash_transaction": parse_cash_transaction,
        "holding_history": parse_holding_history,
        "watch_history": parse_watch_history,
    }

    cfg = get_source_config(source_name)
    expected_columns = len(cfg.get("columns", []))
    use_dataset = dataset or source_name

    total_records = 0
    total_field_errors = 0
    total_type_errors = 0
    null_counts: dict[str, int] = {}
    malformed: list[dict[str, Any]] = []
    files_checked: list[str] = []

    if source_name in _ALLOWED_REFERENCE_SOURCES:
        iter_fn = parse_reference(source_name, batch_id)
    elif source_name == "date":
        iter_fn = parse_date_dim(source_name, batch_id)
    elif source_name == "time":
        iter_fn = parse_time_dim(source_name, batch_id)
    elif source_name in GROUP2_PARSERS:
        iter_fn = GROUP2_PARSERS[source_name](source_name, batch_id)
    else:
        raise ValueError(f"validate_bronze_tpcdi_file: unsupported source '{source_name}'")

    def _chunk_iter(records, cs: int):
        chunk: list[dict[str, Any]] = []
        for record in records:
            chunk.append(record)
            if len(chunk) >= cs:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

    for chunk in _chunk_iter(iter_fn, chunk_size):
        total_records += len(chunk)

        for rec in chunk:
            source_file = rec.get("_source_file", "")
            if source_file not in files_checked:
                files_checked.append(source_file)

            # Field count mismatch
            if rec.get("_parse_error") == "field_count_mismatch":
                total_field_errors += 1
                malformed.append(rec)
                continue

            # Type coercion errors
            parse_errors = rec.get("_parse_errors")
            if parse_errors:
                total_type_errors += 1
                rec["_batch_id"] = batch_id
                rec["_source_name"] = source_name
                rec["_dataset"] = use_dataset
                malformed.append(rec)
                continue

            # Null tracking per column
            for k, v in rec.items():
                if k.startswith("_"):
                    continue
                if v is None or (isinstance(v, str) and v.strip() == ""):
                    null_counts[k] = null_counts.get(k, 0) + 1

    # Quarantine malformed records
    quarantine_path = None
    if malformed:
        quarantine_path = quarantine_records(
            use_dataset,
            malformed,
            reason="bronze_validation_failed",
            batch_id=batch_id,
            source_path=",".join(files_checked) if files_checked else source_name,
            layer="bronze",
        )

    # Build result
    null_ratios = {
        col: round(count / total_records, 4) if total_records > 0 else 0.0
        for col, count in sorted(null_counts.items())
    }

    passed = (total_field_errors == 0 and total_type_errors == 0)
    status = "passed" if passed else "failed"

    details = {
        "source_name": source_name,
        "batch_id": batch_id,
        "files_checked": files_checked,
        "total_records": total_records,
        "field_count_errors": total_field_errors,
        "type_coercion_errors": total_type_errors,
        "null_ratios": null_ratios,
        "quarantine_path": str(quarantine_path) if quarantine_path else None,
    }

    return {
        "dataset": use_dataset,
        "source": source_name,
        "status": status,
        "details": details,
        "exit_code": 0 if passed or no_exit_on_fail else 1,
    }


def validate_bronze_tpcdi_batch(
    *,
    batch_id: str = "batch1",
    chunk_size: int = 10000,
    no_exit_on_fail: bool = True,
) -> list[dict[str, Any]]:
    """Validate all tpc-di sources in a batch."""
    from common.tpcdi_sources import list_sources_for_batch

    results: list[dict[str, Any]] = []
    allowed = {
        "status_type", "trade_type", "tax_rate", "industry", "date", "time",
        "hr", "prospect", "daily_market",
        "trade", "cash_transaction", "holding_history", "watch_history",
    }

    for src in list_sources_for_batch(batch_id):
        if src["name"] not in allowed:
            continue
        result = validate_bronze_tpcdi_file(
            source_name=src["name"],
            batch_id=batch_id,
            chunk_size=chunk_size,
            no_exit_on_fail=no_exit_on_fail,
        )
        results.append(result)

    return results


__all__ = ["validate_bronze_file", "validate_bronze_tpcdi_file", "validate_bronze_tpcdi_batch"]
