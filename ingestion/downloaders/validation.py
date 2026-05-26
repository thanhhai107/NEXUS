from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from common.config import PROJECT_ROOT, load_dataset_catalog
from governance.dlq import record_dlq_event
from governance.quality.quarantine import quarantine_records
from governance.quality.schema import validate_json_schema


def validate_raw_envelope_file(
    *,
    dataset: str,
    raw_path: str | Path,
    run_id: str,
    source: str,
    actor: str = "downloader",
    schema_validation_enabled: bool = False,
    quarantine_invalid_records: bool = True,
) -> dict[str, Any]:
    records = read_jsonl_records(Path(raw_path))
    result: dict[str, Any] = {
        "dataset": dataset,
        "raw_path": str(raw_path),
        "record_count": len(records),
        "schema_validation_enabled": schema_validation_enabled,
        "schema_invalid_records": 0,
        "quarantine_path": None,
    }
    if not schema_validation_enabled:
        return result

    schema = dataset_schema(dataset)
    if not schema:
        result["schema_validation_enabled"] = False
        result["schema_validation_skipped_reason"] = "missing_dataset_schema"
        return result

    invalid_records: list[dict[str, Any]] = []
    for record in records:
        payload = record.get("payload") if isinstance(record, dict) else None
        if not isinstance(payload, dict):
            invalid_records.append(record)
            continue
        valid, _issues = validate_json_schema([payload], schema, max_errors=1)
        if not valid:
            invalid_records.append(record)

    result["schema_invalid_records"] = len(invalid_records)
    if invalid_records and quarantine_invalid_records:
        quarantine_path = quarantine_records(
            dataset,
            invalid_records,
            reason="downloader_json_schema_validation_failed",
            batch_id=run_id,
            run_id=run_id,
            source_path=raw_path,
            actor=actor,
        )
        result["quarantine_path"] = str(quarantine_path)
    return result


def route_parser_failures_to_dlq(
    parser_failure_details: list[dict[str, Any]],
    *,
    dataset: str,
    source: str,
    run_id: str,
    actor: str = "downloader",
) -> int:
    for failure in parser_failure_details:
        record_dlq_event(
            category="download_parser_failed",
            payload=failure,
            source=source,
            dataset=dataset,
            error=str(failure.get("error") or "parser failure"),
            error_type=str(failure.get("error_type") or "ParserError"),
            run_id=run_id,
            source_path=failure.get("path"),
            actor=actor,
        )
    return len(parser_failure_details)


def route_run_failures_to_dlq(run: Any, *, actor: str = "downloader") -> int:
    manifest = read_json(run.run_manifest_path) if run.run_manifest_path.exists() else {}
    routed = 0
    for chunk in manifest.get("chunks", []):
        if chunk.get("status") != "failed":
            continue
        record_dlq_event(
            category="download_chunk_failed",
            payload=chunk,
            source=run.source_id,
            dataset=run.dataset_name,
            error=str(chunk.get("error") or "chunk failed"),
            error_type="DownloadChunkFailure",
            attempts=chunk.get("attempts"),
            run_id=run.run_id,
            source_path=chunk.get("paths", [None])[0] if chunk.get("paths") else None,
            actor=actor,
        )
        routed += 1
    return routed


def route_source_failure_to_dlq(
    *,
    source_id: str,
    dataset: str,
    run_id: str,
    error: Exception,
    actor: str = "downloader",
) -> None:
    record_dlq_event(
        category="download_source_failed",
        payload={"source_id": source_id, "dataset": dataset, "run_id": run_id},
        source=source_id,
        dataset=dataset,
        error=str(error),
        error_type=type(error).__name__,
        run_id=run_id,
        actor=actor,
    )


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    return payload if isinstance(payload, dict) else {}


def read_jsonl_records(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                records.append(payload)
    return records


def dataset_schema(dataset: str) -> dict[str, Any] | None:
    catalog = load_dataset_catalog().get("datasets", {})
    schema_path = (catalog.get(dataset) or {}).get("schema_path")
    if not schema_path:
        return None
    path = Path(str(schema_path))
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
