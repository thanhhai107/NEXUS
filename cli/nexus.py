from __future__ import annotations

import argparse
import csv
import json
import os
import socket
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env", override=True)

from common.config import load_dataset_catalog, load_quality_config, DATASETS_DIR
from common.source_discovery import (
    DEFAULT_OUTPUT_DIR as SOURCE_DISCOVERY_DEFAULT_OUTPUT_DIR,
)
from common.source_discovery import (
    DEFAULT_SOURCE_DIR as SOURCE_DISCOVERY_DEFAULT_SOURCE_DIR,
)
from common.source_discovery import integrate_schema_into_domain
from common.source_discovery import (
    schema_names as source_discovery_schema_names,
)
from common.source_discovery import (
    source_summary as source_discovery_summary,
)
from common.source_discovery import (
    sync_discovery as sync_source_discovery,
)
from common.source_coverage import (
    COVERAGE_MAP_FILE as SOURCE_DISCOVERY_COVERAGE_MAP_FILE,
    write_ingestion_coverage_map,
)
from common.source_registry import get_source, list_sources
from common.data_contract import load_data_contract, list_data_contracts
from common.semantic import (
    build_business_glossary_export,
    build_openmetadata_export,
    load_semantic_contract,
    list_semantic_contracts,
    write_semantic_export,
)
from governance.agents.governance_agent import review_batch
from governance.dlq import list_dlq_events, replay_dlq_events
from governance.entity_resolution import resolve_entities
from governance.audit import write_audit_event
from governance.lineage import record_lineage
from governance.quality.auto_fix import (
    apply_auto_fix,
    normalize_field_name,
    normalize_field_names,
)
from governance.quality.checks import evaluate_quality_status, run_quality_checks
from governance.quality.bronze_validator import validate_bronze_file
from governance.quality.gx_suite import generate_expectation_suite
from governance.quality.metrics import write_quality_metric
from governance.quality.openmetadata import publish_quality_result
from governance.quality.quarantine import quarantine_records
from governance.quality.schema import (
    coerce_records_to_schema,
    normalize_json_schema,
    records_failing_json_schema,
)
from governance.schema_drift import compare_schema_drift
from common.worker import (
    ServiceStatus,
    WorkerHealth,
    check_worker_health,
    read_heartbeats,
    write_heartbeat,
)
from governance.schema_history import save_schema_snapshot
from ingestion.batch.api_ingestion import ingest_api_records
from ingestion.batch.common import read_csv_records, write_jsonl
from ingestion.batch.csv_download_ingestion import download_csv
from ingestion.streaming.producer import default_key, default_url, event_stream

AUTH_STYLES = {
    "openaq_measurements": "x-api-key",
    "ncei_cdo_climate": "token-header",
    "waqi_air_quality": "query-token",
    "openweather_current": "query-appid",
    "tfl_transport_status": "query-app_key",
}


SOURCE_DATASETS = {
    "transport": "transport_events",
    "openaq": "openaq_measurements",
    "waqi": "waqi_air_quality",
    "tfl": "tfl_transport_status",
    "tfl_status": "tfl_transport_status",
    "tfl_arrivals": "tfl_transport_status",
    "tfl_line_status": "tfl_transport_status",
    "gtfs": "gtfs_realtime_events",
    "londonair": "londonair_monitoring",
    "openmeteo": "openmeteo_air_quality",
    "openweather": "openweather_current",
}


def local_source(dataset_config: dict[str, Any], override: Path | None) -> Path:
    if override:
        return override

    source = dataset_config.get("local_sample_uri") or dataset_config.get("source_uri")
    if not source or str(source).startswith(("http://", "https://", "${")):
        raise ValueError("This runner needs a local CSV source. Pass --source for this dataset.")
    return PROJECT_ROOT / str(source)


import re as _re


def _expand_env(value: str) -> str:
    """Expand ${VAR} patterns in a string using os.environ."""
    return _re.sub(
        r"\$\{([^}]+)\}",
        lambda m: os.environ.get(m.group(1), ""),
        value,
    )


def resolve_records(
    dataset_config: dict[str, Any],
    dataset: str,
    override_source: Path | None,
) -> tuple[list[dict[str, str]], str]:
    """Resolve records from the appropriate source based on dataset source_type.

    Returns (records, source_label) where source_label is the path/URL used.
    """
    source_type = dataset_config.get("source_type", "csv")

    if override_source:
        records = read_csv_records(override_source)
        return records, str(override_source)

    if source_type == "csv_download":
        url = _expand_env(dataset_config.get("source_uri", ""))
        if not url:
            raise ValueError(
                f"Dataset {dataset} is csv_download but no valid source_uri found. "
                f"Check .env for the required URL variable."
            )
        downloads_dir = DATASETS_DIR
        downloads_dir.mkdir(parents=True, exist_ok=True)
        csv_path = download_csv(url)
        try:
            records = read_csv_records(csv_path)
            return records, url
        finally:
            csv_path.unlink(missing_ok=True)

    if source_type == "rest_api":
        url = _expand_env(dataset_config.get("source_uri", ""))
        if not url:
            raise ValueError(
                f"Dataset {dataset} is rest_api but no valid source_uri found. "
                f"Check .env for the required URL variable."
            )
        api_key_env = dataset_config.get("api_key_env")
        api_key = os.environ.get(api_key_env) if api_key_env else None
        auth_style = AUTH_STYLES.get(dataset, "bearer")
        records = ingest_api_records(url, api_key, auth_style=auth_style)
        return records, url

    if source_type == "api_stream":
        url = _expand_env(dataset_config.get("source_uri", ""))
        api_key_env = dataset_config.get("api_key_env")
        api_key = os.environ.get(api_key_env) if api_key_env else None
        if url:
            try:
                auth_style = AUTH_STYLES.get(dataset, "bearer")
                records = ingest_api_records(url, api_key, auth_style=auth_style)
                if records:
                    return records, url
            except Exception as exc:
                print(f"Warning: API ingestion failed for {dataset}: {exc}", file=sys.stderr)
        local_path = dataset_config.get("local_sample_uri")
        if local_path:
            source_path = PROJECT_ROOT / str(local_path)
            if source_path.exists():
                return read_csv_records(source_path), str(source_path)

    source_path = local_source(dataset_config, None)
    records = read_csv_records(source_path)
    return records, str(source_path)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                records.append(json.loads(line))
    return records


def load_schema(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    schema_path = PROJECT_ROOT / path
    if not schema_path.exists():
        return None
    with schema_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def load_schema_for_dataset(dataset: str) -> dict[str, Any] | None:
    metadata = load_dataset_catalog().get("datasets", {}).get(dataset, {})
    return load_schema(metadata.get("schema_path"))


def effective_schema(dataset_config: dict[str, Any], rules: dict[str, Any]) -> dict[str, Any] | None:
    return normalize_json_schema(load_schema(dataset_config.get("schema_path")), rules.get("auto_fix"))


def quality_details(
    result: Any,
    auto_fix_summary: dict[str, Any],
    schema_coercion_summary: dict[str, Any],
    thresholds: dict[str, Any],
    threshold_violations: list[str],
    schema_drift: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        **result.__dict__,
        "auto_fix": auto_fix_summary,
        "schema_coercion": schema_coercion_summary,
        "thresholds": thresholds,
        "threshold_violations": threshold_violations,
        "schema_drift": dict(schema_drift or {}),
    }


def schema_invalid_records(records: list[dict[str, Any]], schema: dict[str, Any] | None) -> list[dict[str, Any]]:
    return records_failing_json_schema(records, schema)


def run_batch(args: argparse.Namespace) -> None:
    datasets = load_dataset_catalog().get("datasets", {})
    quality_config = load_quality_config()
    rules = quality_config.get("datasets", {}).get(args.dataset)
    dataset_config = datasets.get(args.dataset)

    if not dataset_config:
        raise SystemExit(f"Unknown dataset: {args.dataset}")
    if not rules:
        raise SystemExit(f"No quality rules configured for dataset: {args.dataset}")

    records, source_label = resolve_records(dataset_config, args.dataset, args.source)
    auto_fix_result = apply_auto_fix(records, rules.get("auto_fix"))
    json_schema = effective_schema(dataset_config, rules)
    schema_coercion_result = coerce_records_to_schema(auto_fix_result.records, json_schema)
    records = schema_coercion_result.records
    raw_path = write_jsonl(args.dataset, records, source_label)
    print(f"Ingested {len(records)} auto-fixed records for dataset={args.dataset} into {raw_path}")

    required_columns = normalize_field_names(rules["required_columns"], rules.get("auto_fix"))
    primary_keys = normalize_field_names(dataset_config.get("primary_keys", []), rules.get("auto_fix"))
    freshness_column = normalize_field_name(rules["freshness_column"], rules.get("auto_fix"))

    result = run_quality_checks(
        dataset=args.dataset,
        records=records,
        required_columns=required_columns,
        primary_keys=primary_keys,
        freshness_column=freshness_column,
        max_age_hours=int(dataset_config.get("freshness_hours", 24)),
        json_schema=json_schema,
    )
    drift_result = compare_schema_drift(
        json_schema,
        records,
        required_fields=required_columns,
        primary_keys=primary_keys,
        downstream_fields=load_data_contract(args.dataset).semantic_dedup_keys,
    )

    if json_schema:
        save_schema_snapshot(
            args.dataset,
            json_schema,
            batch_id=args.batch_id,
            run_id=args.run_id,
            source_path=source_label,
            actor=args.actor,
        )

    invalid_records = [
        record
        for record in records
        if any(record.get(column) in (None, "") for column in required_columns)
    ]
    if invalid_records:
        quarantine_records(
            args.dataset,
            invalid_records,
            reason="missing_required_values",
            batch_id=args.batch_id,
            run_id=args.run_id,
            source_path=source_label,
            actor=args.actor,
        )

    schema_invalid = schema_invalid_records(records, json_schema)
    if schema_invalid:
        quarantine_records(
            args.dataset,
            schema_invalid,
            reason="json_schema_validation_failed",
            batch_id=args.batch_id,
            run_id=args.run_id,
            source_path=source_label,
            actor=args.actor,
        )

    thresholds = dict(quality_config.get("default_rules", {}))
    status, threshold_violations = evaluate_quality_status(result, thresholds)
    if drift_result.status == "failed":
        status = "failed"
        threshold_violations = [*threshold_violations, "Schema drift policy failed."]
    details = quality_details(
        result,
        auto_fix_result.summary,
        schema_coercion_result.summary,
        thresholds,
        threshold_violations,
        drift_result.to_dict(),
    )
    write_audit_event(
        event_type="quality_check",
        dataset=args.dataset,
        status=status,
        details=details,
        batch_id=args.batch_id,
        run_id=args.run_id,
        source_path=source_label,
        actor=args.actor,
    )
    write_quality_metric(
        dataset=args.dataset,
        batch_id=args.batch_id,
        status=status,
        quality=result.__dict__,
        auto_fix=auto_fix_result.summary,
        schema_coercion=schema_coercion_result.summary,
        thresholds=thresholds,
        threshold_violations=threshold_violations,
        run_id=args.run_id,
        source_path=source_label,
        actor=args.actor,
    )
    openmetadata_result = publish_quality_result(
        dataset=args.dataset,
        status=status,
        quality=details,
        batch_id=args.batch_id,
        run_id=args.run_id,
        source_path=source_label,
        actor=args.actor,
    )

    decision = None if args.skip_agent else review_batch(args.dataset, args.batch_id)
    output = {
        "dataset": args.dataset,
        "batch_id": args.batch_id,
        "raw_path": str(raw_path),
        "quality_status": status,
        "quality": details,
        "openmetadata": {
            "published": openmetadata_result.get("published"),
            "publish_target": openmetadata_result.get("publish_target"),
        },
        "agent_decision": decision.to_dict() if decision else None,
    }
    print(json.dumps(output, indent=2))

    if status == "failed" and not args.no_exit_on_fail:
        raise SystemExit(1)


def check_quality(args: argparse.Namespace) -> None:
    records = read_csv(args.source)
    quality_config = load_quality_config()
    rules = quality_config.get("datasets", {}).get(args.dataset, {})
    auto_fix_result = apply_auto_fix(records, rules.get("auto_fix"))
    dataset_config = load_dataset_catalog().get("datasets", {}).get(args.dataset, {})
    json_schema = effective_schema(dataset_config, rules) if dataset_config else None
    schema_coercion_result = coerce_records_to_schema(auto_fix_result.records, json_schema)
    records = schema_coercion_result.records
    required_columns = normalize_field_names(args.required_columns, rules.get("auto_fix"))
    primary_keys = normalize_field_names(args.primary_keys, rules.get("auto_fix"))
    freshness_column = normalize_field_name(args.freshness_column, rules.get("auto_fix"))

    result = run_quality_checks(
        dataset=args.dataset,
        records=records,
        required_columns=required_columns,
        primary_keys=primary_keys,
        freshness_column=freshness_column,
        max_age_hours=args.max_age_hours,
        json_schema=json_schema,
    )
    contract = load_data_contract(args.dataset) if dataset_config else None
    drift_result = compare_schema_drift(
        json_schema,
        records,
        required_fields=required_columns,
        primary_keys=primary_keys,
        downstream_fields=contract.semantic_dedup_keys if contract else (),
    )

    if json_schema:
        save_schema_snapshot(
            args.dataset,
            json_schema,
            batch_id=args.batch_id,
            run_id=args.run_id,
            source_path=args.source,
            actor=args.actor,
        )

    invalid_records = [
        record
        for record in records
        if any(record.get(column) in (None, "") for column in required_columns)
    ]
    if invalid_records:
        quarantine_records(
            args.dataset,
            invalid_records,
            reason="missing_required_values",
            batch_id=args.batch_id,
            run_id=args.run_id,
            source_path=args.source,
            actor=args.actor,
        )

    schema_invalid = schema_invalid_records(records, json_schema)
    if schema_invalid:
        quarantine_records(
            args.dataset,
            schema_invalid,
            reason="json_schema_validation_failed",
            batch_id=args.batch_id,
            run_id=args.run_id,
            source_path=args.source,
            actor=args.actor,
        )

    thresholds = dict(quality_config.get("default_rules", {}))
    thresholds["min_readiness_score"] = args.min_readiness_score
    status, threshold_violations = evaluate_quality_status(result, thresholds)
    if drift_result.status == "failed":
        status = "failed"
        threshold_violations = [*threshold_violations, "Schema drift policy failed."]
    details = quality_details(
        result,
        auto_fix_result.summary,
        schema_coercion_result.summary,
        thresholds,
        threshold_violations,
        drift_result.to_dict(),
    )
    write_audit_event(
        event_type="quality_check",
        dataset=args.dataset,
        status=status,
        details=details,
        batch_id=args.batch_id,
        run_id=args.run_id,
        source_path=args.source,
        actor=args.actor,
    )
    write_quality_metric(
        dataset=args.dataset,
        batch_id=args.batch_id,
        status=status,
        quality=result.__dict__,
        auto_fix=auto_fix_result.summary,
        schema_coercion=schema_coercion_result.summary,
        thresholds=thresholds,
        threshold_violations=threshold_violations,
        run_id=args.run_id,
        source_path=args.source,
        actor=args.actor,
    )
    publish_quality_result(
        dataset=args.dataset,
        status=status,
        quality=details,
        batch_id=args.batch_id,
        run_id=args.run_id,
        source_path=args.source,
        actor=args.actor,
    )

    print(json.dumps(details, indent=2))
    if status == "failed" and not args.no_exit_on_fail:
        raise SystemExit(1)


def check_stream_quality(args: argparse.Namespace) -> None:
    dataset = args.dataset or SOURCE_DATASETS[args.source]
    datasets = load_dataset_catalog().get("datasets", {})
    quality_config = load_quality_config()
    dataset_config = datasets.get(dataset, {})
    rules = quality_config.get("datasets", {}).get(dataset)

    if not rules:
        raise SystemExit(f"No quality rules configured for streaming dataset: {dataset}")

    records = (
        read_jsonl(args.events_jsonl)
        if args.events_jsonl
        else event_stream(
            args.source,
            args.api_url if args.api_url is not None else default_url(args.source),
            args.api_key if args.api_key is not None else default_key(args.source),
            args.sample_events,
        )
    )
    auto_fix_result = apply_auto_fix(records, rules.get("auto_fix"))
    json_schema = effective_schema(dataset_config, rules) if dataset_config else None
    schema_coercion_result = coerce_records_to_schema(auto_fix_result.records, json_schema)
    records = schema_coercion_result.records
    required_columns = normalize_field_names(rules["required_columns"], rules.get("auto_fix"))
    primary_keys = normalize_field_names(dataset_config.get("primary_keys", ["event_id"]), rules.get("auto_fix"))
    freshness_column = normalize_field_name(rules["freshness_column"], rules.get("auto_fix"))

    result = run_quality_checks(
        dataset=dataset,
        records=records,
        required_columns=required_columns,
        primary_keys=primary_keys,
        freshness_column=freshness_column,
        max_age_hours=int(dataset_config.get("freshness_hours", 1)),
        json_schema=json_schema,
    )
    contract = load_data_contract(dataset) if dataset_config else None
    drift_result = compare_schema_drift(
        json_schema,
        records,
        required_fields=required_columns,
        primary_keys=primary_keys,
        downstream_fields=contract.semantic_dedup_keys if contract else (),
    )

    if json_schema:
        save_schema_snapshot(
            dataset,
            json_schema,
            batch_id=args.batch_id,
            run_id=args.run_id,
            source_path=args.events_jsonl or args.api_url or args.source,
            actor=args.actor,
        )

    invalid_records = [
        record
        for record in records
        if any(record.get(column) in (None, "") for column in required_columns)
    ]
    if invalid_records:
        quarantine_records(
            dataset,
            invalid_records,
            reason="stream_missing_required_values",
            batch_id=args.batch_id,
            run_id=args.run_id,
            source_path=args.events_jsonl or args.api_url or args.source,
            actor=args.actor,
        )

    schema_invalid = schema_invalid_records(records, json_schema)
    if schema_invalid:
        quarantine_records(
            dataset,
            schema_invalid,
            reason="json_schema_validation_failed",
            batch_id=args.batch_id,
            run_id=args.run_id,
            source_path=args.events_jsonl or args.api_url or args.source,
            actor=args.actor,
        )

    thresholds = dict(quality_config.get("default_rules", {}))
    status, threshold_violations = evaluate_quality_status(result, thresholds)
    if drift_result.status == "failed":
        status = "failed"
        threshold_violations = [*threshold_violations, "Schema drift policy failed."]
    details = quality_details(
        result,
        auto_fix_result.summary,
        schema_coercion_result.summary,
        thresholds,
        threshold_violations,
        drift_result.to_dict(),
    )
    write_audit_event(
        event_type="streaming_quality_check",
        dataset=dataset,
        status=status,
        details={
            **details,
            "source": args.source,
            "sample_events": len(records),
        },
        batch_id=args.batch_id,
        run_id=args.run_id,
        source_path=args.events_jsonl or args.api_url or args.source,
        actor=args.actor,
    )
    write_quality_metric(
        dataset=dataset,
        batch_id=args.batch_id,
        status=status,
        quality=result.__dict__,
        event_type="streaming_quality_check",
        auto_fix=auto_fix_result.summary,
        schema_coercion=schema_coercion_result.summary,
        thresholds=thresholds,
        threshold_violations=threshold_violations,
        run_id=args.run_id,
        source_path=args.events_jsonl or args.api_url or args.source,
        actor=args.actor,
    )
    publish_quality_result(
        dataset=dataset,
        status=status,
        quality=details,
        batch_id=args.batch_id,
        run_id=args.run_id,
        source_path=args.events_jsonl or args.api_url or args.source,
        actor=args.actor,
    )

    print(json.dumps({
        "dataset": dataset,
        "source": args.source,
        "status": status,
        "quality": details,
    }, indent=2))

    if status == "failed" and not args.no_exit_on_fail:
        raise SystemExit(1)


def review_agent(args: argparse.Namespace) -> None:
    decision = review_batch(args.dataset, args.batch_id)
    print(decision.to_json())


def generate_quality_suite(args: argparse.Namespace) -> None:
    contract = load_data_contract(args.dataset)
    suite = generate_expectation_suite(
        dataset=args.dataset,
        required_columns=contract.required_columns,
        primary_keys=contract.primary_keys,
        freshness_column=contract.freshness_column,
        semantic_rules=contract.semantic.get("dataset_rules"),
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(suite, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"dataset": args.dataset, "output_path": str(args.output)}, indent=2))
    else:
        print(json.dumps(suite, indent=2))


def validate_bronze(args: argparse.Namespace) -> None:
    result = validate_bronze_file(
        dataset=args.dataset,
        source=args.source,
        batch_id=args.batch_id,
        run_id=args.run_id,
        actor=args.actor,
        no_exit_on_fail=args.no_exit_on_fail,
    )
    print(json.dumps(result, indent=2))
    if result["exit_code"]:
        raise SystemExit(result["exit_code"])


def record_lineage_event(args: argparse.Namespace) -> None:
    output_path = record_lineage(
        args.job_name,
        args.inputs,
        args.outputs,
        batch_id=args.batch_id,
        run_id=args.run_id,
        source_path=args.source_path,
        actor=args.actor,
    )
    print(f"Lineage written to {output_path}")


def summarize_source_discovery(args: argparse.Namespace) -> None:
    summary = source_discovery_summary(args.source_dir)
    print(json.dumps(summary, indent=2))


def list_source_discovery_schemas(args: argparse.Namespace) -> None:
    names = source_discovery_schema_names(args.source_dir)
    print(json.dumps({"schema_count": len(names), "schemas": names}, indent=2))


def sync_source_discovery_metadata(args: argparse.Namespace) -> None:
    result = sync_source_discovery(
        source_dir=args.source_dir,
        output_dir=args.output_dir,
        selected_schemas=args.schema,
    )
    print(json.dumps(result, indent=2))


def build_source_discovery_coverage(args: argparse.Namespace) -> None:
    result = write_ingestion_coverage_map(
        output_path=args.output_path,
        source_dir=args.source_dir,
        domains_dir=args.domains_dir,
        config_dir=args.config_dir,
    )
    print(json.dumps(result, indent=2))


def integrate_source_discovery_schema(args: argparse.Namespace) -> None:
    result = integrate_schema_into_domain(
        schema_name=args.schema,
        domain=args.domain,
        dataset=args.dataset,
        source_dir=args.source_dir,
    )
    print(json.dumps(result, indent=2))


def list_registry(args: argparse.Namespace) -> None:
    payload = [entry.to_dict() for entry in list_sources()]
    if args.domain:
        payload = [entry for entry in payload if entry.get("domain") == args.domain]
    print(json.dumps(payload, indent=2))


def show_registry(args: argparse.Namespace) -> None:
    print(json.dumps(get_source(args.dataset).to_dict(), indent=2))


def list_contracts(args: argparse.Namespace) -> None:
    payload = [contract.to_dict() for contract in list_data_contracts()]
    print(json.dumps(payload, indent=2))


def show_contract(args: argparse.Namespace) -> None:
    print(json.dumps(load_data_contract(args.dataset).to_dict(), indent=2))

def list_semantics(args: argparse.Namespace) -> None:
    payload = [contract.to_dict() for contract in list_semantic_contracts()]
    if args.domain:
        payload = [contract for contract in payload if contract.get("domain") == args.domain]
    print(json.dumps(payload, indent=2))

def show_semantic(args: argparse.Namespace) -> None:
    print(json.dumps(load_semantic_contract(args.dataset).to_dict(), indent=2))

def export_semantic(args: argparse.Namespace) -> None:
    dataset_names = args.dataset or None
    if args.kind == "openmetadata":
        payload = build_openmetadata_export(dataset_names, domain=args.domain)
    else:
        payload = build_business_glossary_export(dataset_names, domain=args.domain)
    if args.output:
        output_path = write_semantic_export(payload, args.output)
        print(json.dumps({"output_path": str(output_path), "kind": args.kind}, indent=2))
    else:
        print(json.dumps(payload, indent=2))

def match_semantic_entities(args: argparse.Namespace) -> None:
    records = read_csv_records(args.source)
    result = resolve_entities(
        args.dataset,
        records,
        fuzzy_threshold=args.fuzzy_threshold,
        probabilistic_threshold=args.probabilistic_threshold,
    ).to_dict()
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(json.dumps({
            "dataset": args.dataset,
            "record_count": result["record_count"],
            "crosswalk_count": len(result["crosswalk"]),
            "output_path": str(args.output),
        }, indent=2))
    else:
        print(json.dumps(result, indent=2))

def list_dlq(args: argparse.Namespace) -> None:
    events = list_dlq_events()
    if args.category:
        events = [event for event in events if event.get("category") == args.category]
    if args.source:
        events = [event for event in events if event.get("source") == args.source]
    if args.dataset:
        events = [event for event in events if event.get("dataset") == args.dataset]
    print(json.dumps(events, indent=2))


def replay_dlq(args: argparse.Namespace) -> None:
    if args.target == "kafka":
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )

        def handler(event: dict[str, Any]) -> bool:
            payload = event.get("payload") or event.get("event") or {}
            topic = args.topic or event.get("topic") or event.get("original_topic")
            if not topic:
                return False
            future = producer.send(topic, payload)
            future.get(timeout=10)
            return True

        try:
            result = replay_dlq_events(
                handler,
                category=args.category,
                source=args.source,
                dataset=args.dataset,
            )
        finally:
            producer.flush()
    else:
        def handler(event: dict[str, Any]) -> bool:
            print(json.dumps(event, indent=2))
            return True

        result = replay_dlq_events(
            handler,
            category=args.category,
            source=args.source,
            dataset=args.dataset,
        )
    print(json.dumps(result, indent=2))


def worker_status(args: argparse.Namespace) -> None:
    hostname = args.hostname or socket.gethostname()
    role = "auto" if args.role == "auto" else args.role
    remote = args.ssh or None

    health = check_worker_health(hostname=hostname, role=role, remote_ssh=remote, timeout=args.timeout)

    if args.format == "table":
        label = f" {health.worker_id} ({health.hostname}) " if not remote else f" remote:{remote} "
        print(f"--- Worker Health: {label}---")
        print(f"Role: {health.role}  Reachable: {'YES' if health.is_reachable else 'NO'}  "
              f"Healthy: {health.healthy_count}/{health.total_count}  "
              f"Checked: {health.checked_at}")
        print("-" * 80)
        for svc in health.services:
            mark = "OK" if svc.running else "!!"
            name = (svc.name + " ").ljust(24)
            cid = (svc.container_id or "-").ljust(28)
            uptime_str = f"{svc.uptime_seconds}s" if svc.uptime_seconds is not None else "-"
            print(f"[{mark}] {name} {cid} uptime={uptime_str}")
        print("-" * 80)
        print(f"OVERALL: {'HEALTHY' if health.is_healthy else 'UNHEALTHY'}")
    else:
        print(json.dumps(health.to_dict(), indent=2))

    if not health.is_healthy and not args.no_exit_on_fail:
        raise SystemExit(1)


def worker_heartbeat(_args: argparse.Namespace) -> None:
    payload = write_heartbeat(worker_id=_args.worker_id, ttl_seconds=_args.ttl)
    print(json.dumps(payload, indent=2))


def worker_heartbeats(_args: argparse.Namespace) -> None:
    records = read_heartbeats(max_age_seconds=_args.max_age)
    print(json.dumps(records, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NEXUS operational CLI.")
    subcommands = parser.add_subparsers(dest="command", required=True)

    batch = subcommands.add_parser("batch", help="Batch pipeline commands")
    batch_subcommands = batch.add_subparsers(dest="batch_command", required=True)
    batch_run = batch_subcommands.add_parser("run", help="Run a config-driven batch ingestion")
    batch_run.add_argument("--dataset", required=True)
    batch_run.add_argument("--source", type=Path)
    batch_run.add_argument("--batch-id", default="latest")
    batch_run.add_argument("--run-id")
    batch_run.add_argument("--actor")
    batch_run.add_argument("--skip-agent", action="store_true")
    batch_run.add_argument("--no-exit-on-fail", action="store_true")
    batch_run.set_defaults(func=run_batch)

    quality = subcommands.add_parser("quality", help="Quality check commands")
    quality_subcommands = quality.add_subparsers(dest="quality_command", required=True)
    quality_check = quality_subcommands.add_parser("check", help="Check a flat-file dataset")
    quality_check.add_argument("--dataset", required=True)
    quality_check.add_argument("--source", required=True, type=Path)
    quality_check.add_argument("--required-columns", nargs="+", required=True)
    quality_check.add_argument("--primary-keys", nargs="+", required=True)
    quality_check.add_argument("--freshness-column", required=True)
    quality_check.add_argument("--max-age-hours", type=int, default=24)
    quality_check.add_argument("--min-readiness-score", type=float, default=0.75)
    quality_check.add_argument("--batch-id", default="manual")
    quality_check.add_argument("--run-id")
    quality_check.add_argument("--actor")
    quality_check.add_argument("--no-exit-on-fail", action="store_true")
    quality_check.set_defaults(func=check_quality)

    stream_check = quality_subcommands.add_parser("stream", help="Check a streaming source sample")
    stream_check.add_argument("--source", choices=sorted(SOURCE_DATASETS), default="transport")
    stream_check.add_argument("--dataset")
    stream_check.add_argument("--events-jsonl", type=Path)
    stream_check.add_argument("--sample-events", type=int, default=25)
    stream_check.add_argument("--api-url")
    stream_check.add_argument("--api-key")
    stream_check.add_argument("--batch-id", default="stream-sample")
    stream_check.add_argument("--run-id")
    stream_check.add_argument("--actor")
    stream_check.add_argument("--no-exit-on-fail", action="store_true")
    stream_check.set_defaults(func=check_stream_quality)

    bronze_validate = quality_subcommands.add_parser(
        "bronze-validate",
        help="Validate a Bronze/source file using its configured data contract",
    )
    bronze_validate.add_argument("--dataset", required=True)
    bronze_validate.add_argument("--source", required=True, type=Path)
    bronze_validate.add_argument("--batch-id", default="manual")
    bronze_validate.add_argument("--run-id")
    bronze_validate.add_argument("--actor")
    bronze_validate.add_argument("--no-exit-on-fail", action="store_true")
    bronze_validate.set_defaults(func=validate_bronze)

    gx_suite = quality_subcommands.add_parser(
        "gx-suite",
        help="Generate a Great Expectations suite from a dataset contract",
    )
    gx_suite.add_argument("--dataset", required=True)
    gx_suite.add_argument("--output", type=Path)
    gx_suite.set_defaults(func=generate_quality_suite)

    agent = subcommands.add_parser("agent", help="Governance agent commands")
    agent_subcommands = agent.add_subparsers(dest="agent_command", required=True)
    agent_review = agent_subcommands.add_parser("review", help="Review one dataset batch")
    agent_review.add_argument("--dataset", required=True)
    agent_review.add_argument("--batch-id", required=True)
    agent_review.set_defaults(func=review_agent)

    lineage = subcommands.add_parser("lineage", help="Lineage commands")
    lineage_subcommands = lineage.add_subparsers(dest="lineage_command", required=True)
    lineage_record = lineage_subcommands.add_parser("record", help="Record a lineage event")
    lineage_record.add_argument("--job-name", required=True)
    lineage_record.add_argument("--inputs", nargs="+", required=True)
    lineage_record.add_argument("--outputs", nargs="+", required=True)
    lineage_record.add_argument("--batch-id", default="manual")
    lineage_record.add_argument("--run-id")
    lineage_record.add_argument("--source-path")
    lineage_record.add_argument("--actor")
    lineage_record.set_defaults(func=record_lineage_event)

    source_discovery = subcommands.add_parser(
        "source-discovery",
        help="Inspect and sync generated source discovery metadata",
    )
    source_discovery_subcommands = source_discovery.add_subparsers(
        dest="source_discovery_command",
        required=True,
    )
    source_discovery_summary_command = source_discovery_subcommands.add_parser(
        "summary",
        help="Show discovered source summary",
    )
    source_discovery_summary_command.add_argument(
        "--source-dir",
        type=Path,
        default=SOURCE_DISCOVERY_DEFAULT_SOURCE_DIR,
    )
    source_discovery_summary_command.set_defaults(func=summarize_source_discovery)

    source_discovery_schemas = source_discovery_subcommands.add_parser("schemas", help="List discovered schema names")
    source_discovery_schemas.add_argument(
        "--source-dir",
        type=Path,
        default=SOURCE_DISCOVERY_DEFAULT_SOURCE_DIR,
    )
    source_discovery_schemas.set_defaults(func=list_source_discovery_schemas)

    source_discovery_sync_command = source_discovery_subcommands.add_parser(
        "sync",
        help="Write generated source metadata and JSON Schemas into runtime/source_discovery",
    )
    source_discovery_sync_command.add_argument(
        "--source-dir",
        type=Path,
        default=SOURCE_DISCOVERY_DEFAULT_SOURCE_DIR,
    )
    source_discovery_sync_command.add_argument("--output-dir", type=Path, default=SOURCE_DISCOVERY_DEFAULT_OUTPUT_DIR)
    source_discovery_sync_command.add_argument(
        "--schema",
        action="append",
        help="Schema name to export. Repeat for multiple schemas. Defaults to all schemas.",
    )
    source_discovery_sync_command.set_defaults(func=sync_source_discovery_metadata)

    source_discovery_coverage_command = source_discovery_subcommands.add_parser(
        "coverage",
        help="Build the ingestion coverage map for all discovered sources and schemas",
    )
    source_discovery_coverage_command.add_argument(
        "--source-dir",
        type=Path,
        default=SOURCE_DISCOVERY_DEFAULT_SOURCE_DIR,
    )
    source_discovery_coverage_command.add_argument(
        "--domains-dir",
        type=Path,
        default=PROJECT_ROOT / "domains",
    )
    source_discovery_coverage_command.add_argument(
        "--config-dir",
        type=Path,
        default=PROJECT_ROOT / "config",
    )
    source_discovery_coverage_command.add_argument(
        "--output-path",
        type=Path,
        default=SOURCE_DISCOVERY_DEFAULT_SOURCE_DIR / SOURCE_DISCOVERY_COVERAGE_MAP_FILE,
    )
    source_discovery_coverage_command.set_defaults(func=build_source_discovery_coverage)

    source_discovery_integrate_command = source_discovery_subcommands.add_parser(
        "integrate",
        help="Integrate one discovered schema into domains/<domain>/datasets.yml and schemas/",
    )
    source_discovery_integrate_command.add_argument("--schema", required=True, help="Discovery schema name")
    source_discovery_integrate_command.add_argument("--domain", required=True, help="Domain folder name")
    source_discovery_integrate_command.add_argument("--dataset", required=True, help="Dataset key in catalog")
    source_discovery_integrate_command.add_argument(
        "--source-dir",
        type=Path,
        default=SOURCE_DISCOVERY_DEFAULT_SOURCE_DIR,
    )
    source_discovery_integrate_command.set_defaults(func=integrate_source_discovery_schema)

    registry = subcommands.add_parser("registry", help="Source registry commands")
    registry_subcommands = registry.add_subparsers(dest="registry_command", required=True)
    registry_list = registry_subcommands.add_parser("list", help="List registered sources")
    registry_list.add_argument("--domain")
    registry_list.set_defaults(func=list_registry)
    registry_show = registry_subcommands.add_parser("show", help="Show one source registry entry")
    registry_show.add_argument("--dataset", required=True)
    registry_show.set_defaults(func=show_registry)

    contract = subcommands.add_parser("contract", help="Data contract commands")
    contract_subcommands = contract.add_subparsers(dest="contract_command", required=True)
    contract_list = contract_subcommands.add_parser("list", help="List all data contracts")
    contract_list.set_defaults(func=list_contracts)
    contract_show = contract_subcommands.add_parser("show", help="Show one data contract")
    contract_show.add_argument("--dataset", required=True)
    contract_show.set_defaults(func=show_contract)

    semantic = subcommands.add_parser("semantic", help="Semantic contract commands")
    semantic_subcommands = semantic.add_subparsers(dest="semantic_command", required=True)
    semantic_list = semantic_subcommands.add_parser("list", help="List semantic contracts")
    semantic_list.add_argument("--domain")
    semantic_list.set_defaults(func=list_semantics)
    semantic_show = semantic_subcommands.add_parser("show", help="Show one semantic contract")
    semantic_show.add_argument("--dataset", required=True)
    semantic_show.set_defaults(func=show_semantic)
    semantic_export = semantic_subcommands.add_parser("export", help="Export OpenMetadata or glossary payloads")
    semantic_export.add_argument("--kind", choices=["openmetadata", "glossary"], required=True)
    semantic_export.add_argument("--dataset", action="append", help="Dataset to export. Repeat for multiple datasets.")
    semantic_export.add_argument("--domain", help="Restrict export to one domain.")
    semantic_export.add_argument("--output", type=Path, help="Optional JSON output path.")
    semantic_export.set_defaults(func=export_semantic)
    semantic_match = semantic_subcommands.add_parser("match-entities", help="Create canonical entity IDs and a crosswalk")
    semantic_match.add_argument("--dataset", required=True)
    semantic_match.add_argument("--source", required=True, type=Path)
    semantic_match.add_argument("--output", type=Path, help="Optional JSON output path.")
    semantic_match.add_argument("--fuzzy-threshold", type=float, default=0.88)
    semantic_match.add_argument("--probabilistic-threshold", type=float, default=0.82)
    semantic_match.set_defaults(func=match_semantic_entities)

    dlq = subcommands.add_parser("dlq", help="Dead Letter Queue commands")
    dlq_subcommands = dlq.add_subparsers(dest="dlq_command", required=True)
    dlq_list = dlq_subcommands.add_parser("list", help="List DLQ events from local store")
    dlq_list.add_argument("--category")
    dlq_list.add_argument("--source")
    dlq_list.add_argument("--dataset")
    dlq_list.set_defaults(func=list_dlq)
    dlq_replay = dlq_subcommands.add_parser("replay", help="Replay DLQ events to Kafka or stdout")
    dlq_replay.add_argument("--target", choices=["kafka", "stdout"], default="stdout")
    dlq_replay.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    dlq_replay.add_argument("--topic")
    dlq_replay.add_argument("--category")
    dlq_replay.add_argument("--source")
    dlq_replay.add_argument("--dataset")
    dlq_replay.set_defaults(func=replay_dlq)

    worker = subcommands.add_parser("worker", help="Worker status and health check commands")
    worker_subcommands = worker.add_subparsers(dest="worker_command", required=True)
    worker_status_parser = worker_subcommands.add_parser("status", help="Check worker health status")
    worker_status_parser.add_argument("--hostname", help="Override hostname for display")
    worker_status_parser.add_argument("--role", choices=["master", "worker", "auto"], default="auto")
    worker_status_parser.add_argument("--ssh", help="SSH host to check remote worker (e.g., user@10.0.0.2)")
    worker_status_parser.add_argument("--timeout", type=int, default=30, help="SSH timeout in seconds")
    worker_status_parser.add_argument("--format", choices=["table", "json"], default="table")
    worker_status_parser.add_argument("--no-exit-on-fail", action="store_true")
    worker_status_parser.set_defaults(func=worker_status)

    worker_heartbeat_parser = worker_subcommands.add_parser("heartbeat", help="Send a heartbeat signal")
    worker_heartbeat_parser.add_argument("--worker-id")
    worker_heartbeat_parser.add_argument("--ttl", type=int, default=120, help="Heartbeat TTL in seconds")
    worker_heartbeat_parser.set_defaults(func=worker_heartbeat)

    worker_list_parser = worker_subcommands.add_parser("list", help="List recent worker heartbeats")
    worker_list_parser.add_argument("--max-age", type=int, default=300, help="Max heartbeat age in seconds")
    worker_list_parser.set_defaults(func=worker_heartbeats)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
