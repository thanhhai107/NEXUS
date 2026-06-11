from __future__ import annotations

import argparse
import csv
import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env", override=True)

from common.config import load_dataset_catalog, load_quality_config
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
from ingestion.batch.common import read_csv_records, write_jsonl
from ingestion.data_caterer.runner import (
    DataCatererConfig,
    ERROR_PRESETS,
    generate_all,
    generate_tpcdi,
    list_plans,
    run_plan,
)



def local_source(dataset_config: dict[str, Any], override: Path | None) -> Path:
    if override:
        return override

    source = dataset_config.get("local_sample_uri") or dataset_config.get("source_uri")
    if not source or str(source).startswith(("http://", "https://", "${")):
        raise ValueError("This runner needs a local CSV source. Pass --source for this dataset.")
    return PROJECT_ROOT / str(source)


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


def run_quality_pipeline(
    *,
    records: list[dict[str, Any]],
    dataset: str,
    rules: dict[str, Any],
    dataset_config: dict[str, Any],
    quality_config: dict[str, Any],
    batch_id: str,
    run_id: str | None = None,
    source_path: str | None = None,
    actor: str | None = None,
    event_type: str = "quality_check",
    extra_details: dict[str, Any] | None = None,
    retries: int = 0,
    retry_delay_seconds: int = 5,
) -> tuple[str, dict[str, Any], Any, Any]:
    """Shared quality pipeline: auto-fix → coercion → checks → drift → quarantine → evaluate.

    Returns (status, details, quality_result, drift_result).
    Implements retry with exponential backoff for transient failures.
    """
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return _run_quality_pipeline_inner(
                records=records,
                dataset=dataset,
                rules=rules,
                dataset_config=dataset_config,
                quality_config=quality_config,
                batch_id=batch_id,
                run_id=run_id,
                source_path=source_path,
                actor=actor,
                event_type=event_type,
                extra_details=extra_details,
            )
        except (OSError, IOError, ConnectionError) as exc:
            last_error = exc
            if attempt < retries:
                delay = retry_delay_seconds * (2 ** attempt)
                print(f"Quality pipeline transient error (attempt {attempt+1}/{retries+1}): {exc}. "
                      f"Retrying in {delay}s...", file=sys.stderr)
                time.sleep(delay)
    raise last_error  # type: ignore[misc]


def _run_quality_pipeline_inner(
    *,
    records: list[dict[str, Any]],
    dataset: str,
    rules: dict[str, Any],
    dataset_config: dict[str, Any],
    quality_config: dict[str, Any],
    batch_id: str,
    run_id: str | None = None,
    source_path: str | None = None,
    actor: str | None = None,
    event_type: str = "quality_check",
    extra_details: dict[str, Any] | None = None,
) -> tuple[str, dict[str, Any], Any, Any]:
    auto_fix_result = apply_auto_fix(records, rules.get("auto_fix"))
    json_schema = effective_schema(dataset_config, rules)
    schema_coercion_result = coerce_records_to_schema(auto_fix_result.records, json_schema)
    checked_records = schema_coercion_result.records

    required_columns = normalize_field_names(rules["required_columns"], rules.get("auto_fix"))
    primary_keys = normalize_field_names(dataset_config.get("primary_keys", []), rules.get("auto_fix"))
    freshness_column = normalize_field_name(rules["freshness_column"], rules.get("auto_fix"))

    result = run_quality_checks(
        dataset=dataset,
        records=checked_records,
        required_columns=required_columns,
        primary_keys=primary_keys,
        freshness_column=freshness_column,
        max_age_hours=int(dataset_config.get("freshness_hours", 24)),
        json_schema=json_schema,
    )
    contract = load_data_contract(dataset) if dataset_config else None
    drift_result = compare_schema_drift(
        json_schema,
        checked_records,
        required_fields=required_columns,
        primary_keys=primary_keys,
        downstream_fields=contract.semantic_dedup_keys if contract else (),
    )

    invalid_records = [
        record
        for record in checked_records
        if any(record.get(column) in (None, "") for column in required_columns)
    ]
    if invalid_records:
        quarantine_records(
            dataset,
            invalid_records,
            reason="missing_required_values",
            batch_id=batch_id,
            run_id=run_id,
            source_path=source_path,
            actor=actor,
        )

    schema_invalid = schema_invalid_records(checked_records, json_schema)
    if schema_invalid:
        quarantine_records(
            dataset,
            schema_invalid,
            reason="json_schema_validation_failed",
            batch_id=batch_id,
            run_id=run_id,
            source_path=source_path,
            actor=actor,
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
    if extra_details:
        details = {**details, **extra_details}

    if json_schema:
        save_schema_snapshot(
            dataset,
            json_schema,
            batch_id=batch_id,
            run_id=run_id,
            source_path=source_path,
            actor=actor,
        )

    write_audit_event(
        event_type=event_type,
        dataset=dataset,
        status=status,
        details=details,
        batch_id=batch_id,
        run_id=run_id,
        source_path=source_path,
        actor=actor,
    )
    write_quality_metric(
        dataset=dataset,
        batch_id=batch_id,
        status=status,
        quality=result.__dict__,
        auto_fix=auto_fix_result.summary,
        schema_coercion=schema_coercion_result.summary,
        thresholds=thresholds,
        threshold_violations=threshold_violations,
        run_id=run_id,
        source_path=source_path,
        actor=actor,
    )
    publish_quality_result(
        dataset=dataset,
        status=status,
        quality=details,
        batch_id=batch_id,
        run_id=run_id,
        source_path=source_path,
        actor=actor,
    )

    return status, details, result, drift_result


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
    records = coerce_records_to_schema(
        apply_auto_fix(records, rules.get("auto_fix")).records,
        effective_schema(dataset_config, rules),
    ).records
    raw_path = write_jsonl(args.dataset, records, source_label)
    print(f"Ingested {len(records)} auto-fixed records for dataset={args.dataset} into {raw_path}")

    status, details, _result, _drift = run_quality_pipeline(
        records=records,
        dataset=args.dataset,
        rules=rules,
        dataset_config=dataset_config,
        quality_config=quality_config,
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
        "agent_decision": decision.to_dict() if decision else None,
    }
    print(json.dumps(output, indent=2))

    if status == "failed" and not args.no_exit_on_fail:
        raise SystemExit(1)


def check_quality(args: argparse.Namespace) -> None:
    records = read_csv(args.source)
    quality_config = load_quality_config()
    rules = quality_config.get("datasets", {}).get(args.dataset, {})
    rules = {
        **rules,
        "required_columns": args.required_columns,
        "freshness_column": args.freshness_column,
    }
    dataset_config = load_dataset_catalog().get("datasets", {}).get(args.dataset, {})
    if not dataset_config:
        dataset_config = {}
    dataset_config = {**dataset_config, "primary_keys": args.primary_keys,
                     "freshness_hours": args.max_age_hours}

    records = coerce_records_to_schema(
        apply_auto_fix(records, rules.get("auto_fix")).records,
        effective_schema(dataset_config, rules),
    ).records

    status, details, _result, _drift = run_quality_pipeline(
        records=records,
        dataset=args.dataset,
        rules=rules,
        dataset_config=dataset_config,
        quality_config=quality_config,
        batch_id=args.batch_id,
        run_id=args.run_id,
        source_path=str(args.source),
        actor=args.actor,
    )

    print(json.dumps(details, indent=2))
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


def validate_bronze_tpcdi_cli(args: argparse.Namespace) -> None:
    from governance.quality.bronze_validator import (
        validate_bronze_tpcdi_file,
        validate_bronze_tpcdi_batch,
    )

    previous_scale = os.environ.get("TPCDI_SCALE_FACTOR")
    os.environ["TPCDI_SCALE_FACTOR"] = str(args.scale_factor)
    try:
        if args.all:
            results = validate_bronze_tpcdi_batch(
                batch_id=args.batch,
                chunk_size=args.chunk_size,
                no_exit_on_fail=args.no_exit_on_fail,
            )
        elif args.source_name:
            result = validate_bronze_tpcdi_file(
                source_name=args.source_name,
                batch_id=args.batch,
                chunk_size=args.chunk_size,
                no_exit_on_fail=args.no_exit_on_fail,
            )
            results = [result]
        else:
            print("Error: specify --source-name or --all")
            raise SystemExit(1)
    finally:
        if previous_scale is None:
            os.environ.pop("TPCDI_SCALE_FACTOR", None)
        else:
            os.environ["TPCDI_SCALE_FACTOR"] = previous_scale

    print(json.dumps(results, indent=2))
    exit_code = max(r.get("exit_code", 0) for r in results)
    if exit_code:
        raise SystemExit(exit_code)


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


def generate_tpc_data(args: argparse.Namespace) -> None:
    """Generate TPC-DI benchmark data via Data Caterer."""
    scale = args.scale_factor
    error = args.error_profile
    run_id = args.run_id or f"tpcdi-sf{scale}-{error}"
    dry = args.dry_run

    result = generate_tpcdi(
        scale_factor=scale,
        error_profile=error,
        output_formats=args.output_formats,
        run_id=run_id,
        dry_run=dry,
    )
    print(json.dumps(result, indent=2))


def list_dc_plans(_args: argparse.Namespace) -> None:
    plans = list_plans()
    print(json.dumps({"plans": plans}, indent=2))


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
    quality_check.add_argument("--batch-id", default="manual")
    quality_check.add_argument("--run-id")
    quality_check.add_argument("--actor")
    quality_check.add_argument("--no-exit-on-fail", action="store_true")
    quality_check.set_defaults(func=check_quality)

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

    tpcdi_validate = quality_subcommands.add_parser(
        "tpcdi-validate",
        help="Validate a TPC-DI DIGen source file using streaming chunks",
    )
    tpcdi_validate.add_argument("--source-name", help="Source name in tpcdi_sources.yml")
    tpcdi_validate.add_argument("--batch", default="batch1", help="batch1 | batch2 | batch3")
    tpcdi_validate.add_argument("--scale-factor", type=int, default=3, choices=[3, 10, 50], help="Benchmark scale factor: 3, 10, or 50 (default SF=3)")
    tpcdi_validate.add_argument("--all", action="store_true", help="Validate all sources in batch")
    tpcdi_validate.add_argument("--chunk-size", type=int, default=10000)
    tpcdi_validate.add_argument("--no-exit-on-fail", action="store_true")
    tpcdi_validate.set_defaults(func=validate_bronze_tpcdi_cli)

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

    generate = subcommands.add_parser("generate", help="Generate TPC-DI benchmark data via Data Caterer")
    generate_subcommands = generate.add_subparsers(dest="generate_command", required=True)

    gen_tpc = generate_subcommands.add_parser("tpcdi", help="Generate TPC-DI data")
    gen_tpc.add_argument("--scale-factor", type=int, default=3, choices=[3, 10, 50], help="Benchmark scale factor: 3, 10, or 50 (default SF=3)")
    gen_tpc.add_argument(
        "--error-profile", default="moderate",
        choices=list(ERROR_PRESETS.keys()),
        help="Error injection profile for testing data quality",
    )
    gen_tpc.add_argument(
        "--output-formats", nargs="+", default=["csv", "parquet"],
        choices=["csv", "parquet", "json", "orc"],
        help="Output data formats",
    )
    gen_tpc.add_argument("--run-id", help="Unique run identifier")
    gen_tpc.add_argument("--dry-run", action="store_true", help="Print command without executing")
    gen_tpc.set_defaults(func=generate_tpc_data)

    gen_list = generate_subcommands.add_parser("list-plans", help="List available Data Caterer plans")
    gen_list.set_defaults(func=list_dc_plans)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
