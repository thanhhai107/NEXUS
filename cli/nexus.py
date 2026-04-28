from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from common.config import load_dataset_catalog, load_quality_config
from governance.agents.governance_agent import review_batch
from governance.audit import write_audit_event
from governance.lineage import record_lineage
from governance.quality.auto_fix import apply_auto_fix, normalize_field_name, normalize_field_names
from governance.quality.checks import evaluate_quality_status, run_quality_checks
from governance.quality.metrics import write_quality_metric
from governance.quality.quarantine import quarantine_records
from governance.quality.schema import (
    coerce_records_to_schema,
    normalize_json_schema,
    records_failing_json_schema,
)
from governance.schema_history import save_schema_snapshot
from ingestion.batch.common import read_csv_records, write_jsonl
from ingestion.streaming.producer import default_key, default_url, event_stream


SOURCE_DATASETS = {
    "transport": "transport_events",
    "openaq": "openaq_measurements",
    "waqi": "waqi_air_quality",
    "tfl": "tfl_transport_status",
    "gtfs": "gtfs_realtime_events",
    "singapore": "sg_traffic",
    "education_sim": "education_events",
}


def local_source(dataset_config: dict[str, Any], override: Path | None) -> Path:
    if override:
        return override

    source = dataset_config.get("local_sample_uri") or dataset_config.get("source_uri")
    if not source or str(source).startswith(("http://", "https://", "${")):
        raise ValueError("This runner needs a local CSV source. Pass --source for this dataset.")
    return PROJECT_ROOT / str(source)


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
) -> dict[str, Any]:
    return {
        **result.__dict__,
        "auto_fix": auto_fix_summary,
        "schema_coercion": schema_coercion_summary,
        "thresholds": thresholds,
        "threshold_violations": threshold_violations,
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

    source_path = local_source(dataset_config, args.source)
    records = read_csv_records(source_path)
    auto_fix_result = apply_auto_fix(records, rules.get("auto_fix"))
    json_schema = effective_schema(dataset_config, rules)
    schema_coercion_result = coerce_records_to_schema(auto_fix_result.records, json_schema)
    records = schema_coercion_result.records
    raw_path = write_jsonl(args.dataset, records, str(source_path))
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

    if json_schema:
        save_schema_snapshot(
            args.dataset,
            json_schema,
            batch_id=args.batch_id,
            run_id=args.run_id,
            source_path=source_path,
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
            source_path=source_path,
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
            source_path=source_path,
            actor=args.actor,
        )

    thresholds = dict(quality_config.get("default_rules", {}))
    status, threshold_violations = evaluate_quality_status(result, thresholds)
    details = quality_details(
        result,
        auto_fix_result.summary,
        schema_coercion_result.summary,
        thresholds,
        threshold_violations,
    )
    write_audit_event(
        event_type="quality_check",
        dataset=args.dataset,
        status=status,
        details=details,
        batch_id=args.batch_id,
        run_id=args.run_id,
        source_path=source_path,
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
        source_path=source_path,
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
    details = quality_details(
        result,
        auto_fix_result.summary,
        schema_coercion_result.summary,
        thresholds,
        threshold_violations,
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
    details = quality_details(
        result,
        auto_fix_result.summary,
        schema_coercion_result.summary,
        thresholds,
        threshold_violations,
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

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
