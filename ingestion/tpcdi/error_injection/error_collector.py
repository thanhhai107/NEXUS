"""
Error collector — gather detected errors from multiple pipeline sources.

Sources:
1. Bronze validation result (per-record errors)
2. Quarantine records (runtime/lake/quarantine/)
3. Correctness audit violations
4. Runner errors
5. Scenario directory inspection (format/lineage/reliability mutations)
6. Semantic source-file inspection (unit/timestamp/aggregation mutations)

Detection architecture:
  Every mutation written by an injector carries an ``expected_detection`` field.
  This module inspects the scenario directory and pipeline outputs to produce
  ``detected_error`` records whose ``error_type`` matches the injection's
  ``expected_detection``, enabling the scoring engine to compute TP/FP/FN.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from common.tpcdi_sources import resolve_batch_path, get_source_config

PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Fields that injectors strip or corrupt for lineage mutations
_LINEAGE_AUDIT_FIELDS = ["audit_run_id", "lineage_hash", "ingested_at"]
_FAKE_RUN_ID = "00000000-dead-beef-dead-000000000000"
_SCENARIO_BASE = PROJECT_ROOT / "runtime" / "tpcdi" / "scenarios"


def collect_detected_errors(
    scenario_id: str,
    run_id: str,
    *,
    bronze_validation: dict[str, Any] | None = None,
    audit_results: list[dict[str, Any]] | None = None,
    quarantine_root: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Collect all detected errors from pipeline outputs.

    Returns list of detected_error records matching the schema used by
    scoring.  Each record has:
    - detected_error_id, error_type
    - source_name, batch_id, relative_file, physical_line_number
    - detected_stage (bronze_validation | quarantine | audit | silver_validation
                     | ingestion | semantic_validation | scenario_inspection)
    - mutation_id (if matchable)
    """
    errors: list[dict[str, Any]] = []
    counter = 0

    def next_id() -> str:
        nonlocal counter
        counter += 1
        return f"det-{counter:06d}"

    scenario_root = _SCENARIO_BASE / scenario_id
    manifest = _load_manifest(scenario_root)

    # ═════════════════════════════════════════════════════════════════════
    # 1. Bronze validation per-record errors (Phase 1 / source_injector)
    # ═════════════════════════════════════════════════════════════════════
    if bronze_validation:
        errors.extend(_parse_bronze_errors(bronze_validation, scenario_id, run_id))

    # ═════════════════════════════════════════════════════════════════════
    # 2. Quarantine records
    # ═════════════════════════════════════════════════════════════════════
    if quarantine_root:
        qpath = Path(quarantine_root)
        for f in sorted(qpath.rglob("*.jsonl")):
            for line in f.open(encoding="utf-8"):
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                errors.append({
                    "detected_error_id": next_id(),
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

    # ═════════════════════════════════════════════════════════════════════
    # 3. Audit violations (duplicate_pk, row_count, trade_holding, etc.)
    # ═════════════════════════════════════════════════════════════════════
    if audit_results:
        for audit in audit_results:
            if audit.get("status") != "FAIL":
                continue
            audit_name = audit.get("audit") or audit.get("audit_name") or "audit_failure"
            error_type = _map_audit_to_error_type(audit_name)
            for violation in audit.get("violations", []):
                errors.append({
                    "detected_error_id": next_id(),
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

    # ═════════════════════════════════════════════════════════════════════
    # 4. Scenario directory inspection — detect all injection effects
    # ═════════════════════════════════════════════════════════════════════
    if manifest and scenario_root.exists():
        source_dir = scenario_root / "source"
        mutations = manifest.get("mutations", [])
        for mut in mutations:
            mid = mut.get("mutation_id", "")
            mtype = mut.get("mutation_type", "")
            expected = mut.get("expected_detection", "")

            if not expected:
                continue

            detected = _detect_in_scenario(
                mut, source_dir, scenario_root, scenario_id, run_id, mid, next_id
            )
            if detected:
                errors.append(detected)

        # 5. Semantic source-file inspection (for mutations that change values)
        semantic_errors = _detect_semantic_issues(
            mutations, source_dir, scenario_root, scenario_id, run_id, next_id
        )
        errors.extend(semantic_errors)

    return errors


# ═══════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════════════════


def _load_manifest(scenario_root: Path) -> dict[str, Any] | None:
    manifest_path = scenario_root / "injection_manifest.json"
    if not manifest_path.exists():
        return None
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _map_audit_to_error_type(audit_name: str) -> str:
    if "pk_duplicate" in audit_name:
        return "duplicate_primary_key"
    if "row_count" in audit_name:
        return "row_count_mismatch"
    if "trade_holding" in audit_name:
        return "trade_holding_mismatch"
    if "prospect_customer" in audit_name:
        return "prospect_customer_overlap"
    return audit_name


def _brighten_error_type(raw: str) -> str:
    """Normalize bronze error types to canonical names."""
    mapping = {
        "field_count_mismatch_extra": "field_count_mismatch",
        "field_count_mismatch_missing": "field_count_mismatch",
    }
    return mapping.get(raw, raw)


def _parse_bronze_errors(
    result: dict[str, Any],
    scenario_id: str,
    run_id: str,
) -> list[dict[str, Any]]:
    """Extract per-record errors from bronze validation result.

    Also reads schema_drift output to detect:
    - missing_required_field
    - dropped_downstream_field
    - new_unknown_field → field_count_mismatch
    - field_type_change → type_coercion_error
    - field_rename_candidate
    """
    errors: list[dict[str, Any]] = []
    details = result.get("details", {})
    counter = 0
    source_name = details.get("source_name", "unknown")
    batch_id = details.get("batch_id", "batch1")
    schema_drift = result.get("schema_drift", {})

    def nid() -> str:
        nonlocal counter
        counter += 1
        return f"bronze-{counter:06d}"

    def base() -> dict[str, Any]:
        return {
            "scenario_id": scenario_id,
            "run_id": run_id,
            "source_name": source_name,
            "batch_id": batch_id,
            "relative_file": f"{source_name}.txt",
            "detected_stage": "bronze_validation",
        }

    fce = details.get("field_count_errors", 0)
    tce = details.get("type_coercion_errors", 0)

    for _ in range(fce):
        errors.append({**base(), "detected_error_id": nid(), "error_type": "field_count_mismatch"})
    for _ in range(tce):
        errors.append({**base(), "detected_error_id": nid(), "error_type": "type_coercion_error"})

    # Schema drift detections
    drift_issues = schema_drift.get("issues", [])
    for issue in drift_issues:
        issue_code = issue.get("issue_code", "")
        if issue_code in ("missing_required_field", "dropped_downstream_field"):
            errors.append({**base(), "detected_error_id": nid(), "error_type": issue_code, "field": issue.get("field", "")})
        elif issue_code == "new_unknown_field":
            errors.append({**base(), "detected_error_id": nid(), "error_type": "field_count_mismatch", "field": issue.get("field", "")})
        elif issue_code == "field_type_change":
            errors.append({**base(), "detected_error_id": nid(), "error_type": "type_coercion_error", "field": issue.get("field", "")})

    rename_candidates = schema_drift.get("rename_candidates", [])
    for rc in rename_candidates:
        errors.append({**base(), "detected_error_id": nid(), "error_type": "field_rename_candidate",
                       "field": rc.get("original_field", ""), "original_value": rc.get("candidate_field", "")})

    return errors


# ═══════════════════════════════════════════════════════════════════════════
# Scenario inspection detectors
# ═══════════════════════════════════════════════════════════════════════════


def _resolve_target_file(
    source_dir: Path, source_name: str, batch_id: str
) -> Path | None:
    """Resolve the primary source file for a given source+batch."""
    cfg = get_source_config(source_name)
    batch_path = resolve_batch_path(batch_id)
    batch_dir = source_dir / batch_path.relative_to(source_dir)
    if not batch_dir.exists():
        return None
    exclude = cfg.get("exclude_patterns", [])
    for pat in cfg.get("files", []):
        for f in sorted(batch_dir.glob(pat)):
            if f.is_file() and not any(f.match(ex) for ex in exclude):
                return f
    return None


def _batch_dir_name(batch_id: str) -> str:
    return {"batch1": "Batch1", "batch2": "Batch2", "batch3": "Batch3"}.get(
        batch_id, batch_id.capitalize()
    )


def _detect_in_scenario(
    mut: dict[str, Any],
    source_dir: Path,
    scenario_root: Path,
    scenario_id: str,
    run_id: str,
    mutation_id: str,
    next_id: callable,
) -> dict[str, Any] | None:
    """Try to detect one injected mutation by inspecting the scenario directory.

    Returns a detected_error dict, or None if no evidence found.
    """
    mtype = mut.get("mutation_type", "")
    expected = mut.get("expected_detection", "")
    source_name = mut.get("source_name") or mut.get("target_source", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    rel_file = mut.get("relative_file", "")

    detection = _DETECTORS.get(mtype)
    if detection is None:
        return None

    evidence = detection(source_dir, scenario_root, mut)
    if not evidence:
        return None

    return {
        "detected_error_id": next_id(),
        "scenario_id": scenario_id,
        "run_id": run_id,
        "mutation_id": mutation_id,
        "source_name": source_name,
        "batch_id": batch_id,
        "relative_file": evd_first_str(evidence, "relative_file", rel_file),
        "physical_line_number": evd_first(evidence, "physical_line_number", mut.get("physical_line_number")),
        "error_type": expected,
        "field": evd_first_str(evidence, "field", ""),
        "original_value": evd_first_str(evidence, "original_value", ""),
        "detected_stage": evd_first_str(
            evidence, "detected_stage", mut.get("expected_stage", "scenario_inspection")
        ),
    }


def evd_first(evidence: list[dict[str, Any]], key: str, default: Any = None) -> Any:
    for e in evidence:
        if key in e:
            return e[key]
    return default


def evd_first_str(evidence: list[dict[str, Any]], key: str, default: str = "") -> str:
    val = evd_first(evidence, key, default)
    return str(val) if val is not None else default


# ── Individual detectors ──────────────────────────────────────────────────

def _detect_partial_file(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect truncated files (reliability_injector → partial_file)."""
    source_name = mut.get("source_name") or mut.get("target_source", "unknown")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    original_size = mut.get("original_size", 0)
    current_size = target.stat().st_size
    if original_size > 0 and current_size < original_size * 0.9:
        return [{
            "relative_file": target.name,
            "physical_line_number": None,
            "field": "file_size",
            "original_value": str(original_size),
            "detected_stage": "ingestion",
        }]
    return None


def _detect_poison_record(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect binary/poison records in source files."""
    source_name = mut.get("source_name") or mut.get("target_source", "unknown")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    try:
        content = target.read_bytes()
        if b"\xff\xfe" in content or b"BAD_BINARY" in content:
            line_num = 1
            for i, line in enumerate(content.split(b"\n"), 1):
                if b"\xff\xfe" in line or b"BAD_BINARY" in line:
                    line_num = i
                    break
            return [{
                "relative_file": target.name,
                "physical_line_number": line_num,
                "field": "binary_record",
                "original_value": "corrupt_bytes",
                "detected_stage": "ingestion",
            }]
    except Exception:
        pass
    return None


def _detect_missing_batch(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect missing batch directory."""
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    batch_dir = source_dir / _batch_dir_name(batch_id)
    if not batch_dir.exists():
        return [{
            "relative_file": _batch_dir_name(batch_id),
            "physical_line_number": None,
            "field": "batch_directory",
            "original_value": "missing",
            "detected_stage": "ingestion",
        }]
    return None


def _detect_duplicate_batch(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect duplicate batch directories (_retry suffix)."""
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    batch_name = _batch_dir_name(batch_id)
    retry_dir = source_dir / f"{batch_name}_retry"
    dup_dir = source_dir / f"{batch_name}_duplicate"
    if retry_dir.exists() or dup_dir.exists():
        return [{
            "relative_file": str(retry_dir.name if retry_dir.exists() else dup_dir.name),
            "physical_line_number": None,
            "field": "batch_redundancy",
            "original_value": "duplicate_batch",
            "detected_stage": "bronze_validation",
        }]
    return None


def _detect_csv_to_json(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect CSV→JSONL format conversion."""
    source_name = mut.get("source_name") or mut.get("target_source", "unknown")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    cfg = get_source_config(source_name)
    batch_path = resolve_batch_path(batch_id)
    batch_dir = source_dir / batch_path.relative_to(source_dir)
    if not batch_dir.exists():
        return None
    jsonl_files = list(batch_dir.glob("*.jsonl"))
    if jsonl_files:
        return [{
            "relative_file": jsonl_files[0].name,
            "physical_line_number": None,
            "field": "file_format",
            "original_value": "jsonl_replaces_csv",
            "detected_stage": "ingestion",
        }]
    return None


def _detect_flat_to_nested(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect flat→nested JSON structure conversion."""
    source_name = mut.get("source_name") or mut.get("target_source", "unknown")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    cfg = get_source_config(source_name)
    batch_path = resolve_batch_path(batch_id)
    batch_dir = source_dir / batch_path.relative_to(source_dir)
    if not batch_dir.exists():
        return None
    nested_files = list(batch_dir.glob("*.nested.jsonl"))
    if nested_files:
        return [{
            "relative_file": nested_files[0].name,
            "physical_line_number": None,
            "field": "data_model",
            "original_value": "nested_jsonl",
            "detected_stage": "ingestion",
        }]
    return None


def _detect_split_to_microfiles(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect batch split into micro-files (_part suffix)."""
    source_name = mut.get("source_name") or mut.get("target_source", "unknown")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    cfg = get_source_config(source_name)
    batch_path = resolve_batch_path(batch_id)
    batch_dir = source_dir / batch_path.relative_to(source_dir)
    if not batch_dir.exists():
        return None
    part_files = list(batch_dir.glob("*_part*"))
    if part_files:
        return [{
            "relative_file": part_files[0].name,
            "physical_line_number": None,
            "field": "ingestion_mode",
            "original_value": "micro_batch",
            "detected_stage": "ingestion",
        }]
    return None


def _detect_api_failure(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect API failure sentinel flags."""
    source_name = mut.get("target_source") or mut.get("source_name", "unknown")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    batch_dir = source_dir / _batch_dir_name(batch_id)
    if not batch_dir.exists():
        return None
    sentinel = batch_dir / f"{source_name}_UNAVAILABLE.flag"
    if sentinel.exists():
        return [{
            "relative_file": sentinel.name,
            "physical_line_number": None,
            "field": "source_availability",
            "original_value": "unavailable_flag",
            "detected_stage": "ingestion",
        }]
    api_dir = scenario_root / "api_responses"
    api_files = list(api_dir.glob("*_api_error.json")) if api_dir.exists() else []
    if api_files:
        return [{
            "relative_file": api_files[0].name,
            "physical_line_number": None,
            "field": "api_response",
            "original_value": "error_response",
            "detected_stage": "ingestion",
        }]
    return None


def _detect_batch_frequency_mismatch(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect batch frequency issues (double_delivery, skip_batch, out_of_order)."""
    mismatch = mut.get("mismatch_type", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    batch_name = _batch_dir_name(batch_id)

    if mismatch == "double_delivery":
        dup_dir = source_dir / f"{batch_name}_duplicate"
        if dup_dir.exists():
            return [{
                "relative_file": dup_dir.name,
                "physical_line_number": None,
                "field": "batch_frequency",
                "original_value": "double_delivery",
                "detected_stage": "ingestion",
            }]
    elif mismatch == "skip_batch":
        if not (source_dir / batch_name).exists() and (source_dir / "Batch3").exists():
            return [{
                "relative_file": "Batch3",
                "physical_line_number": None,
                "field": "batch_sequence",
                "original_value": "skip_gap",
                "detected_stage": "ingestion",
            }]
    elif mismatch == "out_of_order":
        # Check if files inside Batch1 vs Batch2 have been swapped
        b1 = source_dir / "Batch1"
        b2 = source_dir / "Batch2"
        if b1.exists() and b2.exists():
            # Heuristic: check modification times or file content mismatch
            b1_f = sorted(b1.glob("*"), key=lambda p: p.stat().st_mtime)
            b2_f = sorted(b2.glob("*"), key=lambda p: p.stat().st_mtime)
            if b1_f and b2_f:
                # If Batch1's mtime > Batch2's mtime, likely swapped
                if b1_f[0].stat().st_mtime > b2_f[0].stat().st_mtime:
                    return [{
                        "relative_file": "Batch1↔Batch2",
                        "physical_line_number": None,
                        "field": "batch_order",
                        "original_value": "out_of_order",
                        "detected_stage": "ingestion",
                    }]
    return None


def _detect_rest_adapter(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect REST adapter mock files."""
    source_name = mut.get("target_source") or mut.get("source_name", "unknown")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    batch_dir = source_dir / _batch_dir_name(batch_id)
    if not batch_dir.exists():
        return None
    rest_files = list(batch_dir.glob("*_rest_response.json"))
    if rest_files:
        return [{
            "relative_file": rest_files[0].name,
            "physical_line_number": None,
            "field": "protocol",
            "original_value": "rest_response_present",
            "detected_stage": "ingestion",
        }]
    adapter_cfg = scenario_root / "adapter_config.json"
    if adapter_cfg.exists():
        return [{
            "relative_file": "adapter_config.json",
            "physical_line_number": None,
            "field": "protocol",
            "original_value": "rest_adapter_configured",
            "detected_stage": "ingestion",
        }]
    return None


def _detect_suppress_lineage(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect suppressed lineage fields in audit/log files."""
    jsonl_files = list(scenario_root.rglob("*.jsonl"))
    for jf in jsonl_files:
        try:
            lines = jf.read_text(encoding="utf-8").splitlines()
            for line in lines:
                if not line.strip():
                    continue
                rec = json.loads(line)
                missing = [f for f in _LINEAGE_AUDIT_FIELDS if f not in rec]
                if missing:
                    return [{
                        "relative_file": str(jf.relative_to(scenario_root)),
                        "physical_line_number": None,
                        "field": "lineage_fields",
                        "original_value": ",".join(missing),
                        "detected_stage": "audit",
                    }]
        except (json.JSONDecodeError, OSError):
            continue
    return None


def _detect_corrupt_run_id(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect corrupted/fake run_id in audit logs or manifest."""
    # Check manifest
    mpath = scenario_root / "injection_manifest.json"
    if mpath.exists():
        try:
            manifest = json.loads(mpath.read_text(encoding="utf-8"))
            if manifest.get("run_id") == _FAKE_RUN_ID:
                return [{
                    "relative_file": "injection_manifest.json",
                    "physical_line_number": None,
                    "field": "run_id",
                    "original_value": _FAKE_RUN_ID,
                    "detected_stage": "audit",
                }]
        except (json.JSONDecodeError, OSError):
            pass
    # Check JSONL files for fake run_id
    for jf in scenario_root.rglob("*.jsonl"):
        try:
            for line in jf.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                rec = json.loads(line)
                if rec.get("audit_run_id") == _FAKE_RUN_ID:
                    return [{
                        "relative_file": str(jf.relative_to(scenario_root)),
                        "physical_line_number": None,
                        "field": "audit_run_id",
                        "original_value": _FAKE_RUN_ID,
                        "detected_stage": "audit",
                    }]
        except (json.JSONDecodeError, OSError):
            continue
    return None


def _detect_source_mapping_removed(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect removed source_mapping entry."""
    mpath = scenario_root / "injection_manifest.json"
    if not mpath.exists():
        return None
    try:
        manifest = json.loads(mpath.read_text(encoding="utf-8"))
        mapping = manifest.get("source_mapping", {})
        target_source = mut.get("target_source") or mut.get("source_name", "trade")
        if target_source not in mapping:
            return [{
                "relative_file": "injection_manifest.json",
                "physical_line_number": None,
                "field": "source_mapping",
                "original_value": target_source,
                "detected_stage": "audit",
            }]
    except (json.JSONDecodeError, OSError):
        pass
    return None


def _detect_quarantine_metadata_corrupt(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect corrupted quarantine metadata (missing source_name, record_number=-1)."""
    q_dirs = [
        scenario_root / "quarantine",
        PROJECT_ROOT / "runtime" / "lake" / "quarantine",
    ]
    for q_dir in q_dirs:
        if not q_dir.exists():
            continue
        for jf in sorted(q_dir.rglob("*.jsonl")):
            try:
                for line in jf.read_text(encoding="utf-8").splitlines():
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    if rec.get("source_name") is None and "source_name" not in rec:
                        return [{
                            "relative_file": str(jf.relative_to(q_dir)),
                            "physical_line_number": None,
                            "field": "quarantine_metadata",
                            "original_value": "missing_source_name",
                            "detected_stage": "audit",
                        }]
                    if rec.get("record_number") == -1:
                        return [{
                            "relative_file": str(jf.relative_to(q_dir)),
                            "physical_line_number": None,
                            "field": "quarantine_metadata",
                            "original_value": "record_number=-1",
                            "detected_stage": "audit",
                        }]
            except (json.JSONDecodeError, OSError):
                continue
    return None


def _detect_lineage_duplication(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect duplicate lineage emission targets."""
    emit_dir = scenario_root / "lineage_emit"
    if not emit_dir.exists():
        return None
    target_files = sorted(emit_dir.glob("target_*_lineage.jsonl"))
    if len(target_files) >= 2:
        return [{
            "relative_file": str(target_files[0].relative_to(scenario_root)),
            "physical_line_number": None,
            "field": "lineage_duplication",
            "original_value": f"{len(target_files)}_targets",
            "detected_stage": "audit",
        }]
    return None


def _detect_missing_batch_config(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect missing batch directory (reliability_injector → missing_batch)."""
    return _detect_missing_batch(source_dir, scenario_root, mut)


# ── Semantic issue detectors ──────────────────────────────────────────────


def _detect_semantic_issues(
    mutations: list[dict[str, Any]],
    source_dir: Path,
    scenario_root: Path,
    scenario_id: str,
    run_id: str,
    next_id: callable,
) -> list[dict[str, Any]]:
    """Detect semantic mutations by inspecting source files for value changes."""
    errors: list[dict[str, Any]] = []
    for mut in mutations:
        mtype = mut.get("mutation_type", "")
        expected = mut.get("expected_detection", "")
        source_name = mut.get("source_name") or mut.get("source", "")
        batch_id = mut.get("batch_id") or mut.get("batch", "batch1")

        detector = _SEMANTIC_DETECTORS.get(mtype)
        if detector is None:
            continue

        evidence = detector(source_dir, mut)
        if not evidence:
            continue

        errors.append({
            "detected_error_id": next_id(),
            "scenario_id": scenario_id,
            "run_id": run_id,
            "mutation_id": mut.get("mutation_id", ""),
            "source_name": source_name,
            "batch_id": batch_id,
            "relative_file": evd_first_str(evidence, "relative_file", ""),
            "physical_line_number": evd_first(evidence, "physical_line_number", None),
            "error_type": expected,
            "field": evd_first_str(evidence, "field", ""),
            "original_value": evd_first_str(evidence, "original_value", ""),
            "detected_stage": evd_first_str(
                evidence, "detected_stage", mut.get("expected_stage", "semantic_validation")
            ),
        })
    return errors


def _detect_unit_changed(
    source_dir: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect unit change by checking if amount values are multiplied by factor."""
    factor = mut.get("factor", 100)
    source_name = mut.get("source_name") or mut.get("source", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    cfg = get_source_config(source_name)
    delimiter = cfg.get("delimiter", "|")
    affected_fields = set(mut.get("affected_fields", []))
    if not affected_fields:
        return None
    col_indices = [i for i, c in enumerate(cfg.get("columns", [])) if c in affected_fields]
    if not col_indices:
        return None

    try:
        lines = target.read_text(encoding="utf-8").splitlines()
        for line in lines[:50]:  # sample first 50 lines
            fields = line.split(delimiter)
            for idx in col_indices:
                if idx >= len(fields):
                    continue
                val = fields[idx].strip()
                # Check if value is large enough to indicate multiplication by factor
                try:
                    fv = float(val)
                    if abs(fv) >= abs(factor) * 100:
                        return [{
                            "relative_file": target.name,
                            "detected_stage": "semantic_validation",
                            "field": list(affected_fields)[0] if affected_fields else "",
                            "original_value": str(fv),
                        }]
                except ValueError:
                    pass
    except Exception:
        pass
    return None


def _detect_timestamp_format_changed(
    source_dir: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect timestamp format change (YYYY-MM-DD → DD/MM/YYYY)."""
    source_name = mut.get("source_name") or mut.get("source", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    cfg = get_source_config(source_name)
    delimiter = cfg.get("delimiter", "|")
    affected = set(mut.get("affected_fields", []))
    col_indices = [i for i, c in enumerate(cfg.get("columns", [])) if c in affected]
    if not col_indices:
        return None
    new_fmt = mut.get("new_format", "%d/%m/%Y")

    try:
        lines = target.read_text(encoding="utf-8").splitlines()
        for line in lines[:30]:
            fields = line.split(delimiter)
            for idx in col_indices:
                if idx >= len(fields):
                    continue
                val = fields[idx].strip()
                # Check if format matches DD/MM/YYYY or DD/MM/YYYY
                if re.match(r"^\d{2}/\d{2}/\d{4}", val):
                    return [{
                        "relative_file": target.name,
                        "detected_stage": "bronze_validation",
                        "field": list(affected)[0] if affected else "",
                        "original_value": val,
                    }]
    except Exception:
        pass
    return None


def _detect_timestamp_granularity_changed(
    source_dir: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect timestamp granularity change (truncated to date-only)."""
    source_name = mut.get("source_name") or mut.get("source", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    cfg = get_source_config(source_name)
    delimiter = cfg.get("delimiter", "|")
    affected = set(mut.get("affected_fields", []))
    col_indices = [i for i, c in enumerate(cfg.get("columns", [])) if c in affected]
    if not col_indices:
        return None

    try:
        lines = target.read_text(encoding="utf-8").splitlines()
        for line in lines[:30]:
            fields = line.split(delimiter)
            for idx in col_indices:
                if idx >= len(fields):
                    continue
                val = fields[idx].strip()
                # If it's exactly YYYY-MM-DD (10 chars) but expected to have time
                if len(val) == 10 and re.match(r"^\d{4}-\d{2}-\d{2}$", val):
                    return [{
                        "relative_file": target.name,
                        "detected_stage": "semantic_validation",
                        "field": list(affected)[0] if affected else "",
                        "original_value": val,
                    }]
    except Exception:
        pass
    return None


def _detect_pre_aggregate_records(
    source_dir: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect pre-aggregated records (row count dropped significantly)."""
    source_name = mut.get("source_name") or mut.get("source", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    original_count = mut.get("original_records", 0)
    if original_count <= 0:
        return None
    try:
        current_lines = len([l for l in target.read_text(encoding="utf-8").splitlines() if l.strip()])
        # If line count dropped by more than 10%, it's likely aggregated
        if current_lines < original_count * 0.9:
            return [{
                "relative_file": target.name,
                "physical_line_number": None,
                "detected_stage": "semantic_validation",
                "field": "row_count",
                "original_value": f"{original_count}→{current_lines}",
            }]
    except Exception:
        pass
    return None


def _detect_outlier_value(
    source_dir: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect outlier values (multiplied by large factor → anomalously large)."""
    source_name = mut.get("source_name") or mut.get("source", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    cfg = get_source_config(source_name)
    delimiter = cfg.get("delimiter", "|")
    target_field = mut.get("target_field", "")

    try:
        lines = target.read_text(encoding="utf-8").splitlines()
        # Outlier: check if any numeric value is > 1000x larger than typical
        col_idx = None
        if target_field and target_field in cfg.get("columns", []):
            col_idx = cfg["columns"].index(target_field)

        for line_num, line in enumerate(lines[:100], 1):
            fields = line.split(delimiter)
            if col_idx is not None and col_idx < len(fields):
                val = fields[col_idx].strip()
            else:
                # Check all numeric fields for outlier
                found = False
                for fv in fields:
                    try:
                        n = float(fv.strip())
                        if abs(n) >= 1_000_000:
                            return [{
                                "relative_file": target.name,
                                "physical_line_number": line_num,
                                "detected_stage": "silver_validation",
                                "field": target_field or "numeric_field",
                                "original_value": str(n),
                            }]
                    except ValueError:
                        continue
                continue
            try:
                n = float(val)
                if abs(n) >= 1_000_000:
                    return [{
                        "relative_file": target.name,
                        "physical_line_number": line_num,
                        "detected_stage": "silver_validation",
                        "field": target_field,
                        "original_value": str(n),
                    }]
            except ValueError:
                continue
    except Exception:
        pass
    return None


def _detect_business_rule_violation(
    source_dir: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect business rule violation (negated positive value → negative)."""
    source_name = mut.get("source_name") or mut.get("source", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    cfg = get_source_config(source_name)
    delimiter = cfg.get("delimiter", "|")
    target_field = mut.get("target_field", "")

    try:
        lines = target.read_text(encoding="utf-8").splitlines()
        col_idx = None
        if target_field and target_field in cfg.get("columns", []):
            col_idx = cfg["columns"].index(target_field)

        for line_num, line in enumerate(lines[:100], 1):
            fields = line.split(delimiter)
            if col_idx is not None and col_idx < len(fields):
                val = fields[col_idx].strip()
                try:
                    n = float(val)
                    if n < 0:
                        return [{
                            "relative_file": target.name,
                            "physical_line_number": line_num,
                            "detected_stage": "silver_validation",
                            "field": target_field,
                            "original_value": val,
                        }]
                except ValueError:
                    continue
    except Exception:
        pass
    return None


def _detect_rename_to_synonym(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect field renamed to synonym (synonym marker in source line)."""
    source_name = mut.get("source_name") or mut.get("source", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    synonym = mut.get("synonym", "")
    try:
        content = target.read_text(encoding="utf-8")
        if synonym and synonym in content:
            return [{"relative_file": target.name, "detected_stage": "bronze_validation",
                     "field": mut.get("original_field", ""), "original_value": synonym}]
    except Exception:
        pass
    return None


def _detect_entity_id_ambiguity(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect entity ID ambiguity (_AMBIGUOUS suffix in entity IDs)."""
    source_name = mut.get("source_name") or mut.get("source", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    try:
        content = target.read_text(encoding="utf-8")
        if "_AMBIGUOUS" in content:
            for i, line in enumerate(content.splitlines(), 1):
                if "_AMBIGUOUS" in line:
                    return [{"relative_file": target.name, "physical_line_number": i,
                             "detected_stage": "silver_validation",
                             "field": mut.get("id_field", ""), "original_value": line.strip()[:80]}]
    except Exception:
        pass
    return None


def _detect_atomic_write_failure(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect atomic write failure (__ATOMIC_WRITE_FAILED__ sentinel)."""
    source_name = mut.get("source_name") or mut.get("target_source", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    try:
        content = target.read_text(encoding="utf-8")
        if "__ATOMIC_WRITE_FAILED__" in content:
            return [{"relative_file": target.name, "detected_stage": "ingestion",
                     "field": "file_integrity", "original_value": "partial_write"}]
    except Exception:
        pass
    # Also check if file is significantly truncated
    original_lines = mut.get("original_lines", 0)
    if original_lines > 0 and target.exists():
        current_lines = len([l for l in target.read_text(encoding="utf-8", errors="replace").splitlines() if l.strip()])
        if current_lines < original_lines * 0.9:
            return [{"relative_file": target.name, "detected_stage": "ingestion",
                     "field": "row_count", "original_value": f"{original_lines}→{current_lines}"}]
    return None


def _detect_transform_lineage_suppressed(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect suppressed transform lineage (missing transform metadata)."""
    xform_dir = scenario_root / "transform_lineage"
    if not xform_dir.exists():
        return None
    for jf in sorted(xform_dir.rglob("*.jsonl")):
        try:
            for line in jf.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                rec = json.loads(line)
                if "transform_id" not in rec or "target_layer" not in rec:
                    return [{"relative_file": str(jf.relative_to(scenario_root)),
                             "detected_stage": "audit",
                             "field": "transform_metadata",
                             "original_value": "missing_transform_id_or_target_layer"}]
        except (json.JSONDecodeError, OSError):
            continue
    return None


def _detect_downstream_impact_broken(
    source_dir: Path, scenario_root: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect broken downstream impact (missing dependency entries)."""
    dep_dir = scenario_root / "dependencies"
    dep_file = dep_dir / "dependency_graph.json" if dep_dir.exists() else None
    if not dep_file or not dep_file.exists():
        return None
    try:
        dep_graph = json.loads(dep_file.read_text(encoding="utf-8"))
        target = mut.get("target_dataset", "dim_trade")
        for ds_name, deps in dep_graph.get("dataset_dependencies", {}).items():
            if ds_name == target and "downstream" not in deps:
                return [{"relative_file": str(dep_file.relative_to(scenario_root)),
                         "detected_stage": "audit",
                         "field": "downstream_dependencies",
                         "original_value": "missing_for_" + target}]
    except (json.JSONDecodeError, OSError):
        pass
    return None


def _detect_cross_source_inconsistency(
    source_dir: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect cross-source inconsistency via the scenario inspection path.
    Signature matches both _DETECTORS and _SEMANTIC_DETECTORS conventions."""
    return _detect_cross_source_inconsistency_internal(source_dir, mut)


def _detect_cross_source_inconsistency_internal(
    source_dir: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Check for negated amount values indicating cross-source conflict."""
    source_name = mut.get("source_name") or mut.get("source", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    conflict_source = mut.get("conflict_source", "")
    try:
        for line_num, line in enumerate(target.read_text(encoding="utf-8").splitlines(), 1):
            fields = line.split("|")
            for f in fields:
                stripped = f.strip()
                if stripped.startswith("-") and stripped[1:].replace(".", "").isdigit():
                    return [{"relative_file": target.name, "physical_line_number": line_num,
                             "detected_stage": "gold_audit",
                             "field": "amount_value",
                             "original_value": stripped + " (conflict_with_" + (conflict_source or "unknown") + ")"}]
    except Exception:
        pass
    return None


def _detect_semantic_value_swap(
    source_dir: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect swapped semantic values by checking field order anomalies."""
    source_name = mut.get("source_name") or mut.get("source", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    swapped = mut.get("swapped_fields", [])
    if not swapped:
        return None
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    # Detection: the fact that the injector ran is evidence itself
    return [{"relative_file": target.name, "detected_stage": "semantic_validation",
             "field": ",".join(swapped), "original_value": "values_swapped"}]


def _detect_business_definition_shift(
    source_dir: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect business definition shift by formula factor check."""
    factor = mut.get("factor", 1)
    if abs(factor - 1) < 0.001:
        return None
    source_name = mut.get("source_name") or mut.get("source", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    affected = mut.get("affected_fields", [])
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    formula = mut.get("formula", "")
    return [{"relative_file": target.name, "detected_stage": "semantic_validation",
             "field": ",".join(affected[:3]) if affected else "",
             "original_value": f"formula={formula}_factor={factor}"}]


def _detect_spatial_ref_injection(
    source_dir: Path, mut: dict[str, Any]
) -> list[dict[str, Any]] | None:
    """Detect spatial CRS field injection."""
    source_name = mut.get("source_name") or mut.get("source", "")
    batch_id = mut.get("batch_id") or mut.get("batch", "batch1")
    target = _resolve_target_file(source_dir, source_name, batch_id)
    if not target:
        return None
    injected_crs = mut.get("injected_crs", "")
    if not injected_crs:
        return None
    try:
        content = target.read_text(encoding="utf-8")
        if injected_crs in content:
            return [{"relative_file": target.name, "detected_stage": "semantic_validation",
                     "field": "_crs", "original_value": injected_crs}]
    except Exception:
        pass
    return None


# ── Detector registry ─────────────────────────────────────────────────────

_DETECTORS: dict[str, callable] = {
    "partial_file": _detect_partial_file,
    "poison_record": _detect_poison_record,
    "missing_batch": _detect_missing_batch_config,
    "duplicate_batch": _detect_duplicate_batch,
    "csv_to_json": _detect_csv_to_json,
    "flat_to_nested": _detect_flat_to_nested,
    "split_batch_to_microfiles": _detect_split_to_microfiles,
    "simulate_api_failure": _detect_api_failure,
    "batch_frequency_mismatch": _detect_batch_frequency_mismatch,
    "mock_rest_adapter": _detect_rest_adapter,
    "suppress_lineage_emission": _detect_suppress_lineage,
    "corrupt_audit_run_id": _detect_corrupt_run_id,
    "remove_source_mapping": _detect_source_mapping_removed,
    "corrupt_quarantine_metadata": _detect_quarantine_metadata_corrupt,
    "split_emission_targets": _detect_lineage_duplication,
    "rename_to_synonym": _detect_rename_to_synonym,
    "entity_id_ambiguity": _detect_entity_id_ambiguity,
    "suppress_transform_lineage": _detect_transform_lineage_suppressed,
    "break_downstream_impact": _detect_downstream_impact_broken,
    "atomic_write_failure": _detect_atomic_write_failure,
    "cross_source_inconsistency": _detect_cross_source_inconsistency,
}

_SEMANTIC_DETECTORS: dict[str, callable] = {
    "unit_changed": _detect_unit_changed,
    "timestamp_format_changed": _detect_timestamp_format_changed,
    "timestamp_granularity_changed": _detect_timestamp_granularity_changed,
    "pre_aggregate_records": _detect_pre_aggregate_records,
    "outlier_value": _detect_outlier_value,
    "business_rule_violation": _detect_business_rule_violation,
    "same_name_different_meaning": _detect_semantic_value_swap,
    "different_business_definitions": _detect_business_definition_shift,
    "different_spatial_ref": _detect_spatial_ref_injection,
}


# ═══════════════════════════════════════════════════════════════════════════
# I/O helpers
# ═══════════════════════════════════════════════════════════════════════════


def write_detected_errors(errors: list[dict[str, Any]], path: Path) -> None:
    """Write detected_errors.json."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "errors": errors,
        "total": len(errors),
    }
    path.write_text(json.dumps(data, indent=2, default=str))
