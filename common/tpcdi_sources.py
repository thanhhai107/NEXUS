"""
TPC-DI DIGen source configuration — resolve file paths for DIGen-generated data.

Reads ``domains/tpc/tpcdi_sources.yml`` and provides functions to resolve
batch paths, list source files, and load DIGen reports/audit files.

This module does NOT parse data content — only metadata and path resolution.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "domains" / "tpc" / "tpcdi_sources.yml"


def load_tpcdi_sources_config(path: str | Path | None = None) -> dict[str, Any]:
    path = Path(path) if path else DEFAULT_CONFIG_PATH
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _get_tpcdi_config(path: str | Path | None = None) -> dict[str, Any]:
    cfg = load_tpcdi_sources_config(path)
    return cfg.get("tpcdi", cfg)


def source_root(path: str | Path | None = None) -> Path:
    cfg = _get_tpcdi_config(path)
    root = cfg.get("source_root", "runtime/tpcdi/sf3")
    return PROJECT_ROOT / root


def resolve_batch_path(batch_id: str) -> Path:
    cfg = _get_tpcdi_config()
    batch_cfg = cfg.get("batches", {}).get(batch_id)
    if not batch_cfg:
        raise KeyError(f"Batch '{batch_id}' not found in tpcdi_sources.yml")
    return source_root() / batch_cfg["path"]


def get_source_config(source_name: str) -> dict[str, Any]:
    cfg = _get_tpcdi_config()
    src = cfg.get("sources", {}).get(source_name)
    if not src:
        raise KeyError(f"Source '{source_name}' not found in tpcdi_sources.yml")
    return src


def list_source_files(source_name: str, batch_id: str) -> list[Path]:
    src = get_source_config(source_name)
    if batch_id not in src.get("batches", []):
        return []

    batch_dir = resolve_batch_path(batch_id)
    patterns = src.get("files", [])
    exclude = src.get("exclude_patterns", [])

    found: list[Path] = []
    for pattern in patterns:
        matched = list(batch_dir.glob(pattern))
        for m in matched:
            if m.is_file() and not any(m.match(ex) for ex in exclude):
                found.append(m)
    return sorted(found)


def list_sources_for_batch(batch_id: str, include_control: bool = False) -> list[dict[str, Any]]:
    cfg = _get_tpcdi_config()
    result = []
    for name, src_cfg in cfg.get("sources", {}).items():
        if batch_id not in src_cfg.get("batches", []):
            continue
        if not include_control and src_cfg.get("ingest") is False:
            continue
        if src_cfg.get("source_kind") == "control":
            if not include_control:
                continue
        result.append({"name": name, **src_cfg})
    return result


def load_digen_report(path: str | Path | None = None) -> dict[str, Any]:
    report_path = Path(path) if path else source_root() / "digen_report.txt"

    if not report_path.exists():
        return {"error": f"digen_report.txt not found at {report_path}"}

    result: dict[str, Any] = {}

    pattern_batch = re.compile(
        r"AuditTotalRecordsSummaryWriter\s*-\s*TotalRecords for Batch(\d+):\s*(\d+)"
    )
    pattern_all = re.compile(
        r"AuditTotalRecordsSummaryWriter\s*-\s*TotalRecords all Batches:\s*(\d+)"
    )

    with report_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            m = pattern_batch.search(line)
            if m:
                batch_num, count = m.groups()
                result[f"batch{batch_num}"] = int(count)
                continue

            m = pattern_all.search(line)
            if m:
                result["all_batches"] = int(m.group(1))

    return result


def load_batch_audit(batch_id: str) -> list[dict[str, Any]]:
    cfg = _get_tpcdi_config()
    batch_num = cfg.get("batches", {}).get(batch_id, {}).get("id", 1)
    audit_files = cfg.get("reports", {}).get("batch_audits", {}).get("files", [])

    expected = f"Batch{batch_num}_audit.csv"
    matching = [f for f in audit_files if expected in f]
    if not matching:
        return [{"error": f"Batch audit file not found for {batch_id}"}]

    audit_path = source_root() / matching[0]
    if not audit_path.exists():
        return [{"error": f"Audit file not found: {audit_path}"}]

    results: list[dict[str, Any]] = []
    with audit_path.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append(row)
    return results


def load_generator_audit() -> list[dict[str, Any]]:
    cfg = _get_tpcdi_config()
    audit_file = cfg.get("reports", {}).get("generator_audit", {}).get("file", "")
    audit_path = source_root() / audit_file

    if not audit_path.exists():
        return [{"error": f"Generator audit not found: {audit_path}"}]

    results: list[dict[str, Any]] = []
    with audit_path.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append(row)
    return results
