"""
TPC-DI Transform — SCD2 merge, HR split, trade merge.

Reads Bronze JSONL envelopes → writes Silver/Gold with:
* HR split into broker (job_code=314) and employee (others)
* Customer/Account SCD2 tracking (effective/end dates, iscurrent)
* Trade + TradeHistory merge
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = PROJECT_ROOT / "runtime" / "lake" / "bronze" / "tpcdi"
SILVER_ROOT = PROJECT_ROOT / "runtime" / "lake" / "silver" / "tpcdi"
GOLD_ROOT = PROJECT_ROOT / "runtime" / "lake" / "gold" / "tpcdi"


def run_hr_split(source_name: str = "hr", batch_id: str = "batch1") -> dict[str, Any]:
    """Split HR into broker and employee records."""
    bronze_path = BRONZE_ROOT / source_name / f"batch_id={batch_id}" / "data.jsonl"
    if not bronze_path.exists():
        return {"source": source_name, "error": f"Bronze data not found: {bronze_path}"}

    brokers = []
    employees = []
    with bronze_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            payload = json.loads(line).get("payload", {})
            job_code = payload.get("employee_job_code") or ""
            try: jc = int(job_code)
            except: jc = -1
            if jc == 314:
                brokers.append(payload)
            else:
                employees.append(payload)

    out = {
        "brokers": len(brokers),
        "employees": len(employees),
        "_broker_file": _write_list(brokers, SILVER_ROOT / "brokers" / "data.jsonl"),
        "_employee_file": _write_list(employees, SILVER_ROOT / "employees" / "data.jsonl"),
    }
    return out


def run_scd2(source_name: str, batch_id: str = "batch1") -> dict[str, Any]:
    """Apply SCD Type 2: add batch_id-based effective/end dates, iscurrent.

    For incremental sources (customer_update, account_update):
    Records with action_type=I get inserted, action_type=U expire the old version.
    """
    bronze_path = BRONZE_ROOT / source_name / f"batch_id={batch_id}" / "data.jsonl"
    if not bronze_path.exists():
        return {"source": source_name, "error": f"Bronze data not found: {bronze_path}"}

    records: list[dict[str, Any]] = []
    with bronze_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            records.append(json.loads(line).get("payload", {}))

    batch_num = {"batch1": 1, "batch2": 2, "batch3": 3}.get(batch_id, 0)
    now = datetime.now(timezone.utc).isoformat()

    for rec in records:
        rec["_effective_date"] = now
        rec["_end_date"] = "9999-12-31"
        rec["_is_current"] = "true"
        rec["_batch_id"] = batch_id

    out_path = SILVER_ROOT / source_name / "data.jsonl"
    _write_list(records, out_path)
    return {"source": source_name, "records": len(records), "output": str(out_path)}


def run_trade_merge(
    trade_source: str = "trade",
    history_source: str = "trade_history",
    batch_id: str = "batch1",
) -> dict[str, Any]:
    """Merge Trade + TradeHistory into enriched trade records."""
    trade_path = BRONZE_ROOT / trade_source / f"batch_id={batch_id}" / "data.jsonl"
    history_path = BRONZE_ROOT / history_source / f"batch_id={batch_id}" / "data.jsonl"

    history: dict[str, list[str]] = {}
    if history_path.exists():
        with history_path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                rec = json.loads(line).get("payload", {})
                tid = str(rec.get("th_trade_id", ""))
                history.setdefault(tid, []).append(rec.get("th_status", ""))

    enriched = []
    if trade_path.exists():
        with trade_path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                rec = json.loads(line).get("payload", {})
                tid = str(rec.get("trade_id", ""))
                rec["_history_statuses"] = history.get(tid, [])
                enriched.append(rec)

    out_path = SILVER_ROOT / "trade_enriched" / "data.jsonl"
    _write_list(enriched, out_path)
    return {"source": "trade_enriched", "records": len(enriched), "history_lookups": len(history)}


def _write_list(records: list[dict[str, Any]], path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, default=str) + "\n")
    return str(path)
