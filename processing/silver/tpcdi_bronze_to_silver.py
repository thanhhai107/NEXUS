"""
TPC-DI Silver transformer — reads Bronze envelopes, writes typed JSONL.

Input:  runtime/lake/bronze/tpcdi/{source}/batch_id={batch}/data.jsonl
Output: runtime/lake/silver/tpcdi/{source}/data.jsonl
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

BRONZE_ROOT = PROJECT_ROOT / "runtime" / "lake" / "bronze" / "tpcdi"
SILVER_ROOT = PROJECT_ROOT / "runtime" / "lake" / "silver" / "tpcdi"


def run(
    source_name: str,
    batch_id: str = "batch1",
    output_root: Path | None = None,
) -> dict[str, Any]:
    """Read Bronze JSONL → clean → write Silver JSONL.

    Drops records with ``_parse_errors``.  Strips internal metadata fields
    (``_source_name``, ``_batch_id``, etc.) from the payload.
    """
    bronze_dir = (output_root or BRONZE_ROOT) / source_name / f"batch_id={batch_id}"
    bronze_path = bronze_dir / "data.jsonl"

    if not bronze_path.exists():
        return {"source": source_name, "error": f"Bronze data not found: {bronze_path}"}

    out_dir = (output_root or SILVER_ROOT) / source_name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.jsonl"

    cleaned_at = datetime.now(timezone.utc).isoformat()
    record_count = 0
    error_count = 0

    with bronze_path.open("r", encoding="utf-8") as fin, out_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue

            envelope = json.loads(line)
            payload = envelope.get("payload", {})

            # Drop records that failed parser-level validation
            if payload.get("_parse_errors"):
                error_count += 1
                continue

            # Strip internal metadata fields from payload
            clean_payload = {k: v for k, v in payload.items() if not k.startswith("_")}

            row = {
                "_nexus_record_id": envelope.get("_nexus_record_id", ""),
                "_nexus_silver_loaded_at": cleaned_at,
                "_nexus_source": source_name,
                "_nexus_batch_id": batch_id,
                "payload": {k: _serialise(v) for k, v in clean_payload.items()},
            }
            fout.write(json.dumps(row, default=str) + "\n")
            record_count += 1

    return {
        "source": source_name,
        "batch_id": batch_id,
        "record_count": record_count,
        "error_count": error_count,
        "output_path": str(out_path),
    }


def _serialise(v: Any) -> Any:
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return v


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-name", required=True)
    parser.add_argument("--batch", default="batch1")
    args = parser.parse_args()
    result = run(source_name=args.source_name, batch_id=args.batch)
    print(json.dumps(result, default=str))
