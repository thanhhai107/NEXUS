"""
TPC-DI Gold loader — pass-through for reference tables, Date, Time.

Input:  runtime/lake/silver/tpcdi/{source}/data.jsonl
Output: runtime/lake/gold/tpcdi/{source}/data.jsonl
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

SILVER_ROOT = PROJECT_ROOT / "runtime" / "lake" / "silver" / "tpcdi"
GOLD_ROOT = PROJECT_ROOT / "runtime" / "lake" / "gold" / "tpcdi"


def run(
    source_name: str,
    output_root: Path | None = None,
) -> dict[str, Any]:
    """Copy Silver → Gold (pass-through for reference tables)."""
    silver_dir = (output_root or SILVER_ROOT) / source_name
    gold_dir = (output_root or GOLD_ROOT) / source_name
    silver_path = silver_dir / "data.jsonl"

    if not silver_path.exists():
        return {"source": source_name, "error": f"Silver data not found: {silver_path}"}

    gold_dir.mkdir(parents=True, exist_ok=True)
    gold_path = gold_dir / "data.jsonl"
    loaded_at = datetime.now(timezone.utc).isoformat()

    record_count = 0
    with silver_path.open("r", encoding="utf-8") as sin, gold_path.open("w", encoding="utf-8") as gout:
        for line in sin:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            record["_nexus_gold_loaded_at"] = loaded_at
            gout.write(json.dumps(record, default=str) + "\n")
            record_count += 1

    return {
        "source": source_name,
        "record_count": record_count,
        "output_path": str(gold_path),
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-name", required=True)
    args = parser.parse_args()
    result = run(source_name=args.source_name)
    print(json.dumps(result, default=str))
