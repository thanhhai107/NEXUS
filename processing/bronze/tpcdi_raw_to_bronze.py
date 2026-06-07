"""
TPC-DI Bronze loader — reads DIGen source parsers, writes raw JSONL envelopes.

Output: runtime/lake/bronze/tpcdi/{source_name}/batch_id={batch_id}/
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


def run(
    source_name: str,
    batch_id: str = "batch1",
    output_root: Path | None = None,
) -> dict[str, Any]:
    """Read DIGen parser output → write Bronze raw JSONL envelopes."""
    from common.tpcdi_sources import get_source_config
    from ingestion.tpcdi.parsers.reference import parse_reference, _ALLOWED_REFERENCE_SOURCES
    from ingestion.tpcdi.parsers.datetime_dim import parse_date_dim, parse_time_dim
    from ingestion.tpcdi.parsers.csv_pipe import (
        parse_hr, parse_daily_market, parse_prospect,
        parse_trade, parse_cash_transaction, parse_holding_history,
        parse_watch_history,
    )
    from ingestion.tpcdi.parsers.complex import (
        parse_customer_mgmt, parse_finwire,
        parse_customer_update, parse_account_update,
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

    GROUP3_PARSERS = {
        "customer_mgmt": parse_customer_mgmt,
        "finwire": parse_finwire,
        "customer_update": parse_customer_update,
        "account_update": parse_account_update,
    }

    if source_name in _ALLOWED_REFERENCE_SOURCES:
        iter_fn = parse_reference(source_name, batch_id)
    elif source_name == "date":
        iter_fn = parse_date_dim(source_name, batch_id)
    elif source_name == "time":
        iter_fn = parse_time_dim(source_name, batch_id)
    elif source_name in GROUP2_PARSERS:
        iter_fn = GROUP2_PARSERS[source_name](source_name, batch_id)
    elif source_name in GROUP3_PARSERS:
        iter_fn = GROUP3_PARSERS[source_name](source_name, batch_id)
    else:
        raise ValueError(f"Unsupported source: {source_name}")

    out_dir = (output_root or BRONZE_ROOT) / source_name / f"batch_id={batch_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.jsonl"

    ingested_at = datetime.now(timezone.utc).isoformat()
    record_count = 0

    with out_path.open("w", encoding="utf-8") as out:
        for record in iter_fn:
            envelope = {
                "_nexus_record_id": f"{source_name}-{record.get('_record_number', 0)}",
                "_nexus_event_time": ingested_at,
                "_nexus_ingested_at": ingested_at,
                "_nexus_source": source_name,
                "_nexus_dataset": source_name,
                "_nexus_batch_id": batch_id,
                "payload": record,
            }
            out.write(json.dumps(envelope, default=str) + "\n")
            record_count += 1

    return {
        "source": source_name,
        "batch_id": batch_id,
        "record_count": record_count,
        "output_path": str(out_path),
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-name", required=True)
    parser.add_argument("--batch", default="batch1")
    args = parser.parse_args()
    result = run(source_name=args.source_name, batch_id=args.batch)
    print(json.dumps(result, default=str))
