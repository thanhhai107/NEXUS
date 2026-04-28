from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.batch.common import write_jsonl


def extract_records(payload: Any) -> list[dict[str, Any]]:
    """Handle common public API shapes while staying explicit for new sources."""
    if isinstance(payload, list):
        return [record for record in payload if isinstance(record, dict)]
    if isinstance(payload, dict):
        for key in ("results", "records", "data", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [record for record in value if isinstance(record, dict)]
        return [payload]
    raise ValueError("Unsupported API response shape")


def ingest_api(dataset: str, url: str, api_key: str | None = None) -> str:
    """Ingest records from an HTTP API into the raw local landing zone."""
    headers = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    records = extract_records(response.json())
    output_path = write_jsonl(dataset=dataset, records=records, source=url)
    print(f"Ingested {len(records)} records for dataset={dataset} into {output_path}")
    return str(output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest an open-data HTTP API source.")
    parser.add_argument("--dataset", required=True, help="Dataset name from domains/*/datasets.yml")
    parser.add_argument("--url", required=True, help="Public API URL")
    parser.add_argument("--api-key", default=None, help="Optional API token")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    ingest_api(dataset=args.dataset, url=args.url, api_key=args.api_key)
