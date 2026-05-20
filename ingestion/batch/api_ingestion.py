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
    """Handle common public API shapes while staying explicit for new sources.

    Supports response shapes found via schema_discovery collectors:
    - Flat list of records
    - Dict with results/records/data/items/rows/features/readings/feed keys
    - Nested dict like {"data": {"records": [...]}}
    """
    if isinstance(payload, list):
        return [record for record in payload if isinstance(record, dict)]
    if isinstance(payload, dict):
        for key in ("results", "records", "data", "items", "rows",
                    "features", "readings", "feed", "events", "incidents",
                    "measurements", "stations", "locations", "sensors"):
            value = payload.get(key)
            if isinstance(value, list):
                return [record for record in value if isinstance(record, dict)]
            if isinstance(value, dict):
                for nested_key in ("records", "readings", "items", "results"):
                    nested = value.get(nested_key)
                    if isinstance(nested, list):
                        return [r for r in nested if isinstance(r, dict)]
        return [payload]
    raise ValueError("Unsupported API response shape")


def ingest_api_records(
    url: str,
    api_key: str | None = None,
    max_pages: int = 5,
    page_size: int = 50,
    auth_style: str = "bearer",
) -> list[dict[str, Any]]:
    """Fetch records from a REST API, following pagination if present.

    Supports JSON:API-style pagination (used by DfT Road Traffic API)
    where `links.next` indicates the next page URL.
    Falls back to a single non-paginated request if pagination params fail.
    """
    headers = {"Accept": "application/json"}
    params = {}
    if api_key:
        if auth_style == "x-api-key":
            headers["X-API-Key"] = api_key
        elif auth_style == "token-header":
            headers["token"] = api_key
        elif auth_style == "query-token":
            params["token"] = api_key
        elif auth_style == "query-appid":
            params["appid"] = api_key
        elif auth_style == "query-app_key":
            params["app_key"] = api_key
        else:
            headers["Authorization"] = f"Bearer {api_key}"

    # First try a simple single request (works for most APIs)
    if page_size:
        params["page[size]"] = str(page_size)
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    all_records = extract_records(payload)

    # If the response has pagination links, follow them
    if isinstance(payload, dict):
        next_url = (
            payload.get("next_page_url")
            or (payload.get("links", {}) if isinstance(payload.get("links"), dict) else {}).get("next")
        )
        page = 1
        while next_url and page < max_pages:
            page += 1
            response = requests.get(next_url, headers=headers, timeout=30)
            response.raise_for_status()
            payload = response.json()
            records = extract_records(payload)
            if not records:
                break
            all_records.extend(records)
            if isinstance(payload, dict):
                next_url = (
                    payload.get("next_page_url")
                    or (payload.get("links", {}) if isinstance(payload.get("links"), dict) else {}).get("next")
                )
            else:
                next_url = None

    return all_records


def ingest_api(dataset: str, url: str, api_key: str | None = None, auth_style: str = "bearer") -> str:
    """Ingest records from an HTTP API into the raw local landing zone."""
    records = ingest_api_records(url, api_key, auth_style=auth_style)
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
