"""
API Polling Stream for NEXUS Streaming.
========================================

Long-running API polling stream that periodically calls REST API endpoints
and writes results directly to the raw landing zone (canonical envelope format).

Usage:
    python -m ingestion.streaming.api_stream --source openaq --dataset aqi --api-url https://api.openaq.org/v3/locations

Environment Variables:
    None required. All configuration passed via CLI arguments.
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.base.http import request_json
from ingestion.canonical.envelope import EnvelopeContext
from ingestion.canonical.writer import write_raw_envelopes
from common.config import BRONZE_DIR


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class ApiStreamConfig:
    source_key: str
    dataset: str
    api_url: str
    api_key: str | None = None
    auth_header: str = "Authorization"
    poll_interval_seconds: float = 60.0
    batch_size: int = 100
    max_pages: int = 5
    max_iterations: int = 0
    rate_limit_delay: float = 1.0


@dataclass
class ApiStreamResult:
    iterations: int = 0
    fetched: int = 0
    landed: int = 0
    errors: list[str] = field(default_factory=list)
    raw_paths: list[str] = field(default_factory=list)
    started_at: str | None = None
    finished_at: str | None = None


# ============================================================================
# API Fetcher
# ============================================================================

def _append_query_param(url: str, key: str, value: str) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query[key] = value
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("results", "records", "data", "items", "events", "incidents"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _next_page_url(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    links = payload.get("links", {}) if isinstance(payload.get("links"), dict) else {}
    value = payload.get("next_page_url") or links.get("next")
    return str(value) if value else None


def _raw_request(url: str, headers: dict[str, str], timeout: int = 30) -> Any:
    import requests
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.json()


def fetch_api_records(
    url: str,
    api_key: str | None,
    auth_header: str,
    max_pages: int = 5,
) -> list[dict[str, Any]]:
    headers: dict[str, str] = {"Accept": "application/json"}
    resolved_url = url

    if api_key:
        if auth_header == "X-API-Key":
            headers[auth_header] = api_key
        elif auth_header == "token-header":
            headers["token"] = api_key
        elif auth_header == "query-token":
            resolved_url = _append_query_param(resolved_url, "token", api_key)
        elif auth_header == "query-app_key":
            resolved_url = _append_query_param(resolved_url, "app_key", api_key)
        elif auth_header == "query-appid":
            resolved_url = _append_query_param(resolved_url, "appid", api_key)
        else:
            headers[auth_header] = f"Bearer {api_key}"

    all_records: list[dict[str, Any]] = []

    try:
        payload = _raw_request(resolved_url, headers)
    except Exception as exc:
        print(f"  API fetch failed: {exc}")
        return []

    all_records.extend(_extract_records(payload))

    next_url = _next_page_url(payload)
    page = 1
    while next_url and page < max_pages:
        page += 1
        try:
            payload = _raw_request(next_url, headers)
        except Exception as exc:
            print(f"  API pagination failed for page {page}: {exc}")
            break

        records = _extract_records(payload)
        if not records:
            break
        all_records.extend(records)
        next_url = _next_page_url(payload)

    return all_records


# ============================================================================
# Raw Layer Writer
# ============================================================================

def _write_api_records_to_raw(
    records: list[dict[str, Any]],
    dataset: str,
    source_key: str,
    run_id: str,
    iteration: int,
) -> Path:
    bronze_base = BRONZE_DIR / dataset / f"run_id={run_id}"
    bronze_base.mkdir(parents=True, exist_ok=True)
    output_path = bronze_base / "raw" / f"iteration_{iteration:04d}.jsonl"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    context = EnvelopeContext(
        dataset_id=dataset,
        source_id=source_key,
        ingestion_type="stream_api",
        source_key=source_key,
        run_id=run_id,
    )

    return write_raw_envelopes(
        records,
        context,
        output_path=output_path,
        normalize_payload=True,
    )


# ============================================================================
# Main Polling Loop
# ============================================================================

def poll_api_stream(config: ApiStreamConfig) -> ApiStreamResult:
    result = ApiStreamResult(
        started_at=datetime.now(timezone.utc).isoformat(),
    )

    run_id = f"stream_api_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"

    iteration = 0
    try:
        while True:
            if config.max_iterations > 0 and iteration >= config.max_iterations:
                break

            iteration += 1
            result.iterations = iteration

            print(f"[{datetime.now(timezone.utc).isoformat()}] Poll iteration {iteration}")

            try:
                records = fetch_api_records(
                    url=config.api_url,
                    api_key=config.api_key,
                    auth_header=config.auth_header,
                    max_pages=config.max_pages,
                )
            except Exception as exc:
                result.errors.append(f"Iteration {iteration}: fetch failed: {exc}")
                print(f"  fetch error: {exc}")
                time.sleep(config.poll_interval_seconds)
                continue

            result.fetched += len(records)
            print(f"  fetched {len(records)} records")

            if records:
                try:
                    path = _write_api_records_to_raw(
                        records,
                        config.dataset,
                        config.source_key,
                        run_id,
                        iteration,
                    )
                    result.raw_paths.append(str(path))
                    result.landed += len(records)
                    print(f"  landed to {path}")
                except Exception as exc:
                    result.errors.append(f"Iteration {iteration}: write failed: {exc}")
                    print(f"  write error: {exc}")

            if config.rate_limit_delay > 0:
                time.sleep(config.rate_limit_delay)

            time.sleep(config.poll_interval_seconds)

    except KeyboardInterrupt:
        print("\nShutting down (KeyboardInterrupt)")

    result.finished_at = datetime.now(timezone.utc).isoformat()
    return result


# ============================================================================
# Convenience Function
# ============================================================================

def run_api_stream(
    source_key: str,
    dataset: str,
    api_url: str,
    api_key: str | None = None,
    poll_interval_seconds: float = 60.0,
    max_iterations: int = 0,
    auth_header: str = "Authorization",
    batch_size: int = 100,
    max_pages: int = 5,
    rate_limit_delay: float = 1.0,
) -> ApiStreamResult:
    config = ApiStreamConfig(
        source_key=source_key,
        dataset=dataset,
        api_url=api_url,
        api_key=api_key,
        auth_header=auth_header,
        poll_interval_seconds=poll_interval_seconds,
        batch_size=batch_size,
        max_pages=max_pages,
        max_iterations=max_iterations,
        rate_limit_delay=rate_limit_delay,
    )
    return poll_api_stream(config)


# ============================================================================
# CLI Entry Point
# ============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Poll a REST API and land records into the NEXUS raw layer."
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Source identifier (e.g. openaq, waqi)",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset name for raw layer (e.g. aqi, transport)",
    )
    parser.add_argument(
        "--api-url",
        required=True,
        help="REST API URL to poll",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="API key or token",
    )
    parser.add_argument(
        "--auth-header",
        default="Authorization",
        help="Auth header name: Authorization (Bearer), X-API-Key, token-header, query-token, query-app_key, query-appid (default: Authorization)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=60.0,
        help="Poll interval in seconds (default: 60.0)",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=0,
        help="Maximum poll iterations, 0=unlimited (default: 0)",
    )
    parser.add_argument(
        "--rate-limit-delay",
        type=float,
        default=1.0,
        help="Rate limit delay between pages in seconds (default: 1.0)",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    config = ApiStreamConfig(
        source_key=args.source,
        dataset=args.dataset,
        api_url=args.api_url,
        api_key=args.api_key,
        auth_header=args.auth_header,
        poll_interval_seconds=args.poll_interval,
        max_iterations=args.max_iterations,
        rate_limit_delay=args.rate_limit_delay,
    )

    print(f"Starting API stream: source={args.source} dataset={args.dataset}")
    print(f"API URL: {args.api_url}")
    print(f"Poll interval: {args.poll_interval}s")
    print(f"Max iterations: {args.max_iterations or 'unlimited'}")
    print()

    result = poll_api_stream(config)

    print()
    print("Result:")
    print(f"  iterations:  {result.iterations}")
    print(f"  fetched:     {result.fetched}")
    print(f"  landed:      {result.landed}")
    print(f"  raw paths:   {len(result.raw_paths)} files written")
    print(f"  started at:  {result.started_at}")
    print(f"  finished at: {result.finished_at}")

    if result.errors:
        print(f"  errors:      {len(result.errors)}")
        for error in result.errors[:5]:
            print(f"    - {error}")

    return 0 if result.landed > 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
