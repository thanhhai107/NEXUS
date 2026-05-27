"""
DfT Road Traffic API Source Adapter.

Downloads UK Department for Transport road traffic data.
"""

from __future__ import annotations

import json
import os
from typing import Any

from ingestion.base.core import DownloadContext, SourceRun
from ingestion.base.http import request_json
from ingestion.base.utils import (
    extract_records,
    source_options,
    years_between,
)


def download_dft(run: SourceRun, context: DownloadContext) -> None:
    """Download DfT road traffic API data."""
    opts = source_options(context, "dft_road_traffic")
    base = os.getenv("DFT_ROAD_TRAFFIC_API_BASE_URL") or opts.get("base_url")
    page_size = int(opts.get("page_size", 500))
    region_id = int(opts.get("region_id", 6))
    page_limit = context.mode.get("dft_page_limit")
    page_limit_int = int(page_limit) if page_limit is not None else None

    _download_paginated(
        run,
        base,
        "count-points",
        {"region_id": region_id},
        "entity=count_points",
        page_size,
        page_limit_int,
    )

    for year in years_between(
        context.mode["transport_start_year"],
        context.mode["transport_end_year"]
    ):
        _download_paginated(
            run,
            base,
            "average-annual-daily-flow",
            {"region_id": region_id, "year": year},
            f"entity=average_annual_daily_flow/year={year}",
            page_size,
            page_limit_int,
        )


def _download_paginated(
    run: SourceRun,
    base: str,
    endpoint: str,
    filters: dict[str, Any],
    rel_prefix: str,
    page_size: int,
    page_limit: int | None,
) -> None:
    """Download paginated DfT API endpoint."""
    chunk_id = f"dft:{endpoint}:{json.dumps(filters, sort_keys=True)}"
    if run.should_skip(chunk_id):
        return

    next_url: str | None = f"{base.rstrip('/')}/{endpoint}"
    next_params: dict[str, Any] | None = {
        "page[size]": page_size,
        **{f"filter[{key}]": value for key, value in filters.items()},
    }
    page = 1
    total = 0

    while next_url:
        if page_limit is not None and page > page_limit:
            break
        payload = request_json(run, next_url, params=next_params)
        records = extract_records(payload)
        if endpoint == "count-points":
            records = [r for r in records if _is_london_record(r)]
        if records:
            run.write_jsonl(f"{rel_prefix}/page={page:04d}.jsonl", records)
        total += len(records)
        next_url = _next_page_url(payload)
        next_params = None
        if not next_url:
            break
        page += 1

    run.mark_complete(chunk_id, {"record_count": total, "pages": page})


def _next_page_url(payload: Any) -> str | None:
    """Extract next page URL from DfT API response."""
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("next_page_url"), str):
        return payload["next_page_url"]
    links = payload.get("links")
    if isinstance(links, dict) and isinstance(links.get("next"), str):
        return links["next"]
    return None


def _is_london_record(record: dict[str, Any]) -> bool:
    """Check if DfT record is for London region."""
    values = record.get("attributes") if isinstance(record.get("attributes"), dict) else record
    region_id = values.get("region_id") or values.get("regionId")
    region_name = str(values.get("region_name") or values.get("regionName") or "").lower()
    return region_id in (6, "6") or "london" in region_name
