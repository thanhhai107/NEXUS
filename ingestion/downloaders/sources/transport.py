from __future__ import annotations

import json
import os
from typing import Any

from ingestion.downloaders.core import DownloadContext, SourceFailure, SourceRun
from ingestion.downloaders.http import download_file, request_json
from ingestion.downloaders.utils import (
    extract_records,
    sanitize_segment,
    source_options,
    years_between,
)


def download_stats19(run: SourceRun, context: DownloadContext) -> None:
    opts = source_options(context, "stats19")
    file_specs = opts.get("files", {})
    max_bytes = int(opts.get("max_bytes_per_file", 500_000_000))
    start_year = context.mode.get("transport_start_year")
    end_year = context.mode.get("transport_end_year")
    for table_name, file_spec in file_specs.items():
        env_name = file_spec.get("env")
        url = os.environ.get(env_name, "") if env_name else ""
        url = url or file_spec.get("url")
        if not url:
            raise SourceFailure(f"No STATS19 URL configured for {table_name}")
        chunk_id = f"stats19:{table_name}:{start_year}-{end_year}"
        if run.should_skip(chunk_id):
            continue
        rel = f"table={sanitize_segment(table_name)}/year_range={start_year}-{end_year}/stats19_{table_name}.csv"
        path, row_count = download_file(run, url, relative_path=rel, max_bytes=max_bytes, timeout=180)
        run.mark_complete(chunk_id, {"record_count": row_count, "path": str(path)})

def download_naptan(run: SourceRun, context: DownloadContext) -> None:
    opts = source_options(context, "naptan")
    url = os.environ.get("NAPTAN_ACCESS_NODES_CSV_URL") or opts.get("url")
    params = dict(opts.get("params", {}))
    chunk_id = "naptan:atco=490"
    if run.should_skip(chunk_id):
        return
    path, row_count = download_file(
        run,
        url,
        params=params,
        relative_path="snapshot=current/atco_area=490/naptan_stops.csv",
        max_bytes=int(opts.get("max_bytes", 200_000_000)),
        timeout=180,
    )
    run.mark_complete(chunk_id, {"record_count": row_count, "path": str(path)})

def download_london_journeys(run: SourceRun, context: DownloadContext) -> None:
    opts = source_options(context, "london_journeys")
    url = os.environ.get("LONDON_PUBLIC_TRANSPORT_JOURNEYS_URL") or opts.get("url")
    chunk_id = "london_journeys:full_csv"
    if run.should_skip(chunk_id):
        return
    path, row_count = download_file(
        run,
        url,
        relative_path="snapshot=current/london_journeys.csv",
        max_bytes=int(opts.get("max_bytes", 50_000_000)),
        timeout=180,
    )
    run.mark_complete(chunk_id, {"record_count": row_count, "path": str(path)})

def download_dft_road_traffic(run: SourceRun, context: DownloadContext) -> None:
    opts = source_options(context, "dft_road_traffic")
    base = os.environ.get("DFT_ROAD_TRAFFIC_API_BASE_URL") or opts.get("base_url")
    page_size = int(opts.get("page_size", 500))
    region_id = int(opts.get("region_id", 6))
    page_limit = context.mode.get("dft_page_limit")
    page_limit_int = int(page_limit) if page_limit is not None else None

    download_dft_paginated_entity(
        run,
        context,
        base,
        "count-points",
        {"region_id": region_id},
        "entity=count_points",
        page_size,
        page_limit_int,
    )
    for year in years_between(context.mode["transport_start_year"], context.mode["transport_end_year"]):
        download_dft_paginated_entity(
            run,
            context,
            base,
            "average-annual-daily-flow",
            {"region_id": region_id, "year": year},
            f"entity=average_annual_daily_flow/year={year}",
            page_size,
            page_limit_int,
        )

def download_dft_paginated_entity(
    run: SourceRun,
    context: DownloadContext,
    base: str,
    endpoint: str,
    filters: dict[str, Any],
    rel_prefix: str,
    page_size: int,
    page_limit: int | None,
) -> None:
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
        raw_records = extract_records(payload)
        records = raw_records
        if endpoint == "count-points":
            records = [record for record in records if is_london_dft_record(record)]
        if records:
            run.write_jsonl(f"{rel_prefix}/page={page:04d}.jsonl", records)
        total += len(records)
        next_url = next_page_url(payload)
        next_params = None
        if not next_url:
            break
        page += 1
    run.mark_complete(chunk_id, {"record_count": total, "pages": page})

def next_page_url(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("next_page_url"), str):
        return payload["next_page_url"]
    links = payload.get("links")
    if isinstance(links, dict) and isinstance(links.get("next"), str):
        return links["next"]
    return None

def is_london_dft_record(record: dict[str, Any]) -> bool:
    values = record.get("attributes") if isinstance(record.get("attributes"), dict) else record
    region_id = values.get("region_id") or values.get("regionId")
    region_name = str(values.get("region_name") or values.get("regionName") or "").lower()
    return region_id in (6, "6") or "london" in region_name
