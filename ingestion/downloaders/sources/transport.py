from __future__ import annotations

import json
import os
import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse

from ingestion.downloaders.core import DownloadContext, SourceFailure, SourceRun
from ingestion.downloaders.http import download_file, request_json, request_text
from ingestion.downloaders.utils import (
    extract_records,
    sanitize_segment,
    source_options,
    years_between,
)


STATS19_TABLE_NAMES = {
    "collision": "collisions",
    "vehicle": "vehicles",
    "casualty": "casualties",
}

STATS19_FILENAME_RE = re.compile(
    r"^dft-road-casualty-statistics-(collision|vehicle|casualty)-(.+)\.csv$",
    re.IGNORECASE,
)


def download_stats19(run: SourceRun, context: DownloadContext) -> None:
    opts = source_options(context, "stats19")
    max_bytes = int(opts.get("max_bytes_per_file", 500_000_000))
    start_year = context.mode.get("transport_start_year")
    end_year = context.mode.get("transport_end_year")

    if opts.get("crawl_source_page", True):
        files = discover_stats19_files(run, opts)
    else:
        files = configured_stats19_files(opts)

    if not files:
        raise SourceFailure("No STATS19 CSV files discovered or configured.")

    run.write_json("metadata/stats19_discovered_files.json", files, record_count=len(files))
    selected_files = select_stats19_files(files, opts)
    if not selected_files:
        raise SourceFailure("No STATS19 CSV files matched selected_groups/selected_periods.")

    for file_spec in selected_files:
        table_name = str(file_spec["table"])
        period = str(file_spec["period"])
        group = str(file_spec["group"])
        url = str(file_spec["url"])
        chunk_id = f"stats19:{group}:{table_name}:{period}"
        if run.should_skip(chunk_id):
            continue
        rel = (
            f"group={sanitize_segment(group)}/table={sanitize_segment(table_name)}"
            f"/period={sanitize_segment(period)}/stats19_{table_name}_{sanitize_segment(period)}.csv"
        )
        path, row_count = download_file(run, url, relative_path=rel, max_bytes=max_bytes, timeout=180)
        run.mark_complete(
            chunk_id,
            {
                "record_count": row_count,
                "path": str(path),
                "source_url": url,
                "transport_year_range": f"{start_year}-{end_year}",
            },
        )


def discover_stats19_files(run: SourceRun, opts: dict[str, Any]) -> list[dict[str, Any]]:
    source_page_url = str(
        opts.get("source_page_url")
        or "https://www.gov.uk/government/statistical-data-sets/road-safety-open-data"
    )
    html = request_text(run, source_page_url, timeout=60)
    anchors = GovukLinkParser(source_page_url).parse(html)
    files: list[dict[str, Any]] = []
    for anchor in anchors:
        href = anchor["href"]
        parsed = urlparse(href)
        filename = parsed.path.rsplit("/", 1)[-1]
        match = STATS19_FILENAME_RE.match(filename)
        if not match:
            continue
        table_key, period = match.groups()
        table = STATS19_TABLE_NAMES[table_key.lower()]
        files.append(
            {
                "table": table,
                "period": period.lower(),
                "group": stats19_period_group(period.lower()),
                "url": href,
                "title": anchor["text"],
                "source_page_url": source_page_url,
            }
        )

    latest_final_year = max(
        (int(file_spec["period"]) for file_spec in files if str(file_spec["period"]).isdigit()),
        default=None,
    )
    for file_spec in files:
        if latest_final_year is not None and file_spec["period"] == str(latest_final_year):
            file_spec["group"] = "latest_final_year"

    files.sort(key=lambda item: (str(item["group"]), str(item["period"]), str(item["table"])))
    return files


def configured_stats19_files(opts: dict[str, Any]) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    for table_name, file_spec in opts.get("files", {}).items():
        env_name = file_spec.get("env")
        url = os.environ.get(env_name, "") if env_name else ""
        url = url or file_spec.get("url")
        if not url:
            continue
        files.append(
            {
                "table": table_name,
                "period": file_spec.get("period", "configured"),
                "group": file_spec.get("group", "configured"),
                "url": url,
                "title": file_spec.get("title", table_name),
                "source_page_url": file_spec.get("source_page_url"),
            }
        )
    return files


def select_stats19_files(files: list[dict[str, Any]], opts: dict[str, Any]) -> list[dict[str, Any]]:
    selected_groups = set(opts.get("selected_groups") or ["latest_provisional", "latest_final_year"])
    selected_periods = {str(period).lower() for period in opts.get("selected_periods", [])}
    selected_tables = set(opts.get("selected_tables") or STATS19_TABLE_NAMES.values())
    selected: list[dict[str, Any]] = []
    for file_spec in files:
        group = str(file_spec.get("group", ""))
        period = str(file_spec.get("period", "")).lower()
        table = str(file_spec.get("table", ""))
        if table not in selected_tables:
            continue
        if group in selected_groups or period in selected_periods:
            selected.append(file_spec)
    return selected


def stats19_period_group(period: str) -> str:
    if period.startswith("provisional-"):
        return "latest_provisional"
    if period == "1979-latest-published-year":
        return "complete_dataset"
    if period == "last-5-years":
        return "last_5_years"
    if period.isdigit():
        return "individual_year"
    return "other"


class GovukLinkParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.links: list[dict[str, str]] = []
        self.current_href: str | None = None
        self.current_text: list[str] = []

    def parse(self, html: str) -> list[dict[str, str]]:
        self.feed(html)
        self.close()
        return self.links

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if not href:
            return
        self.current_href = urljoin(self.base_url, href)
        self.current_text = []

    def handle_data(self, data: str) -> None:
        if self.current_href:
            self.current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or not self.current_href:
            return
        text = " ".join("".join(self.current_text).split())
        self.links.append({"href": self.current_href, "text": text})
        self.current_href = None
        self.current_text = []

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
