"""
STATS19 Road Safety Data Source Adapter.

Downloads UK DfT road safety statistics (STATS19) from gov.uk.
"""

from __future__ import annotations

import os
import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse

from ingestion.base.core import DownloadContext, SourceFailure, SourceRun
from ingestion.base.http import download_file, request_text
from ingestion.base.utils import sanitize_segment, source_options


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
    """Download STATS19 road safety CSV files."""
    opts = source_options(context, "stats19")
    max_bytes = int(opts.get("max_bytes_per_file", 500_000_000))
    start_year = context.mode.get("transport_start_year")
    end_year = context.mode.get("transport_end_year")

    if opts.get("crawl_source_page", True):
        files = _discover_files(run, opts)
    else:
        files = _configured_files(opts)

    if not files:
        raise SourceFailure("No STATS19 CSV files discovered or configured.")

    run.write_json("metadata/stats19_discovered_files.json", files, record_count=len(files))
    selected_files = _select_files(files, opts)
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
        timeout = int(os.getenv("STATS19_DOWNLOAD_TIMEOUT", "180"))
        path, row_count = download_file(run, url, relative_path=rel, max_bytes=max_bytes, timeout=timeout)
        run.mark_complete(
            chunk_id,
            {
                "record_count": row_count,
                "path": str(path),
                "source_url": url,
                "transport_year_range": f"{start_year}-{end_year}",
            },
        )


def _discover_files(run: SourceRun, opts: dict[str, Any]) -> list[dict[str, Any]]:
    """Discover STATS19 files from gov.uk source page."""
    source_page_url = str(
        opts.get("source_page_url")
        or "https://www.gov.uk/government/statistical-data-sets/road-safety-open-data"
    )
    html = request_text(run, source_page_url, timeout=60)
    anchors = _GovukLinkParser(source_page_url).parse(html)
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
                "group": _period_group(period.lower()),
                "url": href,
                "title": anchor["text"],
                "source_page_url": source_page_url,
            }
        )

    latest_final_year = max(
        (int(f["period"]) for f in files if str(f["period"]).isdigit()),
        default=None,
    )
    for f in files:
        if latest_final_year is not None and f["period"] == str(latest_final_year):
            f["group"] = "latest_final_year"

    files.sort(key=lambda item: (str(item["group"]), str(item["period"]), str(item["table"])))
    return files


def _configured_files(opts: dict[str, Any]) -> list[dict[str, Any]]:
    """Get configured STATS19 files."""
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


def _select_files(files: list[dict[str, Any]], opts: dict[str, Any]) -> list[dict[str, Any]]:
    """Select files based on groups, periods, and tables."""
    selected_groups = set(opts.get("selected_groups") or ["latest_provisional", "latest_final_year"])
    selected_periods = {str(p).lower() for p in opts.get("selected_periods", [])}
    selected_tables = set(opts.get("selected_tables") or STATS19_TABLE_NAMES.values())
    selected: list[dict[str, Any]] = []

    for f in files:
        group = str(f.get("group", ""))
        period = str(f.get("period", "")).lower()
        table = str(f.get("table", ""))
        if table not in selected_tables:
            continue
        if group in selected_groups or period in selected_periods:
            selected.append(f)
    return selected


def _period_group(period: str) -> str:
    """Classify period into group."""
    if period.startswith("provisional-"):
        return "latest_provisional"
    if period == "1979-latest-published-year":
        return "complete_dataset"
    if period == "last-5-years":
        return "last_5_years"
    if period.isdigit():
        return "individual_year"
    return "other"


class _GovukLinkParser(HTMLParser):
    """Parse links from gov.uk pages."""

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
