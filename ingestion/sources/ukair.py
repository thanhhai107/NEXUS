"""
UK-AIR Air Quality Archive source adapter.

The UK-AIR flat-files page is site-oriented: each site page lists CSV files
from several archive folders. This adapter discovers those links per site and
downloads the matching CSV files into Bronze without reshaping the payload.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin

from ingestion.base.core import DownloadContext, SourceFailure, SourceRun
from ingestion.base.http import download_file, request_text
from ingestion.base.utils import sanitize_segment, source_options

DEFAULT_FLAT_FILES_URL = os.getenv("UKAIR_FLAT_FILES_URL", "https://uk-air.defra.gov.uk/data/flat_files")
DEFAULT_SITE_IDS = tuple(os.getenv("UKAIR_SITE_IDS", "BG1,BL0,BN1,BX1,CD1,CR5,GR4,EN1,GR7,HG1,HG4,HR1,HS4,HV1,KC1,LW1,MY1,RB4,TD5,TH4,WA2,WM0").split(","))
DEFAULT_PATH_GROUPS = ("site_data", "15min_site_data", "site_pol_data")


@dataclass(frozen=True)
class UkAirFile:
    url: str
    site_id: str
    filename: str
    path_group: str
    year: int | None
    pollutant: str | None


def download_ukair_air_quality_archive(run: SourceRun, context: DownloadContext) -> None:
    """Discover and download UK-AIR archive CSV files for configured London sites."""

    opts = source_options(context, "ukair_air_quality_archive")
    flat_files_url = str(opts.get("flat_files_url", DEFAULT_FLAT_FILES_URL))
    site_ids = selected_site_ids(opts)
    path_groups = set(option_list(opts.get("path_groups") or DEFAULT_PATH_GROUPS))
    years = selected_years(opts)
    pollutant_filter = selected_pollutants(opts)
    max_files_per_site = nullable_int(opts.get("max_files_per_site"))
    max_bytes = nullable_int(opts.get("max_bytes_per_file"))
    timeout = nullable_int(opts.get("timeout_seconds"))

    if not site_ids:
        raise SourceFailure("UK-AIR archive needs at least one site_id.")

    successes = 0
    discovered_total = 0
    discovery_failures = 0
    for site_id in site_ids:
        try:
            files = discover_site_files(
                run,
                flat_files_url,
                site_id,
                path_groups=path_groups,
                years=years,
                pollutant_filter=pollutant_filter,
                max_files=max_files_per_site,
            )
        except Exception as exc:
            run.mark_failed(f"ukair:discover:site={site_id}", str(exc))
            discovery_failures += 1
            continue

        discovered_total += len(files)
        if not files:
            run.mark_skipped(f"ukair:site={site_id}", "no_matching_csv_links")
            continue

        _write_site_manifest(run, site_id, files)
        for file_info in files:
            chunk_id = (
                f"ukair:{file_info.path_group}:site={site_id}:"
                f"year={file_info.year or 'unknown'}:file={file_info.filename}"
            )
            if run.should_skip(chunk_id):
                successes += 1
                continue

            relative_path = (
                f"group={sanitize_segment(file_info.path_group)}"
                f"/site={sanitize_segment(site_id)}"
                f"/year={file_info.year or 'unknown'}"
                f"/{sanitize_segment(file_info.filename)}"
            )
            try:
                path, row_count = download_file(
                    run,
                    file_info.url,
                    relative_path=relative_path,
                    max_bytes=max_bytes,
                    timeout=timeout,
                )
                run.mark_complete(
                    chunk_id,
                    {
                        "record_count": row_count,
                        "path": str(path),
                        "source_url": file_info.url,
                        "site_id": site_id,
                        "path_group": file_info.path_group,
                        "year": file_info.year,
                        "pollutant": file_info.pollutant,
                    },
                )
                successes += 1
            except Exception as exc:
                run.mark_failed(chunk_id, str(exc))

    if successes == 0 and discovery_failures > 0:
        raise SourceFailure(
            f"All UK-AIR archive downloads failed or no matching files were found. "
            f"sites={len(site_ids)} discovered_files={discovered_total}"
        )


def discover_site_files(
    run: SourceRun,
    flat_files_url: str,
    site_id: str,
    *,
    path_groups: set[str],
    years: set[int] | None,
    pollutant_filter: set[str] | None,
    max_files: int | None,
) -> list[UkAirFile]:
    html = request_text(run, flat_files_url, params={"site_id": site_id})
    if not html.strip():
        run.mark_skipped(f"ukair:site={site_id}", "empty_site_page")
        return []

    parser = LinkParser()
    parser.feed(html)
    discovered: list[UkAirFile] = []

    for href in parser.hrefs:
        absolute = urljoin(flat_files_url, href)
        if ".csv" not in absolute.lower():
            continue
        file_info = parse_ukair_csv_url(absolute, site_id)
        if file_info is None:
            continue
        if file_info.path_group not in path_groups:
            continue
        if years is not None and file_info.year not in years:
            continue
        if pollutant_filter is not None and file_info.pollutant not in pollutant_filter:
            continue
        discovered.append(file_info)

    discovered = sorted(
        dedupe_files(discovered),
        key=lambda item: (item.site_id, item.path_group, item.year or 0, item.pollutant or "", item.filename),
    )
    if max_files is not None:
        return discovered[:max_files]
    return discovered


def parse_ukair_csv_url(url: str, expected_site_id: str) -> UkAirFile | None:
    match = re.search(
        r"/data_files/(?P<group>site_data|15min_site_data|site_pol_data)/(?P<filename>[^/?#]+\.csv)",
        url,
        flags=re.IGNORECASE,
    )
    if not match:
        return None

    filename = match.group("filename")
    stem = filename.removesuffix(".csv")
    parts = stem.split("_")
    if len(parts) < 2 or parts[0].upper() != expected_site_id.upper():
        return None

    year = None
    if parts[-1].isdigit() and len(parts[-1]) == 4:
        year = int(parts[-1])

    pollutant = None
    if len(parts) > 2:
        pollutant = "_".join(parts[1:-1]).upper() or None

    return UkAirFile(
        url=url,
        site_id=expected_site_id.upper(),
        filename=filename,
        path_group=match.group("group"),
        year=year,
        pollutant=pollutant,
    )


def dedupe_files(files: list[UkAirFile]) -> list[UkAirFile]:
    seen: set[str] = set()
    output: list[UkAirFile] = []
    for file_info in files:
        if file_info.url in seen:
            continue
        seen.add(file_info.url)
        output.append(file_info)
    return output


def selected_site_ids(opts: dict[str, Any]) -> list[str]:
    site_ids = option_list(opts.get("site_ids") or DEFAULT_SITE_IDS)
    limit = nullable_int(opts.get("site_limit"))
    if limit is not None:
        site_ids = site_ids[: max(limit, 0)]
    return [site_id.upper() for site_id in site_ids]


def selected_years(opts: dict[str, Any]) -> set[int] | None:
    explicit_years = opts.get("years")
    if explicit_years:
        return {int(year) for year in option_list(explicit_years)}

    year_start = resolve_year(opts.get("year_start"))
    year_end = resolve_year(opts.get("year_end"))
    if year_start is None and year_end is None:
        return None
    if year_start is None:
        year_start = 0
    if year_end is None:
        year_end = datetime.now(timezone.utc).year
    return set(range(int(year_start), int(year_end) + 1))


def selected_pollutants(opts: dict[str, Any]) -> set[str] | None:
    pollutants = option_list(opts.get("pollutants") or [])
    if not pollutants:
        return None
    return {pollutant.upper() for pollutant in pollutants}


def resolve_year(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"", "null", "none", "all"}:
            return None
        if normalized in {"current_year", "now"}:
            return datetime.now(timezone.utc).year
    return int(value)


def nullable_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, str) and value.strip().lower() in {"", "null", "none"}:
        return None
    return int(value)


def option_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return [str(item).strip() for item in value if str(item).strip()]


def _write_site_manifest(run: SourceRun, site_id: str, files: list[UkAirFile]) -> None:
    payload = {
        "source": "UK-AIR Air Quality Archive",
        "site_id": site_id,
        "file_count": len(files),
        "files": [
            {
                "url": file_info.url,
                "filename": file_info.filename,
                "path_group": file_info.path_group,
                "year": file_info.year,
                "pollutant": file_info.pollutant,
            }
            for file_info in files
        ],
    }
    run.write_json(
        f"metadata/site={sanitize_segment(site_id)}/ukair_archive_files.json",
        payload,
        record_count=len(files),
    )


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value:
                self.hrefs.append(value)
