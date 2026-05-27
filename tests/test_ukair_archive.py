from __future__ import annotations

from pathlib import Path
from typing import Any

from ingestion.base.core import DownloadContext, SourceRun
from ingestion.sources.ukair import (
    discover_site_files,
    download_ukair_air_quality_archive,
    parse_ukair_csv_url,
)


def test_parse_ukair_csv_url_identifies_group_year_and_pollutant() -> None:
    all_hourly = parse_ukair_csv_url(
        "https://uk-air.defra.gov.uk/datastore/data_files/site_data/MY1_1997.csv?v=1",
        "MY1",
    )
    site_pollutant = parse_ukair_csv_url(
        "https://uk-air.defra.gov.uk/datastore/data_files/site_pol_data/MY1_ETHANE_2026.csv",
        "MY1",
    )

    assert all_hourly is not None
    assert all_hourly.path_group == "site_data"
    assert all_hourly.year == 1997
    assert all_hourly.pollutant is None

    assert site_pollutant is not None
    assert site_pollutant.path_group == "site_pol_data"
    assert site_pollutant.year == 2026
    assert site_pollutant.pollutant == "ETHANE"


def test_discover_site_files_filters_years_and_path_groups(monkeypatch, tmp_path: Path) -> None:
    html = """
    <a href="https://uk-air.defra.gov.uk/datastore/data_files/site_data/MY1_2025.csv?v=1">2025</a>
    <a href="https://uk-air.defra.gov.uk/datastore/data_files/15min_site_data/MY1_2025.csv">15min</a>
    <a href="https://uk-air.defra.gov.uk/datastore/data_files/site_pol_data/MY1_O3_2026.csv">O3</a>
    <a href="https://uk-air.defra.gov.uk/datastore/data_files/site_data/MY1_1997.csv">old</a>
    """
    run = SourceRun("ukair_air_quality_archive", make_context(tmp_path), "ukair_air_quality_archive")
    monkeypatch.setattr("ingestion.sources.ukair.request_text", lambda *_args, **_kwargs: html)

    files = discover_site_files(
        run,
        "https://uk-air.defra.gov.uk/data/flat_files",
        "MY1",
        path_groups={"site_data", "15min_site_data", "site_pol_data"},
        years={2025, 2026},
        pollutant_filter=None,
        max_files=None,
    )

    assert [file.filename for file in files] == ["MY1_2025.csv", "MY1_2025.csv", "MY1_O3_2026.csv"]


def test_download_ukair_archive_discovers_and_downloads_csvs(monkeypatch, tmp_path: Path) -> None:
    pages = {
        "MY1": '<a href="https://uk-air.defra.gov.uk/datastore/data_files/site_data/MY1_2025.csv?v=1">MY1</a>',
        "KC1": '<a href="https://uk-air.defra.gov.uk/datastore/data_files/site_pol_data/KC1_BC_2026.csv">KC1 BC</a>',
    }

    class FakeResponse:
        status_code = 200
        reason = "ok"
        headers: dict[str, str] = {}

        def __init__(self, text: str) -> None:
            self.text = text
            self.content = text.encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int):
            yield self.content

    def fake_get(url: str, **kwargs: Any):
        params = kwargs.get("params") or {}
        if "flat_files" in url:
            return FakeResponse(pages[str(params["site_id"])])
        return FakeResponse("date,value\n2025-01-01,1\n")

    monkeypatch.setattr("requests.get", fake_get)
    run_context = make_context(
        tmp_path,
        options={
            "site_ids": ["MY1", "KC1"],
            "year_start": 2025,
            "year_end": 2026,
            "path_groups": ["site_data", "site_pol_data"],
        },
    )
    run = SourceRun(
        "ukair_air_quality_archive",
        run_context,
        "ukair_air_quality_archive",
        dataset_name="ukair_air_quality_archive",
    )

    download_ukair_air_quality_archive(run, run_context)

    csv_files = sorted(path.name for path in run.raw_dir.rglob("*.csv"))
    assert csv_files == ["KC1_BC_2026.csv", "MY1_2025.csv"]


def test_download_ukair_archive_skips_empty_site_pages(monkeypatch, tmp_path: Path) -> None:
    pages = {
        "MY1": "",
    }

    class FakeResponse:
        status_code = 200
        reason = "ok"
        headers: dict[str, str] = {}

        def __init__(self, text: str) -> None:
            self.text = text
            self.content = text.encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int):
            yield self.content

    def fake_get(url: str, **kwargs: Any):
        params = kwargs.get("params") or {}
        if "flat_files" in url:
            return FakeResponse(pages[str(params["site_id"])])
        return FakeResponse("date,value\n2025-01-01,1\n")

    monkeypatch.setattr("requests.get", fake_get)
    run_context = make_context(
        tmp_path,
        options={
            "site_ids": ["MY1"],
            "year_start": 2025,
            "year_end": 2025,
            "path_groups": ["site_data"],
        },
    )
    run = SourceRun(
        "ukair_air_quality_archive",
        run_context,
        "ukair_air_quality_archive",
        dataset_name="ukair_air_quality_archive",
    )

    download_ukair_air_quality_archive(run, run_context)

    assert not list(run.raw_dir.rglob("*.csv"))


def make_context(tmp_path: Path, options: dict[str, Any] | None = None) -> DownloadContext:
    return DownloadContext(
        config={
            "ukair_air_quality_archive": {
                **(options or {}),
            },
            "resilient_runtime": {
                "retry_policy": {
                    "max_attempts": 1,
                    "backoff_base_seconds": 0,
                    "backoff_max_seconds": 0,
                    "jitter_seconds": 0,
                },
                "coverage_policy": {
                    "min_success_ratio": 1.0,
                    "allow_publish_with_warnings": False,
                    "required_chunks": [],
                },
            },
            "rate_limits": {"default_delay_seconds": 0},
        },
        mode_name="test",
        mode={},
        output_dir=tmp_path,
        run_id="run-1",
    )
