from __future__ import annotations

import json
from pathlib import Path

from ingestion.base.core import DownloadContext, SourceRun
from ingestion.sources.tfl import download_tfl_arrivals, download_tfl_line_status


def _context(tmp_path: Path, config: dict) -> DownloadContext:
    return DownloadContext(
        config=config,
        mode_name="test",
        mode={},
        output_dir=tmp_path,
        run_id="run-1",
    )


def test_tfl_line_status_uses_optional_key_and_selected_line_ids(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("TFL_API_KEY", raising=False)
    calls: list[tuple[str, dict[str, str] | None]] = []

    def fake_request_json(run, url, *, params=None, **_kwargs):
        calls.append((url, params))
        if url.endswith("/Status"):
            return [
                {
                    "id": "bakerloo",
                    "name": "Bakerloo",
                    "lineStatuses": [{"statusSeverity": 10}],
                }
            ]
        return []

    monkeypatch.setattr("ingestion.sources.tfl.request_json", fake_request_json)
    context = _context(
        tmp_path,
        {
            "tfl": {
                "base_url": "https://api.tfl.gov.uk",
                "selected_line_ids": ["bakerloo", "elizabeth"],
            },
            "tfl_line_status": {},
        },
    )
    run = SourceRun("tfl_line_status", context, "tfl_line_status")

    download_tfl_line_status(run, context)

    assert calls[0] == (
        "https://api.tfl.gov.uk/Line/bakerloo,elizabeth/Status",
        {},
    )
    assert calls[1][0].endswith("/Route")
    assert calls[2][0].endswith("/Disruption")
    status_files = list(run.raw_dir.rglob("status_*.json"))
    assert status_files
    payload = json.loads(status_files[0].read_text(encoding="utf-8"))
    assert payload[0]["id"] == "bakerloo"


def test_tfl_arrivals_writes_jsonl_without_api_key(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("TFL_API_KEY", raising=False)
    calls: list[tuple[str, dict[str, str] | None]] = []

    def fake_request_json(run, url, *, params=None, **_kwargs):
        calls.append((url, params))
        return [
            {
                "id": "prediction-1",
                "naptanId": "940GZZLUKSX",
                "lineId": "victoria",
                "vehicleId": "105",
                "expectedArrival": "2026-05-27T17:44:11Z",
            }
        ]

    monkeypatch.setattr("ingestion.sources.tfl.request_json", fake_request_json)
    context = _context(
        tmp_path,
        {
            "tfl": {
                "base_url": "https://api.tfl.gov.uk",
                "selected_stop_ids": ["940GZZLUKSX"],
            },
            "tfl_arrivals": {},
        },
    )
    run = SourceRun("tfl_arrivals", context, "tfl_arrivals")

    download_tfl_arrivals(run, context)

    assert calls == [
        ("https://api.tfl.gov.uk/StopPoint/940GZZLUKSX/Arrivals", {})
    ]
    jsonl_files = list(run.raw_dir.rglob("*.jsonl"))
    assert jsonl_files
    rows = [
        json.loads(line)
        for line in jsonl_files[0].read_text(encoding="utf-8").splitlines()
    ]
    assert rows[0]["vehicleId"] == "105"
