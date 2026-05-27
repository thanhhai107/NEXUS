from __future__ import annotations

from pathlib import Path
from typing import Any

from ingestion.base.core import DownloadContext, SourceRun
from ingestion.sources.openmeteo_historical_weather import (
    BoundingBox,
    download_openmeteo_historical_weather,
    generate_grid_points,
)


def test_generate_grid_points_covers_bbox_edges() -> None:
    bbox = BoundingBox(lat_min=51.28, lon_min=-0.51, lat_max=51.69, lon_max=0.33)

    points = generate_grid_points(bbox, spacing_km=10)

    assert points[0].latitude == 51.28
    assert points[0].longitude == -0.51
    assert any(point.latitude == 51.69 for point in points)
    assert any(point.longitude == 0.33 for point in points)
    assert len(points) > 16


def test_download_openmeteo_historical_weather_uses_csv_grid_params(monkeypatch, tmp_path: Path) -> None:
    calls: list[dict[str, Any]] = []

    class FakeResponse:
        status_code = 200
        reason = "ok"
        headers: dict[str, str] = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int):
            yield b"time,location_id,temperature_2m\n2025-01-01T00:00,0,7.2\n"

    def fake_get(url: str, **kwargs):
        calls.append({"url": url, **kwargs})
        return FakeResponse()

    monkeypatch.setattr("requests.get", fake_get)

    context = DownloadContext(
        config={
            "spatial_scope": {
                "bbox": {
                    "south": 51.28,
                    "north": 51.29,
                    "west": -0.51,
                    "east": -0.50,
                }
            },
            "openmeteo_historical_weather": {
                "archive_url": "https://archive-api.open-meteo.com/v1/archive",
                "grid_spacing_km": 10,
                "max_locations_per_request": 99,
                "timezone": "GMT",
                "format": "csv",
                "hourly": ["temperature_2m"],
                "daily": ["precipitation_sum"],
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
        mode={"core_start": "2025-01-01", "core_end": "2025-01-02"},
        output_dir=tmp_path,
        run_id="run-1",
    )
    run = SourceRun(
        "openmeteo_historical_weather",
        context,
        "openmeteo_historical_weather",
        dataset_name="openmeteo_historical_weather",
    )

    download_openmeteo_historical_weather(run, context)

    assert calls
    params = calls[0]["params"]
    assert params["format"] == "csv"
    assert params["timezone"] == "GMT"
    assert params["hourly"] == "temperature_2m"
    assert params["daily"] == "precipitation_sum"
    assert params["latitude"] == "51.2800,51.2800,51.2900,51.2900"
    assert params["longitude"] == "-0.5100,-0.5000,-0.5100,-0.5000"

    csv_files = list(run.raw_dir.rglob("openmeteo_historical_weather.csv"))
    assert len(csv_files) == 1
    assert "temperature_2m" in csv_files[0].read_text(encoding="utf-8")
