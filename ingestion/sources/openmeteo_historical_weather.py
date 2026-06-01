"""
Open-Meteo Historical Weather grid source adapter.

Downloads CSV weather history for a generated coordinate grid over the
configured bounding box. The adapter keeps the source payload as CSV in Bronze;
normalization into Silver can be handled by downstream processing.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any, Iterable

from ingestion.base.core import DownloadContext, SourceFailure, SourceRun
from ingestion.base.http import download_file
from ingestion.base.utils import sanitize_segment, source_options

DEFAULT_ARCHIVE_URL = os.getenv("OPENMETEO_HISTORICAL_ARCHIVE_URL", "https://archive-api.open-meteo.com/v1/archive")
DEFAULT_GRID_SPACING_KM = 10.0
DEFAULT_MAX_LOCATIONS_PER_REQUEST = 50
KM_PER_LAT_DEGREE = 111.32


@dataclass(frozen=True)
class GridPoint:
    latitude: float
    longitude: float


@dataclass(frozen=True)
class BoundingBox:
    lat_min: float
    lon_min: float
    lat_max: float
    lon_max: float


def download_openmeteo_historical_weather(run: SourceRun, context: DownloadContext) -> None:
    """Download historical weather CSV for grid points covering the configured bbox."""

    opts = source_options(context, "openmeteo_historical_weather")
    bbox = resolve_bbox(context, opts)
    spacing_km = float(opts.get("grid_spacing_km", DEFAULT_GRID_SPACING_KM))
    max_locations = max(1, int(opts.get("max_locations_per_request", DEFAULT_MAX_LOCATIONS_PER_REQUEST)))
    grid = generate_grid_points(bbox, spacing_km)

    if not grid:
        raise SourceFailure("Open-Meteo historical weather grid is empty.")

    url = str(opts.get("archive_url") or opts.get("weather_url") or DEFAULT_ARCHIVE_URL)
    start_date = str(opts.get("start_date") or context.mode["core_start"])
    end_date = str(opts.get("end_date") or context.mode["core_end"])
    timezone_name = str(opts.get("timezone", "GMT"))
    output_format = str(opts.get("format", "csv"))
    hourly = option_list(opts.get("hourly") or opts.get("weather_hourly") or [])
    daily = option_list(opts.get("daily") or [])
    max_bytes = opts.get("max_bytes_per_file")
    timeout = opts.get("timeout_seconds")

    if not hourly and not daily:
        raise SourceFailure("Open-Meteo historical weather needs at least one hourly or daily variable.")

    _write_grid_metadata(run, bbox, grid, spacing_km, start_date, end_date, timezone_name)

    successes = 0
    failures: list[str] = []
    for batch_index, batch in enumerate(batch_points(grid, max_locations), start=1):
        chunk_id = (
            "openmeteo_historical_weather:"
            f"batch={batch_index:03d}:points={len(batch)}:{start_date}:{end_date}"
        )
        if run.should_skip(chunk_id):
            successes += 1
            continue

        params = {
            "latitude": ",".join(format_coord(point.latitude) for point in batch),
            "longitude": ",".join(format_coord(point.longitude) for point in batch),
            "start_date": start_date,
            "end_date": end_date,
            "timezone": timezone_name,
            "format": output_format,
            "timeformat": str(opts.get("timeformat", "iso8601")),
            "cell_selection": str(opts.get("cell_selection", "nearest")),
        }
        if hourly:
            params["hourly"] = ",".join(hourly)
        if daily:
            params["daily"] = ",".join(daily)

        relative_path = (
            f"grid_spacing_km={sanitize_segment(format_distance(spacing_km))}"
            f"/start={sanitize_segment(start_date)}_end={sanitize_segment(end_date)}"
            f"/batch={batch_index:03d}/openmeteo_historical_weather.{output_format}"
        )

        try:
            path, row_count = download_file(
                run,
                url,
                relative_path=relative_path,
                params=params,
                max_bytes=int(max_bytes) if max_bytes is not None else None,
                timeout=int(timeout) if timeout is not None else None,
            )
            run.mark_complete(
                chunk_id,
                {
                    "record_count": row_count,
                    "path": str(path),
                    "point_count": len(batch),
                    "grid_spacing_km": spacing_km,
                    "bbox": bbox_to_dict(bbox),
                    "latitude": params["latitude"],
                    "longitude": params["longitude"],
                    "format": output_format,
                },
            )
            successes += 1
        except Exception as exc:
            failures.append(f"batch={batch_index:03d}: {exc}")
            run.mark_failed(chunk_id, str(exc))

    if successes == 0:
        detail = "; ".join(failures[:3])
        suffix = f" First failures: {detail}" if detail else ""
        raise SourceFailure(f"All Open-Meteo historical weather grid requests failed.{suffix}")


def resolve_bbox(context: DownloadContext, opts: dict[str, Any]) -> BoundingBox:
    """Resolve bbox from source options, then spatial_scope.bbox."""

    raw_bbox = opts.get("bbox")
    if not isinstance(raw_bbox, dict):
        raw_bbox = context.bbox

    lat_min = raw_bbox.get("lat_min", raw_bbox.get("south"))
    lat_max = raw_bbox.get("lat_max", raw_bbox.get("north"))
    lon_min = raw_bbox.get("lon_min", raw_bbox.get("west"))
    lon_max = raw_bbox.get("lon_max", raw_bbox.get("east"))

    if None in {lat_min, lat_max, lon_min, lon_max}:
        raise SourceFailure("Open-Meteo historical weather requires bbox south/north/west/east values.")

    bbox = BoundingBox(
        lat_min=float(lat_min),
        lon_min=float(lon_min),
        lat_max=float(lat_max),
        lon_max=float(lon_max),
    )
    if bbox.lat_min > bbox.lat_max or bbox.lon_min > bbox.lon_max:
        raise SourceFailure(f"Invalid bbox bounds: {bbox_to_dict(bbox)}")
    return bbox


def generate_grid_points(bbox: BoundingBox, spacing_km: float) -> list[GridPoint]:
    """Generate an inclusive lat/lon grid with approximate spacing in kilometers."""

    if spacing_km <= 0:
        raise SourceFailure("grid_spacing_km must be greater than 0.")

    mid_lat = (bbox.lat_min + bbox.lat_max) / 2
    lat_step = spacing_km / KM_PER_LAT_DEGREE
    lon_km_per_degree = max(KM_PER_LAT_DEGREE * math.cos(math.radians(mid_lat)), 1e-6)
    lon_step = spacing_km / lon_km_per_degree

    latitudes = inclusive_values(bbox.lat_min, bbox.lat_max, lat_step)
    longitudes = inclusive_values(bbox.lon_min, bbox.lon_max, lon_step)
    return [GridPoint(latitude=lat, longitude=lon) for lat in latitudes for lon in longitudes]


def inclusive_values(start: float, end: float, step: float) -> list[float]:
    values: list[float] = []
    current = start
    epsilon = step / 1000
    while current <= end + epsilon:
        values.append(round(min(current, end), 4))
        current += step
    rounded_end = round(end, 4)
    if not values or values[-1] != rounded_end:
        values.append(rounded_end)
    return values


def batch_points(points: list[GridPoint], batch_size: int) -> Iterable[list[GridPoint]]:
    for index in range(0, len(points), batch_size):
        yield points[index : index + batch_size]


def option_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, Iterable):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def format_coord(value: float) -> str:
    return f"{value:.4f}"


def format_distance(value: float) -> str:
    return str(int(value)) if value.is_integer() else f"{value:g}"


def bbox_to_dict(bbox: BoundingBox) -> dict[str, float]:
    return {
        "lat_min": bbox.lat_min,
        "lon_min": bbox.lon_min,
        "lat_max": bbox.lat_max,
        "lon_max": bbox.lon_max,
    }


def _write_grid_metadata(
    run: SourceRun,
    bbox: BoundingBox,
    grid: list[GridPoint],
    spacing_km: float,
    start_date: str,
    end_date: str,
    timezone_name: str,
) -> None:
    payload = {
        "source": "Open-Meteo Historical Weather",
        "bbox": bbox_to_dict(bbox),
        "grid_spacing_km": spacing_km,
        "point_count": len(grid),
        "start_date": start_date,
        "end_date": end_date,
        "timezone": timezone_name,
        "points": [
            {"location_id": index + 1, "latitude": point.latitude, "longitude": point.longitude}
            for index, point in enumerate(grid)
        ],
    }
    run.write_json("metadata/openmeteo_historical_weather_grid.json", payload, record_count=len(grid))
