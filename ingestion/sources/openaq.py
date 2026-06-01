from __future__ import annotations

import os
from typing import Any

from ingestion.base.core import DownloadContext, SourceRun
from ingestion.base.http import request_json, require_env
from ingestion.base.utils import (
    extract_records,
    iso_end,
    iso_start,
    limit_items,
    month_ranges,
    sanitize_segment,
    source_options,
)


def download_openaq(run: SourceRun, context: DownloadContext) -> None:
    env = require_env(run, "OPENAQ_API_KEY")
    opts = source_options(context, "openaq")
    base = openaq_base_url(os.environ.get("OPENAQ_API_URL") or opts.get("base_url"))
    headers = {"X-API-Key": env["OPENAQ_API_KEY"]}
    page_limit = min(int(opts.get("page_limit", 1000)), 1000)

    if opts.get("include_parameters", True):
        download_openaq_parameters(run, base, headers, page_limit)

    locations = discover_openaq_locations(run, context, base, headers, opts, page_limit)
    location_limit = context.mode.get("openaq_location_limit")
    locations = limit_items(locations, int(location_limit) if location_limit is not None else None)
    run.write_jsonl("discovery/locations.jsonl", locations)

    sensors = discover_openaq_sensors(run, base, headers, locations, page_limit)
    sensor_limit = context.mode.get("openaq_sensor_limit")
    sensors = limit_items(sensors, int(sensor_limit) if sensor_limit is not None else None)
    run.write_jsonl("discovery/location_sensors.jsonl", sensors)

    if opts.get("include_sensor_details", True):
        sensor_details = download_openaq_sensor_details(run, base, headers, sensors)
        if sensor_details:
            sensors = sensor_details
            run.write_jsonl("discovery/sensors.jsonl", sensor_details)
        else:
            run.write_jsonl("discovery/sensors.jsonl", sensors)
    else:
        run.write_jsonl("discovery/sensors.jsonl", sensors)

    endpoint_name = str(opts.get("measurement_endpoint", "hours"))
    for sensor in sensors:
        sensor_id = sensor.get("id") or sensor.get("sensor_id")
        if sensor_id is None:
            continue
        for start, end in month_ranges(context.mode["core_start"], context.mode["core_end"]):
            download_openaq_sensor_values(
                run,
                base,
                headers,
                sensor_id=sensor_id,
                endpoint_name=endpoint_name,
                start_date=start,
                end_date=end,
                page_limit=page_limit,
            )


def openaq_base_url(configured_url: Any = None) -> str:
    """Normalize OpenAQ host/base/endpoint config to the v3 API base."""
    base = str(configured_url or os.environ.get("OPENAQ_API_URL", "https://api.openaq.org/v3")).strip().rstrip("/")
    for suffix in ("/measurements", "/locations", "/sensors", "/parameters", "/hours"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    if base.endswith("/v3"):
        return base
    if base.endswith("/v2"):
        return f"{base[:-3]}/v3"
    return f"{base}/v3"


def download_openaq_parameters(
    run: SourceRun,
    base: str,
    headers: dict[str, str],
    page_limit: int,
) -> list[dict[str, Any]]:
    parameters = download_openaq_collection(
        run,
        base,
        headers,
        "parameters",
        {},
        chunk_id="openaq:parameters",
        relative_prefix="reference/parameters",
        page_limit=page_limit,
    )
    if parameters:
        run.write_jsonl("reference/parameters.jsonl", parameters)
    return parameters


def discover_openaq_locations(
    run: SourceRun,
    context: DownloadContext,
    base: str,
    headers: dict[str, str],
    opts: dict[str, Any],
    page_limit: int,
) -> list[dict[str, Any]]:
    center = context.spatial_scope.get("center", {"latitude": 51.5074, "longitude": -0.1278})
    radius_meters = int(opts.get("radius_meters", 40000))
    discovery_strategy = str(opts.get("discovery_strategy", "bbox")).lower()
    params: dict[str, Any] = {}
    if discovery_strategy == "bbox" and context.bbox:
        bbox = context.bbox
        params["bbox"] = f"{bbox['west']},{bbox['south']},{bbox['east']},{bbox['north']}"
    else:
        params["coordinates"] = f"{center['latitude']},{center['longitude']}"
        params["radius"] = min(radius_meters, 25000)
    return download_openaq_collection(
        run,
        base,
        headers,
        "locations",
        params,
        chunk_id=f"openaq:locations:{params}",
        relative_prefix="discovery/location_pages",
        page_limit=page_limit,
    )


def discover_openaq_sensors(
    run: SourceRun,
    base: str,
    headers: dict[str, str],
    locations: list[dict[str, Any]],
    page_limit: int,
) -> list[dict[str, Any]]:
    sensors: list[dict[str, Any]] = []
    seen: set[str] = set()
    for location in locations:
        location_id = location.get("id")
        embedded = location.get("sensors")
        if isinstance(embedded, list) and embedded:
            for sensor in embedded:
                add_openaq_sensor(sensors, seen, sensor, location)
            continue
        if location_id is None:
            continue
        records = download_openaq_collection(
            run,
            base,
            headers,
            f"locations/{location_id}/sensors",
            {},
            chunk_id=f"openaq:location_sensors:{location_id}",
            relative_prefix=f"discovery/location_sensor_pages/location_id={sanitize_segment(location_id)}",
            page_limit=page_limit,
        )
        for sensor in records:
            add_openaq_sensor(sensors, seen, sensor, location)
    return sensors


def add_openaq_sensor(
    sensors: list[dict[str, Any]],
    seen: set[str],
    sensor: Any,
    location: dict[str, Any],
) -> None:
    if not isinstance(sensor, dict):
        return
    sensor_id = str(sensor.get("id") or sensor.get("sensor_id") or "")
    if not sensor_id or sensor_id in seen:
        return
    enriched = dict(sensor)
    enriched["_location_id"] = location.get("id")
    enriched["_location_name"] = location.get("name")
    sensors.append(enriched)
    seen.add(sensor_id)


def download_openaq_sensor_details(
    run: SourceRun,
    base: str,
    headers: dict[str, str],
    sensors: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    for sensor in sensors:
        sensor_id = sensor.get("id") or sensor.get("sensor_id")
        if sensor_id is None:
            continue
        chunk_id = f"openaq:sensor_detail:{sensor_id}"
        rel = f"discovery/sensor_details/sensor_id={sanitize_segment(sensor_id)}.json"
        if run.should_skip(chunk_id):
            continue
        try:
            payload = request_json(run, f"{base}/sensors/{sensor_id}", headers=headers)
            records = extract_records(payload)
            detail = records[0] if records else {}
            if isinstance(detail, dict):
                merged = {**detail, "_location_id": sensor.get("_location_id"), "_location_name": sensor.get("_location_name")}
                path = run.write_json(rel, merged, record_count=1)
                run.mark_complete(chunk_id, {"record_count": 1, "path": str(path)})
                details.append(merged)
        except Exception as exc:
            run.mark_failed(chunk_id, str(exc))
            details.append(sensor)
    return details


def download_openaq_sensor_values(
    run: SourceRun,
    base: str,
    headers: dict[str, str],
    *,
    sensor_id: Any,
    endpoint_name: str,
    start_date: Any,
    end_date: Any,
    page_limit: int,
) -> None:
    chunk_id = f"openaq:{endpoint_name}:sensor={sensor_id}:month={start_date:%Y-%m}"
    if run.should_skip(chunk_id):
        return
    params = {
        "datetime_from": iso_start(start_date),
        "datetime_to": iso_end(end_date),
    }
    rel_prefix = (
        f"{endpoint_name}/sensor_id={sanitize_segment(sensor_id)}"
        f"/year={start_date.year}/month={start_date.month:02d}"
    )
    try:
        records = download_openaq_collection(
            run,
            base,
            headers,
            f"sensors/{sensor_id}/{endpoint_name}",
            params,
            chunk_id=chunk_id,
            relative_prefix=rel_prefix,
            page_limit=page_limit,
            mark_complete=False,
        )
        run.mark_complete(chunk_id, {"record_count": len(records)})
    except Exception as exc:
        run.mark_failed(chunk_id, str(exc))


def download_openaq_collection(
    run: SourceRun,
    base: str,
    headers: dict[str, str],
    endpoint: str,
    params: dict[str, Any],
    *,
    chunk_id: str,
    relative_prefix: str,
    page_limit: int,
    mark_complete: bool = True,
) -> list[dict[str, Any]]:
    if run.should_skip(chunk_id):
        return []
    records_all: list[dict[str, Any]] = []
    page = 1
    while True:
        payload = request_json(
            run,
            f"{base}/{endpoint}",
            headers=headers,
            params={**params, "limit": page_limit, "page": page},
        )
        records = extract_records(payload)
        if records:
            run.write_jsonl(f"{relative_prefix}/page={page:04d}.jsonl", records)
            records_all.extend(records)
        if not should_continue_openaq_page(payload, records, page_limit):
            break
        page += 1
    if mark_complete:
        run.mark_complete(chunk_id, {"record_count": len(records_all), "pages": page})
    return records_all


def should_continue_openaq_page(payload: Any, records: list[dict[str, Any]], page_limit: int) -> bool:
    if len(records) < page_limit:
        return False
    meta = payload.get("meta") if isinstance(payload, dict) else None
    if not isinstance(meta, dict):
        return True
    found = meta.get("found")
    if isinstance(found, int):
        page = int(meta.get("page") or 1)
        limit = int(meta.get("limit") or page_limit)
        return page * limit < found
    return True
