from __future__ import annotations

from typing import Any

from ingestion.base.core import DownloadContext, SourceFailure, SourceRun
from ingestion.base.http import request_json
from ingestion.base.utils import (
    estimate_record_count,
    sanitize_segment,
    selected_boroughs,
    source_options,
)


def download_openmeteo(run: SourceRun, context: DownloadContext) -> None:
    opts = source_options(context, "openmeteo")
    core_start = context.mode["core_start"]
    core_end = context.mode["core_end"]
    timezone_name = opts.get("timezone", "Europe/London")

    # Build services list with graceful handling for missing URLs
    services: list[tuple[str, str, list[str]]] = []

    # Check air quality
    include_air_quality = opts.get("include_air_quality", True)
    if "air_quality_url" in opts and include_air_quality:
        services.append(("air_quality", opts["air_quality_url"], opts.get("air_quality_hourly", [])))

    # Check weather
    include_weather = opts.get("include_weather", True)
    if "weather_url" in opts and include_weather:
        services.append(("weather", opts["weather_url"], opts.get("weather_hourly", [])))

    # If no services enabled and at least one is expected to be enabled, raise error
    if not services:
        configured_urls = []
        if "air_quality_url" in opts:
            configured_urls.append("air_quality_url")
        if "weather_url" in opts:
            configured_urls.append("weather_url")

        if not configured_urls:
            raise SourceFailure(
                "Open-Meteo air_quality_url and weather_url are not configured. "
                "Set at least one of them in source options."
            )
        else:
            # URLs are configured but both were explicitly disabled
            raise SourceFailure(
                f"Open-Meteo services are disabled. Configured but disabled: {', '.join(configured_urls)}. "
                "Remove them from config or set 'include_air_quality': true / 'include_weather': true."
            )

    boroughs = list(selected_boroughs(context))
    if not boroughs:
        raise SourceFailure("No boroughs selected for Open-Meteo download.")

    for borough in boroughs:
        borough_slug = sanitize_segment(borough["name"])
        for kind, url, hourly in services:
            chunk_id = f"{kind}:{borough_slug}:{core_start}:{core_end}"
            if run.should_skip(chunk_id):
                continue
            params = {
                "latitude": borough["latitude"],
                "longitude": borough["longitude"],
                "hourly": ",".join(hourly) if hourly else None,
                "start_date": core_start,
                "end_date": core_end,
                "timezone": timezone_name,
            }
            # Remove None values
            params = {k: v for k, v in params.items() if v is not None}
            payload = request_json(run, url, params=params)
            record_count = estimate_record_count(payload)
            rel = f"kind={kind}/borough={borough_slug}/year={core_start[:4]}/{kind}.json"
            run.write_json(rel, payload, record_count=record_count)
            run.mark_complete(
                chunk_id,
                {"borough": borough["name"], "kind": kind, "record_count": record_count},
            )
