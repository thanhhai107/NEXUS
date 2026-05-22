from __future__ import annotations

from ingestion.downloaders.core import DownloadContext, SourceRun
from ingestion.downloaders.http import request_json
from ingestion.downloaders.utils import (
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
    services = [
        (
            "air_quality",
            opts["air_quality_url"],
            opts.get("air_quality_hourly", []),
        ),
        (
            "weather",
            opts["weather_url"],
            opts.get("weather_hourly", []),
        ),
    ]
    for borough in selected_boroughs(context):
        borough_slug = sanitize_segment(borough["name"])
        for kind, url, hourly in services:
            chunk_id = f"{kind}:{borough_slug}:{core_start}:{core_end}"
            if run.should_skip(chunk_id):
                continue
            params = {
                "latitude": borough["latitude"],
                "longitude": borough["longitude"],
                "hourly": ",".join(hourly),
                "start_date": core_start,
                "end_date": core_end,
                "timezone": timezone_name,
            }
            payload = request_json(run, url, params=params)
            record_count = estimate_record_count(payload)
            rel = f"kind={kind}/borough={borough_slug}/year={core_start[:4]}/{kind}.json"
            run.write_json(rel, payload, record_count=record_count)
            run.mark_complete(
                chunk_id,
                {"borough": borough["name"], "kind": kind, "record_count": record_count},
            )
