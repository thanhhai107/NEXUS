"""
WAQI (World Air Quality Index) Source Adapter.

Downloads air quality measurements from WAQI stations within the configured bounding box.
"""

from __future__ import annotations

import os
from typing import Any

from ingestion.base.core import DownloadContext, SourceFailure, SourceRun
from ingestion.base.http import request_json, require_env
from ingestion.base.utils import (
    extract_records,
    limit_items,
    poll_time_slug,
    sanitize_segment,
    source_options,
)


def download_waqi(run: SourceRun, context: DownloadContext) -> None:
    """Download WAQI air quality measurements for stations in bounding box."""
    env = require_env(run, "WAQI_API_TOKEN")
    opts = source_options(context, "waqi")
    base = (os.environ.get("WAQI_API_URL") or opts.get("base_url", "https://api.waqi.info")).rstrip("/")
    map_path = str(os.environ.get("WAQI_MAP_PATH", opts.get("map_path", "/v2/map/bounds")))
    bbox = context.bbox
    token = env["WAQI_API_TOKEN"]

    map_params = {
        "latlng": f"{bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']}",
        "token": token,
    }
    networks = opts.get("networks")
    if networks:
        map_params["networks"] = networks

    poll_time = poll_time_slug(context)
    map_chunk_id = f"waqi:map_bounds:poll={poll_time['stamp']}"
    stations: list[dict[str, Any]] = []

    if not run.should_skip(map_chunk_id):
        try:
            payload = request_json(
                run,
                f"{base}/{map_path.strip('/')}",
                params=map_params,
            )
            _ensure_waqi_ok(payload, "map/bounds")
            stations = extract_records(payload)
            station_limit = context.mode.get("waqi_station_limit")
            stations = limit_items(stations, int(station_limit) if station_limit is not None else None)
            run.write_jsonl(f"date={poll_time['date']}/hour={poll_time['hour']}/stations.jsonl", stations)
            run.mark_complete(map_chunk_id, {"record_count": len(stations)})
        except Exception as exc:
            run.mark_failed(map_chunk_id, str(exc))
            raise

    for station in stations:
        uid = station.get("uid")
        if uid is None:
            continue
        chunk_id = f"uid={uid}:poll={poll_time['stamp']}"
        try:
            feed = request_json(run, f"{base}/feed/@{uid}/", params={"token": token})
            _ensure_waqi_ok(feed, f"feed/@{uid}")
            rel = f"date={poll_time['date']}/hour={poll_time['hour']}/feed_uid={sanitize_segment(uid)}.json"
            run.write_json(rel, feed, record_count=1)
            run.mark_complete(chunk_id, {"record_count": 1})
        except Exception as exc:
            run.mark_failed(chunk_id, str(exc))


def _ensure_waqi_ok(payload: Any, endpoint: str) -> None:
    """Validate WAQI API response status."""
    if not isinstance(payload, dict):
        raise SourceFailure(f"WAQI {endpoint} returned a non-object payload.")
    status = payload.get("status")
    if status == "ok":
        return
    message = payload.get("message") or payload.get("data") or "unknown WAQI error"
    raise SourceFailure(f"WAQI {endpoint} returned status={status!r}: {message}")
