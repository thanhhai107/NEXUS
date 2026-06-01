"""
TfL (Transport for London) Source Adapter.

Downloads transport status and arrivals from TfL API.
"""

from __future__ import annotations

import os
from typing import Any

from ingestion.base.core import DownloadContext, SourceFailure, SourceRun
from ingestion.base.http import request_json
from ingestion.base.utils import (
    estimate_record_count,
    extract_records,
    poll_time_slug,
    sanitize_segment,
    source_options,
)


def download_tfl(run: SourceRun, context: DownloadContext) -> None:
    """Download the combined TfL realtime snapshot."""
    base, params, poll_time = _tfl_request_context(run, context, "tfl")

    _download_tfl_status(run, base, params, poll_time, "tfl")
    _download_tfl_arrivals(run, base, params, poll_time, "tfl")


def download_tfl_line_status(run: SourceRun, context: DownloadContext) -> None:
    """Download TfL line status, routes, and disruptions only."""
    base, params, poll_time = _tfl_request_context(run, context, "tfl_line_status")

    _download_tfl_status(run, base, params, poll_time, "tfl_line_status")


def download_tfl_arrivals(run: SourceRun, context: DownloadContext) -> None:
    """Download TfL stop arrivals only."""
    base, params, poll_time = _tfl_request_context(run, context, "tfl_arrivals")

    _download_tfl_arrivals(run, base, params, poll_time, "tfl_arrivals")


def _tfl_request_context(
    run: SourceRun,
    context: DownloadContext,
    option_source: str,
) -> tuple[str, dict[str, str], dict[str, str]]:
    opts = _tfl_options(context, option_source)
    base = str(opts.get("base_url", "https://api.tfl.gov.uk")).rstrip("/")
    params = _tfl_auth_params(opts)
    poll_time = poll_time_slug(context)
    return base, params, poll_time


def _tfl_options(context: DownloadContext, option_source: str) -> dict[str, Any]:
    base_options = source_options(context, "tfl")
    if option_source == "tfl":
        return base_options
    return {**base_options, **source_options(context, option_source)}


def _tfl_auth_params(opts: dict[str, Any]) -> dict[str, str]:
    """Build optional TfL API authentication params."""
    api_key_env = str(opts.get("api_key_env") or "TFL_API_KEY")
    api_key = os.environ.get(api_key_env) or os.environ.get("TFL_API_KEY")
    params: dict[str, str] = {}
    if api_key:
        params["app_key"] = api_key
    app_id = os.environ.get("TFL_APP_ID")
    if app_id:
        params["app_id"] = app_id
    return params


def _tfl_line_endpoint_base(base: str, opts: dict[str, Any]) -> str:
    line_ids = [
        str(line_id).strip()
        for line_id in (opts.get("selected_line_ids") or [])
        if str(line_id).strip()
    ]
    if line_ids:
        return f"{base}/Line/{','.join(line_ids)}"

    default_modes = os.getenv("TFL_DEFAULT_MODES", "tube,dlr,overground,elizabeth-line").split(",")
    modes = ",".join(
        str(mode).strip()
        for mode in opts.get("selected_modes", default_modes)
        if str(mode).strip()
    )
    return f"{base}/Line/Mode/{modes}"


def _download_tfl_status(
    run: SourceRun,
    base: str,
    params: dict[str, str],
    poll_time: dict[str, str],
    option_source: str,
) -> None:
    """Download TfL line status, routes, and disruptions."""
    opts = _tfl_options(run.context, option_source)
    line_base = _tfl_line_endpoint_base(base, opts)
    endpoints = {
        "status": f"{line_base}/Status",
        "routes": f"{line_base}/Route",
        "disruptions": f"{line_base}/Disruption",
    }
    successes = 0

    for name, url in endpoints.items():
        chunk_id = f"{option_source}:{name}:poll={poll_time['stamp']}"
        if run.should_skip(chunk_id):
            successes += 1
            continue
        try:
            payload = request_json(run, url, params=params)
            rel = f"date={poll_time['date']}/hour={poll_time['hour']}/{name}_{poll_time['stamp']}.json"
            run.write_json(rel, payload)
            run.mark_complete(chunk_id, {"record_count": estimate_record_count(payload)})
            successes += 1
        except Exception as exc:
            run.mark_failed(chunk_id, str(exc))

    if successes == 0:
        raise SourceFailure("All TfL status/route/disruption requests failed.")


def _download_tfl_arrivals(
    run: SourceRun,
    base: str,
    params: dict[str, str],
    poll_time: dict[str, str],
    option_source: str,
) -> None:
    """Download TfL stop arrivals."""
    opts = _tfl_options(run.context, option_source)
    stop_ids = opts.get("selected_stop_ids") or []
    if not stop_ids:
        return

    successes = 0
    for stop_id in stop_ids:
        chunk_id = f"{option_source}:arrivals:stop={stop_id}:poll={poll_time['stamp']}"
        if run.should_skip(chunk_id):
            successes += 1
            continue
        try:
            payload = request_json(run, f"{base}/StopPoint/{stop_id}/Arrivals", params=params)
            records = extract_records(payload)
            rel = (
                f"date={poll_time['date']}/hour={poll_time['hour']}"
                f"/stop_id={sanitize_segment(stop_id)}/arrivals_{poll_time['stamp']}.jsonl"
            )
            run.write_jsonl(rel, records)
            run.mark_complete(chunk_id, {"record_count": len(records)})
            successes += 1
        except Exception as exc:
            run.mark_failed(chunk_id, str(exc))

    if successes == 0:
        raise SourceFailure("All TfL arrivals requests failed.")
