"""
TfL (Transport for London) Source Adapter.

Downloads transport status and arrivals from TfL API.
"""

from __future__ import annotations

import os
from typing import Any

from ingestion.base.core import DownloadContext, SourceFailure, SourceRun
from ingestion.base.http import request_json, require_env
from ingestion.base.utils import (
    estimate_record_count,
    extract_records,
    poll_time_slug,
    sanitize_segment,
    source_options,
)


def download_tfl(run: SourceRun, context: DownloadContext) -> None:
    """Download TfL line status and arrivals data."""
    env = require_env(run, "TFL_API_KEY")
    opts = source_options(context, "tfl")
    base = opts.get("base_url", "https://api.tfl.gov.uk")
    params = _tfl_auth_params(env["TFL_API_KEY"])
    poll_time = poll_time_slug(context)

    _download_tfl_status(run, base, params, poll_time)
    _download_tfl_arrivals(run, base, params, poll_time)


def _tfl_auth_params(api_key: str) -> dict[str, str]:
    """Build TfL API authentication params."""
    params = {"app_key": api_key}
    app_id = os.environ.get("TFL_APP_ID")
    if app_id:
        params["app_id"] = app_id
    return params


def _download_tfl_status(run: SourceRun, base: str, params: dict[str, str], poll_time: dict[str, str]) -> None:
    """Download TfL line status, routes, and disruptions."""
    opts = source_options(run.context, "tfl")
    modes = ",".join(opts.get("selected_modes", ["tube", "dlr", "overground", "elizabeth-line"]))
    endpoints = {
        "status": f"{base}/Line/Mode/{modes}/Status",
        "routes": f"{base}/Line/Mode/{modes}/Route",
        "disruptions": f"{base}/Line/Mode/{modes}/Disruption",
    }
    successes = 0

    for name, url in endpoints.items():
        chunk_id = f"tfl:{name}:poll={poll_time['stamp']}"
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


def _download_tfl_arrivals(run: SourceRun, base: str, params: dict[str, str], poll_time: dict[str, str]) -> None:
    """Download TfL stop arrivals."""
    opts = source_options(run.context, "tfl")
    stop_ids = opts.get("selected_stop_ids") or []
    if not stop_ids:
        return

    successes = 0
    for stop_id in stop_ids:
        chunk_id = f"tfl_arrivals:stop={stop_id}:poll={poll_time['stamp']}"
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
