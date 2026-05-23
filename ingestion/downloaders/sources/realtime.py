from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from ingestion.downloaders.core import DownloadContext, SourceFailure, SourceRun
from ingestion.downloaders.http import request_json, require_env
from ingestion.downloaders.utils import (
    estimate_record_count,
    extract_records,
    limit_items,
    month_ranges,
    poll_time_slug,
    sanitize_segment,
    selected_boroughs,
    source_options,
)


def download_waqi_snapshot(run: SourceRun, context: DownloadContext) -> None:
    env = require_env(run, "WAQI_API_TOKEN")
    opts = source_options(context, "waqi")
    base = str(opts.get("base_url", "https://api.waqi.info")).rstrip("/")
    map_path = str(opts.get("map_path", "/v2/map/bounds"))
    bbox = context.bbox
    token = env["WAQI_API_TOKEN"]
    map_params = {
        "latlng": f"{bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']}",
        "token": token,
    }
    networks = opts.get("networks")
    if networks:
        map_params["networks"] = networks

    payload = request_json(
        run,
        f"{base}/{map_path.strip('/')}",
        params=map_params,
    )
    ensure_waqi_ok(payload, "map/bounds")
    stations = extract_records(payload)
    station_limit = context.mode.get("waqi_station_limit")
    stations = limit_items(stations, int(station_limit) if station_limit is not None else None)
    poll_time = poll_time_slug(context)
    run.write_jsonl(f"date={poll_time['date']}/hour={poll_time['hour']}/stations.jsonl", stations)

    for station in stations:
        uid = station.get("uid")
        if uid is None:
            continue
        chunk_id = f"uid={uid}:poll={poll_time['stamp']}"
        try:
            feed = request_json(run, f"{base}/feed/@{uid}/", params={"token": token})
            ensure_waqi_ok(feed, f"feed/@{uid}")
            rel = f"date={poll_time['date']}/hour={poll_time['hour']}/feed_uid={sanitize_segment(uid)}.json"
            run.write_json(rel, feed, record_count=1)
            run.mark_complete(chunk_id, {"record_count": 1})
        except Exception as exc:
            run.mark_failed(chunk_id, str(exc))


def ensure_waqi_ok(payload: Any, endpoint: str) -> None:
    if not isinstance(payload, dict):
        raise SourceFailure(f"WAQI {endpoint} returned a non-object payload.")
    status = payload.get("status")
    if status == "ok":
        return
    message = payload.get("message") or payload.get("data") or "unknown WAQI error"
    raise SourceFailure(f"WAQI {endpoint} returned status={status!r}: {message}")


def download_openweather_snapshot(run: SourceRun, context: DownloadContext) -> None:
    env = require_env(run, "OPENWEATHER_API_KEY")
    opts = source_options(context, "openweather")
    base = (os.environ.get("OPENWEATHER_API_URL") or opts.get("base_url", "https://api.openweathermap.org")).rstrip("/")
    history_base = str(opts.get("history_base_url", "https://history.openweathermap.org")).rstrip("/")
    appid = env["OPENWEATHER_API_KEY"]
    units = opts.get("units", "metric")
    lang = opts.get("lang")
    poll_time = poll_time_slug(context)
    successes = 0

    if opts.get("write_air_pollution_index_scales", True):
        run.write_json("metadata/air_pollution_index_scales.json", openweather_air_pollution_index_scales(), record_count=4)

    for borough in selected_boroughs(context, limit_key="openweather_borough_limit"):
        borough_slug = sanitize_segment(borough["name"])
        base_params = {
            "lat": borough["latitude"],
            "lon": borough["longitude"],
            "appid": appid,
        }
        localized_params = {**base_params, "units": units}
        if lang:
            localized_params["lang"] = lang

        if opts.get("include_one_call", True):
            one_call_params = dict(localized_params)
            exclude = str(opts.get("one_call_exclude") or "").strip()
            if exclude:
                one_call_params["exclude"] = exclude
            successes += download_openweather_json_chunk(
                run,
                f"{base}/data/3.0/onecall",
                one_call_params,
                chunk_id=f"openweather:onecall:{borough_slug}:poll={poll_time['stamp']}",
                relative_path=(
                    f"date={poll_time['date']}/hour={poll_time['hour']}/borough={borough_slug}"
                    "/onecall_current_forecast.json"
                ),
            )

        if opts.get("include_weather_overview", True):
            successes += download_openweather_json_chunk(
                run,
                f"{base}/data/3.0/onecall/overview",
                localized_params,
                chunk_id=f"openweather:overview:{borough_slug}:poll={poll_time['stamp']}",
                relative_path=(
                    f"date={poll_time['date']}/hour={poll_time['hour']}/borough={borough_slug}"
                    "/weather_overview.json"
                ),
            )

        if opts.get("include_day_summary", True):
            successes += download_openweather_json_chunk(
                run,
                f"{base}/data/3.0/onecall/day_summary",
                {**localized_params, "date": poll_time["date"]},
                chunk_id=f"openweather:day_summary:{borough_slug}:date={poll_time['date']}",
                relative_path=(
                    f"date={poll_time['date']}/hour={poll_time['hour']}/borough={borough_slug}"
                    "/day_summary.json"
                ),
            )

        if opts.get("include_current_weather_fallback", True):
            successes += download_openweather_json_chunk(
                run,
                f"{base}/data/2.5/weather",
                localized_params,
                chunk_id=f"openweather:current_2_5:{borough_slug}:poll={poll_time['stamp']}",
                relative_path=(
                    f"date={poll_time['date']}/hour={poll_time['hour']}/borough={borough_slug}"
                    "/weather_current_2_5.json"
                ),
            )

        if opts.get("include_air_pollution_current", True):
            successes += download_openweather_json_chunk(
                run,
                f"{base}/data/2.5/air_pollution",
                base_params,
                chunk_id=f"openweather:air_pollution_current:{borough_slug}:poll={poll_time['stamp']}",
                relative_path=(
                    f"date={poll_time['date']}/hour={poll_time['hour']}/borough={borough_slug}"
                    "/air_pollution_current.json"
                ),
            )

        if opts.get("include_air_pollution_forecast", True):
            successes += download_openweather_json_chunk(
                run,
                f"{base}/data/2.5/air_pollution/forecast",
                base_params,
                chunk_id=f"openweather:air_pollution_forecast:{borough_slug}:poll={poll_time['stamp']}",
                relative_path=(
                    f"date={poll_time['date']}/hour={poll_time['hour']}/borough={borough_slug}"
                    "/air_pollution_forecast.json"
                ),
            )

        if opts.get("include_history", False):
            chunk_days = int(opts.get("history_chunk_days", 7))
            for start, end in openweather_history_ranges(context.mode["core_start"], context.mode["core_end"], chunk_days):
                successes += download_openweather_json_chunk(
                    run,
                    f"{history_base}/data/2.5/history/city",
                    {
                        **localized_params,
                        "type": "hour",
                        "start": unix_start(start),
                        "end": unix_end(end),
                    },
                    chunk_id=f"openweather:history:{borough_slug}:{start.isoformat()}:{end.isoformat()}",
                    relative_path=(
                        f"history/borough={borough_slug}/year={start.year}/month={start.month:02d}"
                        f"/start={start.isoformat()}_end={end.isoformat()}.json"
                    ),
                )

    if successes == 0:
        raise SourceFailure("All OpenWeather requests failed.")


def download_openweather_json_chunk(
    run: SourceRun,
    url: str,
    params: dict[str, Any],
    *,
    chunk_id: str,
    relative_path: str,
) -> int:
    if run.should_skip(chunk_id):
        return 0
    try:
        payload = request_json(run, url, params=params)
        record_count = estimate_record_count(payload)
        path = run.write_json(relative_path, payload, record_count=record_count)
        run.mark_complete(chunk_id, {"record_count": record_count, "path": str(path)})
        return 1
    except Exception as exc:
        run.mark_failed(chunk_id, str(exc))
        return 0


def openweather_history_ranges(start_date: str, end_date: str, chunk_days: int) -> list[tuple[date, date]]:
    ranges: list[tuple[date, date]] = []
    max_days = max(1, min(chunk_days, 7))
    for month_start, month_end in month_ranges(start_date, end_date):
        current = month_start
        while current <= month_end:
            chunk_end = min(month_end, current + timedelta(days=max_days - 1))
            ranges.append((current, chunk_end))
            current = chunk_end + timedelta(days=1)
    return ranges


def unix_start(value: date) -> int:
    return int(datetime.combine(value, time.min, tzinfo=timezone.utc).timestamp())


def unix_end(value: date) -> int:
    return int(datetime.combine(value, time.max, tzinfo=timezone.utc).timestamp())


def openweather_air_pollution_index_scales() -> dict[str, Any]:
    return {
        "source": "OpenWeather documentation",
        "units": "ug/m3 unless noted otherwise",
        "uk_daily_air_quality_index": [
            {"index": "1", "band": "Low", "SO2": "0-88", "NO2": "0-67", "PM25": "0-11", "PM10": "0-16", "O3": "0-33"},
            {"index": "2", "band": "Low", "SO2": "89-177", "NO2": "68-134", "PM25": "12-23", "PM10": "17-33", "O3": "34-66"},
            {"index": "3", "band": "Low", "SO2": "178-266", "NO2": "135-200", "PM25": "24-35", "PM10": "34-50", "O3": "67-100"},
            {"index": "4", "band": "Moderate", "SO2": "267-354", "NO2": "201-267", "PM25": "36-41", "PM10": "52-58", "O3": "101-120"},
            {"index": "5", "band": "Moderate", "SO2": "355-443", "NO2": "268-334", "PM25": "42-47", "PM10": "59-66", "O3": "121-140"},
            {"index": "6", "band": "Moderate", "SO2": "444-532", "NO2": "335-400", "PM25": "48-53", "PM10": "67-75", "O3": "141-160"},
            {"index": "7", "band": "High", "SO2": "533-710", "NO2": "401-467", "PM25": "54-58", "PM10": "76-83", "O3": "161-187"},
            {"index": "8", "band": "High", "SO2": "711-887", "NO2": "468-534", "PM25": "59-64", "PM10": "84-91", "O3": "188-213"},
            {"index": "9", "band": "High", "SO2": "888-1064", "NO2": "535-600", "PM25": "65-70", "PM10": "92-100", "O3": "214-240"},
            {"index": "10", "band": "Very High", "SO2": ">=1065", "NO2": ">=601", "PM25": ">=71", "PM10": ">=101", "O3": ">=241"},
        ],
        "europe_hourly_index": [
            {"band": "Very Low", "index": "0-25", "NO2": "0-50", "PM10": "0-25", "O3": "0-60", "PM25": "0-15"},
            {"band": "Low", "index": "25-50", "NO2": "50-100", "PM10": "25-50", "O3": "60-120", "PM25": "15-30"},
            {"band": "Medium", "index": "50-75", "NO2": "100-200", "PM10": "50-90", "O3": "120-180", "PM25": "30-55"},
            {"band": "High", "index": "75-100", "NO2": "200-400", "PM10": "90-180", "O3": "180-240", "PM25": "55-110"},
            {"band": "Very high", "index": ">100", "NO2": ">400", "PM10": ">180", "O3": ">240", "PM25": ">110"},
        ],
        "usa_aqi_categories": [
            {"aqi": "0-50", "concern": "Good", "color": "Green"},
            {"aqi": "51-100", "concern": "Moderate", "color": "Yellow"},
            {"aqi": "101-150", "concern": "Unhealthy for sensitive groups", "color": "Orange"},
            {"aqi": "151-200", "concern": "Unhealthy", "color": "Red"},
            {"aqi": "201-300", "concern": "Very unhealthy", "color": "Purple"},
            {"aqi": "301-500", "concern": "Hazardous", "color": "Maroon"},
            {"aqi": "501-1000", "concern": "Very Hazardous", "color": "Brown"},
        ],
        "mainland_china_categories": [
            {"aqi": "0-50", "level": "Level 1", "category": "Excellent"},
            {"aqi": "51-100", "level": "Level 2", "category": "Good"},
            {"aqi": "101-150", "level": "Level 3", "category": "Lightly polluted"},
            {"aqi": "151-200", "level": "Level 4", "category": "Moderately polluted"},
            {"aqi": "201-300", "level": "Level 5", "category": "Heavily polluted"},
            {"aqi": ">300", "level": "Level 6", "category": "Severely polluted"},
        ],
    }

def download_tfl_status(run: SourceRun, context: DownloadContext) -> None:
    env = require_env(run, "TFL_API_KEY")
    opts = source_options(context, "tfl")
    base = opts.get("base_url", "https://api.tfl.gov.uk")
    modes = ",".join(opts.get("selected_modes", ["tube", "dlr", "overground", "elizabeth-line"]))
    params = tfl_auth_params(env["TFL_API_KEY"])
    poll_time = poll_time_slug(context)
    endpoints = {
        "status": f"{base}/Line/Mode/{modes}/Status",
        "routes": f"{base}/Line/Mode/{modes}/Route",
        "disruptions": f"{base}/Line/Mode/{modes}/Disruption",
    }
    successes = 0
    for name, url in endpoints.items():
        chunk_id = f"tfl:{name}:poll={poll_time['stamp']}"
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

def download_tfl_arrivals(run: SourceRun, context: DownloadContext) -> None:
    env = require_env(run, "TFL_API_KEY")
    opts = source_options(context, "tfl")
    base = opts.get("base_url", "https://api.tfl.gov.uk")
    stop_ids = opts.get("selected_stop_ids") or []
    if not stop_ids:
        raise SourceFailure("No TfL selected_stop_ids configured.")
    params = tfl_auth_params(env["TFL_API_KEY"])
    poll_time = poll_time_slug(context)
    successes = 0
    for stop_id in stop_ids:
        chunk_id = f"tfl_arrivals:stop={stop_id}:poll={poll_time['stamp']}"
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

def tfl_auth_params(api_key: str) -> dict[str, str]:
    params = {"app_key": api_key}
    app_id = os.environ.get("TFL_APP_ID")
    if app_id:
        params["app_id"] = app_id
    return params
