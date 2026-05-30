from __future__ import annotations

import calendar
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "ingestion" / "config" / "download_defaults.yml"
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"

from common.config import BRONZE_DIR


BOROUGH_CENTROIDS: list[dict[str, Any]] = [
    {"name": "Barking and Dagenham", "latitude": 51.5607, "longitude": 0.1557},
    {"name": "Barnet", "latitude": 51.6252, "longitude": -0.1517},
    {"name": "Bexley", "latitude": 51.4549, "longitude": 0.1505},
    {"name": "Brent", "latitude": 51.5588, "longitude": -0.2817},
    {"name": "Bromley", "latitude": 51.4039, "longitude": 0.0198},
    {"name": "Camden", "latitude": 51.5290, "longitude": -0.1255},
    {"name": "City of London", "latitude": 51.5155, "longitude": -0.0922},
    {"name": "Croydon", "latitude": 51.3714, "longitude": -0.0977},
    {"name": "Ealing", "latitude": 51.5130, "longitude": -0.3089},
    {"name": "Enfield", "latitude": 51.6538, "longitude": -0.0799},
    {"name": "Greenwich", "latitude": 51.4892, "longitude": 0.0648},
    {"name": "Hackney", "latitude": 51.5450, "longitude": -0.0553},
    {"name": "Hammersmith and Fulham", "latitude": 51.4927, "longitude": -0.2339},
    {"name": "Haringey", "latitude": 51.5906, "longitude": -0.1110},
    {"name": "Harrow", "latitude": 51.5898, "longitude": -0.3346},
    {"name": "Havering", "latitude": 51.5812, "longitude": 0.1837},
    {"name": "Hillingdon", "latitude": 51.5441, "longitude": -0.4760},
    {"name": "Hounslow", "latitude": 51.4746, "longitude": -0.3680},
    {"name": "Islington", "latitude": 51.5416, "longitude": -0.1022},
    {"name": "Kensington and Chelsea", "latitude": 51.5020, "longitude": -0.1947},
    {"name": "Kingston upon Thames", "latitude": 51.4085, "longitude": -0.3064},
    {"name": "Lambeth", "latitude": 51.4607, "longitude": -0.1163},
    {"name": "Lewisham", "latitude": 51.4452, "longitude": -0.0209},
    {"name": "Merton", "latitude": 51.4014, "longitude": -0.1958},
    {"name": "Newham", "latitude": 51.5077, "longitude": 0.0469},
    {"name": "Redbridge", "latitude": 51.5590, "longitude": 0.0741},
    {"name": "Richmond upon Thames", "latitude": 51.4479, "longitude": -0.3260},
    {"name": "Southwark", "latitude": 51.5035, "longitude": -0.0804},
    {"name": "Sutton", "latitude": 51.3618, "longitude": -0.1945},
    {"name": "Tower Hamlets", "latitude": 51.5203, "longitude": -0.0293},
    {"name": "Waltham Forest", "latitude": 51.5908, "longitude": -0.0134},
    {"name": "Wandsworth", "latitude": 51.4567, "longitude": -0.1910},
    {"name": "Westminster", "latitude": 51.4973, "longitude": -0.1372},
]


def source_options(context: Any, source: str) -> dict[str, Any]:
    """Get source-specific options from global config plus mode overrides.
    
    Args:
        context: DownloadContext instance
        source: Source name (e.g., 'londonair', 'openaq')
    
    Returns:
        Dictionary of source options with mode-level overrides applied.
    """
    config_options = {}
    if hasattr(context, "config") and isinstance(context.config, dict):
        source_config = context.config.get(source, {})
        if isinstance(source_config, dict):
            config_options = dict(source_config)

    mode_options = {}
    if hasattr(context, "mode") and isinstance(context.mode, dict):
        source_mode_options = context.mode.get(f"{source}_options", {})
        if isinstance(source_mode_options, dict):
            mode_options = dict(source_mode_options)

    return {**config_options, **mode_options}


def selected_boroughs(
    config_or_context: Any,
    limit_key: str | None = None,
) -> list[dict[str, Any]]:
    """Get selected boroughs based on mode config.
    
    Args:
        config_or_context: Either a config dict or DownloadContext with .mode
        limit_key: Optional key to get borough_limit from config (default: "borough_limit")
    
    Returns:
        List of borough centroid dicts
    """
    # Support both config dict and DownloadContext
    if hasattr(config_or_context, "mode"):
        config = config_or_context.mode
    else:
        config = config_or_context
    
    key = limit_key or "borough_limit"
    mode = config.get(key, "london")
    if mode == "london":
        return BOROUGH_CENTROIDS
    if isinstance(mode, int):
        return BOROUGH_CENTROIDS[:mode]
    if isinstance(mode, list):
        return [b for b in BOROUGH_CENTROIDS if b["name"] in mode]
    return BOROUGH_CENTROIDS

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def run_id_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def earliest_timestamp(*values: str | None) -> str | None:
    timestamps = [value for value in values if value]
    return min(timestamps) if timestamps else None

def latest_timestamp(*values: str | None) -> str | None:
    timestamps = [value for value in values if value]
    return max(timestamps) if timestamps else None

def sanitize_segment(value: Any) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_.=-]+", "_", str(value).strip())
    return cleaned.strip("_") or "unknown"

def load_config(config_path: Path | None = None) -> dict[str, Any]:
    path = config_path or DEFAULT_CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"Download config not found: {path}")
    with path.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}
    return config

def resolve_mode(config: dict[str, Any], mode_name: str | None) -> tuple[str, dict[str, Any]]:
    resolved_name = mode_name or config.get("default_mode", "full_demo")
    modes = config.get("modes", {})
    if resolved_name not in modes:
        raise ValueError(f"Unknown download mode: {resolved_name}")
    return resolved_name, resolve_dynamic_config_value(dict(modes[resolved_name]))

def resolve_dynamic_config_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: resolve_dynamic_config_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_dynamic_config_value(item) for item in value]
    if isinstance(value, str):
        token = value.strip().lower()
        today = datetime.now(timezone.utc).date()
        if token in {"current_date", "today"}:
            return today.isoformat()
        if token == "current_year":
            return today.year
    return value

def resolve_output_dir(config: dict[str, Any], output_dir: Path | None) -> Path:
    if output_dir:
        return output_dir
    return BRONZE_DIR

def limit_items(items: list[Any], limit: int | None) -> list[Any]:
    if limit is None:
        return items
    return items[: max(limit, 0)]

def parse_ymd(value: str) -> date:
    return date.fromisoformat(value)

def month_ranges(start_date: str, end_date: str) -> list[tuple[date, date]]:
    start = parse_ymd(start_date)
    end = parse_ymd(end_date)
    current = date(start.year, start.month, 1)
    ranges: list[tuple[date, date]] = []
    while current <= end:
        month_end = date(current.year, current.month, calendar.monthrange(current.year, current.month)[1])
        ranges.append((max(start, current), min(end, month_end)))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return ranges

def years_between(start_year: int, end_year: int) -> list[int]:
    return list(range(int(start_year), int(end_year) + 1))

def profile_files(raw_dir: Path) -> tuple[int, float]:
    if not raw_dir.exists():
        return 0, 0.0
    files = [path for path in raw_dir.rglob("*") if path.is_file() and not path.name.endswith(".part")]
    size_bytes = sum(path.stat().st_size for path in files)
    return len(files), round(size_bytes / (1024 * 1024), 3)

def iter_timestamp_strings(value: Any) -> Iterable[str]:
    if isinstance(value, list):
        for item in value:
            yield from iter_timestamp_strings(item)
        return
    if not isinstance(value, dict):
        return
    for key, item in value.items():
        if is_timestamp_key(key) and isinstance(item, list):
            for entry in item:
                if isinstance(entry, str) and looks_like_timestamp(entry):
                    yield entry
            continue
        if is_timestamp_key(key) and isinstance(item, str) and looks_like_timestamp(item):
            yield item
            continue
        if isinstance(item, (dict, list)):
            yield from iter_timestamp_strings(item)

def is_timestamp_key(key: str) -> bool:
    normalized = key.lower()
    if normalized in {"timezone", "time_zone", "timeformat", "time_format"}:
        return False
    return any(part in normalized for part in ("time", "date", "timestamp", "datetime", "lastupdated"))

def looks_like_timestamp(value: str) -> bool:
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}(?:[T\s]\d{2}:\d{2}(?::\d{2})?)?", value))

def estimate_record_count(payload: Any) -> int:
    if isinstance(payload, list):
        return len(payload)
    if not isinstance(payload, dict):
        return 1
    hourly = payload.get("hourly")
    if isinstance(hourly, dict) and isinstance(hourly.get("time"), list):
        return len(hourly["time"])
    for key in ("results", "data", "items", "records", "features", "rows", "readings", "feed"):
        value = payload.get(key)
        if isinstance(value, list):
            return len(value)
        if isinstance(value, dict):
            count = estimate_record_count(value)
            if count:
                return count
    nested_counts = [estimate_record_count(value) for value in payload.values() if isinstance(value, (dict, list))]
    return max(nested_counts) if nested_counts else 1

def extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [record for record in payload if isinstance(record, dict)]
    if isinstance(payload, dict):
        for key in ("results", "data", "records", "items", "features", "rows", "readings", "feed"):
            value = payload.get(key)
            if isinstance(value, list):
                return [record for record in value if isinstance(record, dict)]
            if isinstance(value, dict):
                nested = extract_records(value)
                if nested:
                    return nested
        return [payload]
    return []

def iter_dicts(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from iter_dicts(item)
    elif isinstance(value, list):
        for item in value:
            yield from iter_dicts(item)

def iso_start(value: date) -> str:
    return f"{value.isoformat()}T00:00:00Z"

def iso_end(value: date) -> str:
    return f"{value.isoformat()}T23:59:59Z"

def poll_time_slug(context: Any) -> dict[str, str]:
    from ingestion.base.core import DownloadContext
    poll_time = context.poll_time if isinstance(context, DownloadContext) else datetime.now(timezone.utc)
    if hasattr(context, 'poll_time') and context.poll_time:
        poll_time = context.poll_time
    else:
        poll_time = datetime.now(timezone.utc)
    return {
        "date": poll_time.strftime("%Y-%m-%d"),
        "hour": poll_time.strftime("%H"),
        "stamp": poll_time.strftime("%H%M%S"),
    }
