from __future__ import annotations

import argparse
import calendar
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import requests
import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "download_defaults.yml"
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"

SENSITIVE_QUERY_KEYS = {
    "token",
    "appid",
    "app_id",
    "app_key",
    "key",
    "api_key",
    "subscription-key",
    "subscription_key",
}

RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

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


class SourceFailure(RuntimeError):
    """Raised when a single source cannot be downloaded safely."""


@dataclass(frozen=True)
class SourceSpec:
    key: str
    source_id: str
    description: str
    func: Callable[["SourceRun", "DownloadContext"], None]
    required_env: tuple[str, ...] = ()
    realtime: bool = False


@dataclass
class DownloadContext:
    config: dict[str, Any]
    mode_name: str
    mode: dict[str, Any]
    output_dir: Path
    run_id: str
    resume: bool = True
    overwrite: bool = False
    poll_time: datetime | None = None

    @property
    def spatial_scope(self) -> dict[str, Any]:
        return self.config.get("spatial_scope", {})

    @property
    def bbox(self) -> dict[str, float]:
        return self.spatial_scope.get("bbox", {})


class SourceRun:
    def __init__(self, source_id: str, context: DownloadContext, source_key: str) -> None:
        self.source_id = source_id
        self.source_key = source_key
        self.context = context
        self.run_id = context.run_id
        self.started_at = now_iso()
        self.base_dir = context.output_dir / source_id / f"run_id={self.run_id}"
        self.raw_dir = self.base_dir / "raw"
        self.metadata_dir = self.base_dir / "metadata"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.request_log_path = self.metadata_dir / "request_log.jsonl"
        self.checkpoint_path = self.metadata_dir / "checkpoint.json"
        self.profile_path = self.metadata_dir / "profile.json"
        self.manifest_path = self.metadata_dir / "source_manifest.json"
        self.row_count = 0
        self.failed_requests = 0
        self.first_timestamp: str | None = None
        self.last_timestamp: str | None = None
        self.previous_profile = self._load_previous_profile()
        self.checkpoint = self._load_checkpoint()
        self.initial_checkpoint_row_count = self._checkpoint_row_count()
        self.write_manifest()

    def _load_previous_profile(self) -> dict[str, Any]:
        if self.context.overwrite or not self.profile_path.exists():
            return {}
        try:
            return json.loads(self.profile_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def _load_checkpoint(self) -> dict[str, Any]:
        if not self.context.resume or self.context.overwrite or not self.checkpoint_path.exists():
            return {
                "source_id": self.source_id,
                "run_id": self.run_id,
                "completed_chunks": {},
                "failed_chunks": {},
                "last_run_at": None,
            }
        try:
            checkpoint = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            checkpoint = {}
        checkpoint.setdefault("source_id", self.source_id)
        checkpoint.setdefault("run_id", self.run_id)
        checkpoint.setdefault("completed_chunks", {})
        checkpoint.setdefault("failed_chunks", {})
        checkpoint.setdefault("last_run_at", None)
        return checkpoint

    def write_manifest(self) -> None:
        manifest = {
            "source_id": self.source_id,
            "source_key": self.source_key,
            "run_id": self.run_id,
            "mode": self.context.mode_name,
            "started_at": self.started_at,
            "spatial_scope": self.context.spatial_scope,
            "date_ranges": {
                "core_start": self.context.mode.get("core_start"),
                "core_end": self.context.mode.get("core_end"),
                "transport_start": self.context.mode.get("transport_start"),
                "transport_end": self.context.mode.get("transport_end"),
            },
        }
        self.manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    def should_skip(self, chunk_id: str) -> bool:
        return bool(
            self.context.resume
            and not self.context.overwrite
            and chunk_id in self.checkpoint.get("completed_chunks", {})
        )

    def mark_complete(self, chunk_id: str, metadata: dict[str, Any] | None = None) -> None:
        completed = self.checkpoint.setdefault("completed_chunks", {})
        completed[chunk_id] = {
            "completed_at": now_iso(),
            **(metadata or {}),
        }
        self.checkpoint.get("failed_chunks", {}).pop(chunk_id, None)
        self._write_checkpoint()
        self._write_profile("running")

    def mark_failed(self, chunk_id: str, error: str) -> None:
        failed = self.checkpoint.setdefault("failed_chunks", {})
        failed[chunk_id] = {"failed_at": now_iso(), "error": error}
        self._write_checkpoint()
        self._write_profile("partial")

    def _write_checkpoint(self) -> None:
        self.checkpoint["last_run_at"] = now_iso()
        self.checkpoint_path.write_text(json.dumps(self.checkpoint, indent=2), encoding="utf-8")

    def _checkpoint_row_count(self) -> int:
        total = 0
        for metadata in self.checkpoint.get("completed_chunks", {}).values():
            if not isinstance(metadata, dict):
                continue
            try:
                total += int(metadata.get("record_count") or 0)
            except (TypeError, ValueError):
                continue
        return total

    def log_request(
        self,
        *,
        url: str,
        status_code: int | None,
        record_count: int,
        duration_ms: int,
        retry_count: int,
        error: str | None = None,
        bytes_downloaded: int | None = None,
    ) -> None:
        event = {
            "timestamp": now_iso(),
            "source_id": self.source_id,
            "url": url,
            "status_code": status_code,
            "record_count": record_count,
            "duration_ms": duration_ms,
            "retry_count": retry_count,
        }
        if bytes_downloaded is not None:
            event["bytes_downloaded"] = bytes_downloaded
        if error:
            event["error"] = error
            self.failed_requests += 1
        elif status_code and status_code >= 400:
            self.failed_requests += 1
        with self.request_log_path.open("a", encoding="utf-8", newline="\n") as file:
            file.write(json.dumps(event, ensure_ascii=False) + "\n")

    def write_json(self, relative_path: str, payload: Any, record_count: int | None = None) -> Path:
        path = self._resolve_output_path(relative_path)
        tmp_path = path.with_suffix(path.suffix + ".part")
        try:
            tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            tmp_path.replace(path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        count = estimate_record_count(payload) if record_count is None else record_count
        self._record_output(path, count, payload)
        return path

    def write_jsonl(self, relative_path: str, records: Iterable[dict[str, Any]]) -> Path:
        path = self._resolve_output_path(relative_path)
        tmp_path = path.with_suffix(path.suffix + ".part")
        rows = list(records)
        try:
            with tmp_path.open("w", encoding="utf-8", newline="\n") as file:
                for record in rows:
                    file.write(json.dumps(record, ensure_ascii=False) + "\n")
            tmp_path.replace(path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        self._record_output(path, len(rows), rows)
        return path

    def _resolve_output_path(self, relative_path: str) -> Path:
        path = self.raw_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _record_output(self, path: Path, record_count: int, payload: Any) -> None:
        self.row_count += max(record_count, 0)
        self._update_timestamps(payload)

    def _update_timestamps(self, payload: Any) -> None:
        for timestamp in iter_timestamp_strings(payload):
            if self.first_timestamp is None or timestamp < self.first_timestamp:
                self.first_timestamp = timestamp
            if self.last_timestamp is None or timestamp > self.last_timestamp:
                self.last_timestamp = timestamp

    def _write_profile(self, status: str, error: str | None = None) -> dict[str, Any]:
        previous_row_count = 0 if self.context.overwrite else int(self.previous_profile.get("row_count") or 0)
        previous_row_count = max(previous_row_count, 0 if self.context.overwrite else self.initial_checkpoint_row_count)
        previous_failed = 0 if self.context.overwrite else int(self.previous_profile.get("failed_requests") or 0)
        first_timestamp = earliest_timestamp(self.previous_profile.get("first_timestamp"), self.first_timestamp)
        last_timestamp = latest_timestamp(self.previous_profile.get("last_timestamp"), self.last_timestamp)
        file_count, size_mb = profile_files(self.raw_dir)
        updated_at = now_iso()
        profile = {
            "source_id": self.source_id,
            "run_id": self.run_id,
            "mode": self.context.mode_name,
            "date_from": self.context.mode.get("core_start"),
            "date_to": self.context.mode.get("core_end"),
            "transport_date_from": self.context.mode.get("transport_start"),
            "transport_date_to": self.context.mode.get("transport_end"),
            "spatial_scope": self.context.spatial_scope.get("name", "Greater London"),
            "row_count": previous_row_count + self.row_count,
            "file_count": file_count,
            "size_mb": size_mb,
            "first_timestamp": first_timestamp,
            "last_timestamp": last_timestamp,
            "failed_requests": previous_failed + self.failed_requests,
            "status": status,
            "started_at": self.started_at,
            "updated_at": updated_at,
            "finished_at": None if status == "running" else updated_at,
        }
        if error:
            profile["error"] = error
        self.profile_path.write_text(json.dumps(profile, indent=2), encoding="utf-8")
        return profile

    def finish(self, status: str, error: str | None = None) -> dict[str, Any]:
        return self._write_profile(status, error)


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
    return resolved_name, dict(modes[resolved_name])


def resolve_output_dir(config: dict[str, Any], output_dir: Path | None) -> Path:
    if output_dir:
        return output_dir
    configured = Path(str(config.get("output_dir", "runtime/downloads")))
    return configured if configured.is_absolute() else PROJECT_ROOT / configured


def find_latest_run_id(output_dir: Path, source_keys: list[str]) -> str | None:
    candidates: list[tuple[float, str]] = []
    for source in source_keys:
        spec = SOURCE_REGISTRY[normalize_source_key(source)]
        source_dir = output_dir / spec.source_id
        if not source_dir.exists():
            continue
        for run_dir in source_dir.glob("run_id=*"):
            if not run_dir.is_dir():
                continue
            metadata_dir = run_dir / "metadata"
            marker_files = [
                metadata_dir / "checkpoint.json",
                metadata_dir / "profile.json",
                metadata_dir / "source_manifest.json",
            ]
            mtimes = [path.stat().st_mtime for path in marker_files if path.exists()]
            if not mtimes:
                mtimes = [run_dir.stat().st_mtime]
            candidates.append((max(mtimes), run_dir.name.removeprefix("run_id=")))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


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


def mask_url(url: str, params: dict[str, Any] | None = None) -> str:
    prepared = requests.Request("GET", url, params=params or {}).prepare()
    full_url = prepared.url or url
    if "?" not in full_url:
        return full_url
    base, query = full_url.split("?", 1)
    masked_parts: list[str] = []
    for part in query.split("&"):
        key, sep, value = part.partition("=")
        if key.lower() in SENSITIVE_QUERY_KEYS:
            masked_parts.append(f"{key}{sep}***")
        else:
            masked_parts.append(f"{key}{sep}{value}" if sep else key)
    return f"{base}?{'&'.join(masked_parts)}"


def request_json(
    run: SourceRun,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int | None = None,
    delay_seconds: float | None = None,
) -> Any:
    retry_cfg = run.context.config.get("retry", {})
    max_retries = int(retry_cfg.get("max_retries", 5))
    timeout_seconds = int(timeout or retry_cfg.get("timeout_seconds", 60))
    masked = mask_url(url, params)
    for attempt in range(max_retries + 1):
        started = time.perf_counter()
        status_code: int | None = None
        try:
            response = requests.get(url, params=params or {}, headers=headers or {}, timeout=timeout_seconds)
            status_code = response.status_code
            if status_code in RETRY_STATUS_CODES:
                raise requests.HTTPError(f"HTTP {status_code}", response=response)
            response.raise_for_status()
            payload = response.json()
            duration_ms = int((time.perf_counter() - started) * 1000)
            run.log_request(
                url=masked,
                status_code=status_code,
                record_count=estimate_record_count(payload),
                duration_ms=duration_ms,
                retry_count=attempt,
            )
            sleep_after(run, delay_seconds)
            return payload
        except Exception as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            is_last = attempt >= max_retries
            run.log_request(
                url=masked,
                status_code=status_code,
                record_count=0,
                duration_ms=duration_ms,
                retry_count=attempt,
                error=str(exc) if is_last else f"retryable: {exc}",
            )
            if is_last:
                raise
            sleep_for_retry(run.context.config, attempt, status_code)
    raise SourceFailure(f"Request failed after {max_retries} retries: {masked}")


def download_file(
    run: SourceRun,
    url: str,
    *,
    relative_path: str,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    max_bytes: int | None = None,
    timeout: int | None = None,
    delay_seconds: float | None = None,
) -> tuple[Path, int]:
    retry_cfg = run.context.config.get("retry", {})
    max_retries = int(retry_cfg.get("max_retries", 5))
    timeout_seconds = int(timeout or retry_cfg.get("timeout_seconds", 60))
    masked = mask_url(url, params)
    for attempt in range(max_retries + 1):
        started = time.perf_counter()
        status_code: int | None = None
        bytes_downloaded = 0
        row_count = 0
        try:
            path = run._resolve_output_path(relative_path)
            tmp_path = path.with_suffix(path.suffix + ".part")
            with requests.get(
                url,
                params=params or {},
                headers=headers or {},
                stream=True,
                timeout=timeout_seconds,
            ) as response:
                status_code = response.status_code
                if status_code in RETRY_STATUS_CODES:
                    raise requests.HTTPError(f"HTTP {status_code}", response=response)
                response.raise_for_status()
                with tmp_path.open("wb") as file:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        bytes_downloaded += len(chunk)
                        if max_bytes and bytes_downloaded > max_bytes:
                            raise SourceFailure(f"Download exceeded max_bytes={max_bytes}")
                        row_count += chunk.count(b"\n")
                        file.write(chunk)
            tmp_path.replace(path)
            if path.suffix.lower() == ".csv" and row_count > 0:
                row_count = max(row_count - 1, 0)
            run.row_count += row_count
            duration_ms = int((time.perf_counter() - started) * 1000)
            run.log_request(
                url=masked,
                status_code=status_code,
                record_count=row_count,
                duration_ms=duration_ms,
                retry_count=attempt,
                bytes_downloaded=bytes_downloaded,
            )
            sleep_after(run, delay_seconds)
            return path, row_count
        except Exception as exc:
            tmp_path = locals().get("tmp_path")
            if isinstance(tmp_path, Path):
                tmp_path.unlink(missing_ok=True)
            duration_ms = int((time.perf_counter() - started) * 1000)
            is_last = attempt >= max_retries
            run.log_request(
                url=masked,
                status_code=status_code,
                record_count=0,
                duration_ms=duration_ms,
                retry_count=attempt,
                error=str(exc) if is_last else f"retryable: {exc}",
                bytes_downloaded=bytes_downloaded,
            )
            if is_last:
                raise
            sleep_for_retry(run.context.config, attempt, status_code)
    raise SourceFailure(f"Download failed after {max_retries} retries: {masked}")


def sleep_after(run: SourceRun, delay_seconds: float | None) -> None:
    if delay_seconds is None:
        delay_seconds = source_delay(run.context.config, run.source_key)
    if delay_seconds > 0:
        time.sleep(delay_seconds)


def source_delay(config: dict[str, Any], source_key: str) -> float:
    rate_limits = config.get("rate_limits", {})
    return float(
        rate_limits.get(f"{source_key}_delay_seconds")
        or rate_limits.get("default_delay_seconds")
        or 0
    )


def sleep_for_retry(config: dict[str, Any], attempt: int, status_code: int | None) -> None:
    retry_cfg = config.get("retry", {})
    base = float(retry_cfg.get("backoff_base_seconds", 1.0))
    maximum = float(retry_cfg.get("backoff_max_seconds", 60.0))
    delay = min(maximum, base * (2**attempt)) + random.uniform(0, 0.5)
    if status_code == 429:
        delay = max(delay, base * 2)
    time.sleep(delay)


def require_env(run: SourceRun, *names: str) -> dict[str, str]:
    values = {name: os.environ.get(name, "") for name in names}
    missing = [name for name, value in values.items() if not value]
    if missing:
        for name in missing:
            run.log_request(
                url=f"env://{name}",
                status_code=0,
                record_count=0,
                duration_ms=0,
                retry_count=0,
                error=f"Missing required environment variable: {name}",
            )
        raise SourceFailure(f"Missing required environment variable(s): {', '.join(missing)}")
    return values


def source_options(context: DownloadContext, key: str) -> dict[str, Any]:
    value = context.config.get(key, {})
    return value if isinstance(value, dict) else {}


def selected_boroughs(context: DownloadContext, limit_key: str = "borough_limit") -> list[dict[str, Any]]:
    limit = context.mode.get(limit_key)
    return limit_items(BOROUGH_CENTROIDS, int(limit) if limit is not None else None)


def iso_start(value: date) -> str:
    return f"{value.isoformat()}T00:00:00Z"


def iso_end(value: date) -> str:
    return f"{value.isoformat()}T23:59:59Z"


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


def download_londonair(run: SourceRun, context: DownloadContext) -> None:
    opts = source_options(context, "londonair")
    base = os.environ.get("LONDONAIR_API_BASE_URL") or opts.get("base_url")
    sites_payload = request_json(run, f"{base}/Information/MonitoringSites/GroupName=London/Json")
    run.write_json("discovery/londonair_sites.json", sites_payload)
    species_payload = request_json(run, f"{base}/Information/Species/Json")
    run.write_json("discovery/londonair_species.json", species_payload)

    site_codes = extract_londonair_site_codes(sites_payload)
    if not site_codes:
        site_codes = list(opts.get("fallback_site_codes", []))
    site_limit = context.mode.get("londonair_site_limit")
    site_codes = limit_items(site_codes, int(site_limit) if site_limit is not None else None)
    species_codes = context.mode.get("londonair_species") or ["NO2", "PM10", "PM25", "O3"]

    for site_code in site_codes:
        for species_code in species_codes:
            for start, end in month_ranges(context.mode["core_start"], context.mode["core_end"]):
                chunk_id = f"{site_code}:{species_code}:{start:%Y-%m}"
                if run.should_skip(chunk_id):
                    continue
                url = (
                    f"{base}/Data/SiteSpecies/SiteCode={site_code}"
                    f"/SpeciesCode={species_code}/StartDate={start.isoformat()}"
                    f"/EndDate={end.isoformat()}/Json"
                )
                try:
                    payload = request_json(run, url)
                    record_count = estimate_record_count(payload)
                    rel = (
                        f"site={sanitize_segment(site_code)}/species={sanitize_segment(species_code)}"
                        f"/year={start.year}/month={start.month:02d}.json"
                    )
                    run.write_json(rel, payload, record_count=record_count)
                    run.mark_complete(chunk_id, {"record_count": record_count})
                except Exception as exc:
                    run.mark_failed(chunk_id, str(exc))


def extract_londonair_site_codes(payload: Any) -> list[str]:
    codes: list[str] = []
    for record in iter_dicts(payload):
        for key in ("@SiteCode", "SiteCode", "site_code", "code"):
            value = record.get(key)
            if isinstance(value, str) and value and value not in codes:
                codes.append(value)
    return codes


def download_openaq(run: SourceRun, context: DownloadContext) -> None:
    env = require_env(run, "OPENAQ_API_KEY")
    opts = source_options(context, "openaq")
    base = opts.get("base_url", "https://api.openaq.org/v3")
    headers = {"X-API-Key": env["OPENAQ_API_KEY"]}
    page_limit = int(opts.get("page_limit", 1000))
    center = context.spatial_scope.get("center", {"latitude": 51.5074, "longitude": -0.1278})
    radius_meters = int(opts.get("radius_meters", 40000))

    locations: list[dict[str, Any]] = []
    page = 1
    while True:
        payload = request_json(
            run,
            f"{base}/locations",
            headers=headers,
            params={
                "coordinates": f"{center['latitude']},{center['longitude']}",
                "radius": radius_meters,
                "limit": page_limit,
                "page": page,
            },
        )
        records = extract_records(payload)
        if not records:
            break
        locations.extend(records)
        if len(records) < page_limit:
            break
        page += 1
    location_limit = context.mode.get("openaq_location_limit")
    locations = limit_items(locations, int(location_limit) if location_limit is not None else None)
    run.write_jsonl("discovery/locations.jsonl", locations)

    sensors = discover_openaq_sensors(run, context, base, headers, locations, page_limit)
    sensor_limit = context.mode.get("openaq_sensor_limit")
    sensors = limit_items(sensors, int(sensor_limit) if sensor_limit is not None else None)
    run.write_jsonl("discovery/sensors.jsonl", sensors)

    endpoint_name = opts.get("measurement_endpoint", "hours")
    for sensor in sensors:
        sensor_id = sensor.get("id") or sensor.get("sensor_id")
        if sensor_id is None:
            continue
        sensor_slug = sanitize_segment(sensor_id)
        for start, end in month_ranges(context.mode["core_start"], context.mode["core_end"]):
            chunk_id = f"sensor={sensor_id}:month={start:%Y-%m}"
            if run.should_skip(chunk_id):
                continue
            total = 0
            page = 1
            try:
                while True:
                    payload = request_json(
                        run,
                        f"{base}/sensors/{sensor_id}/{endpoint_name}",
                        headers=headers,
                        params={
                            "datetime_from": iso_start(start),
                            "datetime_to": iso_end(end),
                            "limit": page_limit,
                            "page": page,
                        },
                    )
                    records = extract_records(payload)
                    if records:
                        rel = (
                            f"sensor_id={sensor_slug}/year={start.year}/month={start.month:02d}"
                            f"/part-{page:04d}.jsonl"
                        )
                        run.write_jsonl(rel, records)
                    total += len(records)
                    if len(records) < page_limit:
                        break
                    page += 1
                run.mark_complete(chunk_id, {"record_count": total, "pages": page})
            except Exception as exc:
                run.mark_failed(chunk_id, str(exc))


def discover_openaq_sensors(
    run: SourceRun,
    context: DownloadContext,
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
                if isinstance(sensor, dict):
                    sensor_id = str(sensor.get("id") or sensor.get("sensor_id") or "")
                    if sensor_id and sensor_id not in seen:
                        sensor["_location_id"] = location_id
                        sensors.append(sensor)
                        seen.add(sensor_id)
            continue
        if location_id is None:
            continue
        page = 1
        while True:
            try:
                payload = request_json(
                    run,
                    f"{base}/locations/{location_id}/sensors",
                    headers=headers,
                    params={"limit": page_limit, "page": page},
                )
            except Exception:
                break
            records = extract_records(payload)
            for sensor in records:
                sensor_id = str(sensor.get("id") or sensor.get("sensor_id") or "")
                if sensor_id and sensor_id not in seen:
                    sensor["_location_id"] = location_id
                    sensors.append(sensor)
                    seen.add(sensor_id)
            if len(records) < page_limit:
                break
            page += 1
    return sensors


def download_ncei(run: SourceRun, context: DownloadContext) -> None:
    env = require_env(run, "NCEI_API_TOKEN")
    opts = source_options(context, "ncei")
    base = opts.get("base_url", "https://www.ncei.noaa.gov/cdo-web/api/v2")
    headers = {"token": env["NCEI_API_TOKEN"]}
    bbox = context.bbox
    dataset_id = opts.get("dataset_id", "GHCND")

    station_ids: list[str] = []
    try:
        payload = request_json(
            run,
            f"{base}/stations",
            headers=headers,
            params={
                "datasetid": dataset_id,
                "extent": f"{bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']}",
                "startdate": context.mode["core_start"],
                "enddate": context.mode["core_end"],
                "limit": 1000,
            },
        )
        stations = extract_records(payload)
        run.write_jsonl("discovery/stations.jsonl", stations)
        station_ids = [str(station.get("id")) for station in stations if station.get("id")]
    except Exception as exc:
        run.mark_failed("station_discovery", str(exc))

    if not station_ids:
        station_ids = list(opts.get("fallback_station_ids", []))
    station_limit = context.mode.get("ncei_station_limit")
    station_ids = limit_items(station_ids, int(station_limit) if station_limit is not None else None)
    if not station_ids:
        raise SourceFailure("No NCEI station ids discovered or configured.")

    datatypes = opts.get("datatypes", ["TMAX", "TMIN", "TAVG", "PRCP", "AWND"])
    for station_id in station_ids:
        chunk_id = f"station={station_id}:{context.mode['core_start']}:{context.mode['core_end']}"
        if run.should_skip(chunk_id):
            continue
        offset = 1
        total = 0
        try:
            while True:
                payload = request_json(
                    run,
                    f"{base}/data",
                    headers=headers,
                    params={
                        "datasetid": dataset_id,
                        "stationid": station_id,
                        "datatypeid": datatypes,
                        "startdate": context.mode["core_start"],
                        "enddate": context.mode["core_end"],
                        "limit": 1000,
                        "offset": offset,
                        "units": "metric",
                    },
                )
                records = extract_records(payload)
                if records:
                    rel = f"station={sanitize_segment(station_id)}/offset={offset:06d}.jsonl"
                    run.write_jsonl(rel, records)
                total += len(records)
                if len(records) < 1000:
                    break
                offset += 1000
            run.mark_complete(chunk_id, {"record_count": total})
        except Exception as exc:
            run.mark_failed(chunk_id, str(exc))


def download_waqi_snapshot(run: SourceRun, context: DownloadContext) -> None:
    env = require_env(run, "WAQI_API_TOKEN")
    opts = source_options(context, "waqi")
    base = opts.get("base_url", "https://api.waqi.info")
    bbox = context.bbox
    token = env["WAQI_API_TOKEN"]

    payload = request_json(
        run,
        f"{base}/map/bounds/",
        params={"latlng": f"{bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']}", "token": token},
    )
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
            rel = f"date={poll_time['date']}/hour={poll_time['hour']}/feed_uid={sanitize_segment(uid)}.json"
            run.write_json(rel, feed, record_count=1)
            run.mark_complete(chunk_id, {"record_count": 1})
        except Exception as exc:
            run.mark_failed(chunk_id, str(exc))


def download_openweather_snapshot(run: SourceRun, context: DownloadContext) -> None:
    env = require_env(run, "OPENWEATHER_API_KEY")
    opts = source_options(context, "openweather")
    base = opts.get("base_url", "https://api.openweathermap.org")
    appid = env["OPENWEATHER_API_KEY"]
    units = opts.get("units", "metric")
    poll_time = poll_time_slug(context)

    for borough in selected_boroughs(context, limit_key="openweather_borough_limit"):
        borough_slug = sanitize_segment(borough["name"])
        base_params = {
            "lat": borough["latitude"],
            "lon": borough["longitude"],
            "appid": appid,
        }
        weather = request_json(
            run,
            f"{base}/data/2.5/weather",
            params={**base_params, "units": units},
        )
        run.write_json(
            f"date={poll_time['date']}/hour={poll_time['hour']}/borough={borough_slug}/weather_current.json",
            weather,
            record_count=1,
        )
        air = request_json(run, f"{base}/data/2.5/air_pollution", params=base_params)
        run.write_json(
            f"date={poll_time['date']}/hour={poll_time['hour']}/borough={borough_slug}/air_pollution_current.json",
            air,
        )
        if opts.get("include_air_pollution_forecast", True):
            forecast = request_json(run, f"{base}/data/2.5/air_pollution/forecast", params=base_params)
            run.write_json(
                f"date={poll_time['date']}/hour={poll_time['hour']}/borough={borough_slug}/air_pollution_forecast.json",
                forecast,
            )


def download_stats19(run: SourceRun, context: DownloadContext) -> None:
    opts = source_options(context, "stats19")
    file_specs = opts.get("files", {})
    max_bytes = int(opts.get("max_bytes_per_file", 500_000_000))
    start_year = context.mode.get("transport_start_year")
    end_year = context.mode.get("transport_end_year")
    for table_name, file_spec in file_specs.items():
        env_name = file_spec.get("env")
        url = os.environ.get(env_name, "") if env_name else ""
        url = url or file_spec.get("url")
        if not url:
            raise SourceFailure(f"No STATS19 URL configured for {table_name}")
        chunk_id = f"stats19:{table_name}:{start_year}-{end_year}"
        if run.should_skip(chunk_id):
            continue
        rel = f"table={sanitize_segment(table_name)}/year_range={start_year}-{end_year}/stats19_{table_name}.csv"
        path, row_count = download_file(run, url, relative_path=rel, max_bytes=max_bytes, timeout=180)
        run.mark_complete(chunk_id, {"record_count": row_count, "path": str(path)})


def download_naptan(run: SourceRun, context: DownloadContext) -> None:
    opts = source_options(context, "naptan")
    url = os.environ.get("NAPTAN_ACCESS_NODES_CSV_URL") or opts.get("url")
    params = dict(opts.get("params", {}))
    chunk_id = "naptan:atco=490"
    if run.should_skip(chunk_id):
        return
    path, row_count = download_file(
        run,
        url,
        params=params,
        relative_path="snapshot=current/atco_area=490/naptan_stops.csv",
        max_bytes=int(opts.get("max_bytes", 200_000_000)),
        timeout=180,
    )
    run.mark_complete(chunk_id, {"record_count": row_count, "path": str(path)})


def download_london_journeys(run: SourceRun, context: DownloadContext) -> None:
    opts = source_options(context, "london_journeys")
    url = os.environ.get("LONDON_PUBLIC_TRANSPORT_JOURNEYS_URL") or opts.get("url")
    chunk_id = "london_journeys:full_csv"
    if run.should_skip(chunk_id):
        return
    path, row_count = download_file(
        run,
        url,
        relative_path="snapshot=current/london_journeys.csv",
        max_bytes=int(opts.get("max_bytes", 50_000_000)),
        timeout=180,
    )
    run.mark_complete(chunk_id, {"record_count": row_count, "path": str(path)})


def download_dft_road_traffic(run: SourceRun, context: DownloadContext) -> None:
    opts = source_options(context, "dft_road_traffic")
    base = os.environ.get("DFT_ROAD_TRAFFIC_API_BASE_URL") or opts.get("base_url")
    page_size = int(opts.get("page_size", 500))
    region_id = int(opts.get("region_id", 6))
    page_limit = context.mode.get("dft_page_limit")
    page_limit_int = int(page_limit) if page_limit is not None else None

    download_dft_paginated_entity(
        run,
        context,
        base,
        "count-points",
        {"region_id": region_id},
        "entity=count_points",
        page_size,
        page_limit_int,
    )
    for year in years_between(context.mode["transport_start_year"], context.mode["transport_end_year"]):
        download_dft_paginated_entity(
            run,
            context,
            base,
            "average-annual-daily-flow",
            {"region_id": region_id, "year": year},
            f"entity=average_annual_daily_flow/year={year}",
            page_size,
            page_limit_int,
        )


def download_dft_paginated_entity(
    run: SourceRun,
    context: DownloadContext,
    base: str,
    endpoint: str,
    filters: dict[str, Any],
    rel_prefix: str,
    page_size: int,
    page_limit: int | None,
) -> None:
    chunk_id = f"dft:{endpoint}:{json.dumps(filters, sort_keys=True)}"
    if run.should_skip(chunk_id):
        return
    next_url: str | None = f"{base.rstrip('/')}/{endpoint}"
    next_params: dict[str, Any] | None = {
        "page[size]": page_size,
        **{f"filter[{key}]": value for key, value in filters.items()},
    }
    page = 1
    total = 0
    while next_url:
        if page_limit is not None and page > page_limit:
            break
        payload = request_json(run, next_url, params=next_params)
        raw_records = extract_records(payload)
        records = raw_records
        if endpoint == "count-points":
            records = [record for record in records if is_london_dft_record(record)]
        if records:
            run.write_jsonl(f"{rel_prefix}/page={page:04d}.jsonl", records)
        total += len(records)
        next_url = next_page_url(payload)
        next_params = None
        if not next_url:
            break
        page += 1
    run.mark_complete(chunk_id, {"record_count": total, "pages": page})


def next_page_url(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("next_page_url"), str):
        return payload["next_page_url"]
    links = payload.get("links")
    if isinstance(links, dict) and isinstance(links.get("next"), str):
        return links["next"]
    return None


def is_london_dft_record(record: dict[str, Any]) -> bool:
    values = record.get("attributes") if isinstance(record.get("attributes"), dict) else record
    region_id = values.get("region_id") or values.get("regionId")
    region_name = str(values.get("region_name") or values.get("regionName") or "").lower()
    return region_id in (6, "6") or "london" in region_name


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


def poll_time_slug(context: DownloadContext) -> dict[str, str]:
    poll_time = context.poll_time or datetime.now(timezone.utc)
    return {
        "date": poll_time.strftime("%Y-%m-%d"),
        "hour": poll_time.strftime("%H"),
        "stamp": poll_time.strftime("%H%M%S"),
    }


def run_source(spec: SourceSpec, context: DownloadContext) -> dict[str, Any]:
    run = SourceRun(spec.source_id, context, spec.key)
    print(f"[{spec.key}] start source_id={spec.source_id} run_id={context.run_id}")
    try:
        spec.func(run, context)
    except Exception as exc:
        profile = run.finish("failed", str(exc))
        print(f"[{spec.key}] failed: {exc}")
        return profile
    status = "partial" if run.failed_requests else "success"
    profile = run.finish(status)
    print(
        f"[{spec.key}] {status}: rows={profile['row_count']} "
        f"files={profile['file_count']} size_mb={profile['size_mb']}"
    )
    return profile


def run_once(
    *,
    source_keys: list[str] | None = None,
    mode_name: str | None = None,
    run_id: str | None = None,
    output_dir: Path | None = None,
    config_path: Path | None = None,
    resume: bool = True,
    resume_latest: bool = False,
    overwrite: bool = False,
    poll_time: datetime | None = None,
) -> list[dict[str, Any]]:
    load_dotenv(DEFAULT_ENV_PATH, override=True)
    config = load_config(config_path)
    resolved_mode_name, mode = resolve_mode(config, mode_name)
    resolved_output_dir = resolve_output_dir(config, output_dir)
    selected = source_keys or list(config.get("sources", {}).get("default", []))
    resolved_run_id = run_id
    if not resolved_run_id and resume and resume_latest:
        resolved_run_id = find_latest_run_id(resolved_output_dir, selected)
        if resolved_run_id:
            print(f"[resume] using latest run_id={resolved_run_id}")
    resolved_run_id = resolved_run_id or run_id_now()
    context = DownloadContext(
        config=config,
        mode_name=resolved_mode_name,
        mode=mode,
        output_dir=resolved_output_dir,
        run_id=resolved_run_id,
        resume=resume,
        overwrite=overwrite,
        poll_time=poll_time,
    )
    specs = [SOURCE_REGISTRY[normalize_source_key(source)] for source in selected]
    return [run_source(spec, context) for spec in specs]


def run_polling(
    *,
    source_keys: list[str] | None = None,
    mode_name: str | None = None,
    run_id: str | None = None,
    output_dir: Path | None = None,
    config_path: Path | None = None,
    duration_days: float = 7,
    interval_minutes: float = 15,
    resume: bool = True,
    resume_latest: bool = False,
    overwrite: bool = False,
) -> list[dict[str, Any]]:
    config = load_config(config_path)
    selected = source_keys or list(config.get("sources", {}).get("polling", []))
    end_at = datetime.now(timezone.utc) + timedelta(days=duration_days)
    resolved_output_dir = resolve_output_dir(config, output_dir)
    resolved_run_id = run_id
    if not resolved_run_id and resume and resume_latest:
        resolved_run_id = find_latest_run_id(resolved_output_dir, selected)
        if resolved_run_id:
            print(f"[resume] using latest run_id={resolved_run_id}")
    resolved_run_id = resolved_run_id or run_id_now()
    all_profiles: list[dict[str, Any]] = []
    while datetime.now(timezone.utc) <= end_at:
        poll_time = datetime.now(timezone.utc)
        print(f"[poll] snapshot at {poll_time.isoformat()} sources={','.join(selected)}")
        all_profiles.extend(
            run_once(
                source_keys=selected,
                mode_name=mode_name,
                run_id=resolved_run_id,
                output_dir=output_dir,
                config_path=config_path,
                resume=resume,
                resume_latest=False,
                overwrite=overwrite,
                poll_time=poll_time,
            )
        )
        if datetime.now(timezone.utc) >= end_at:
            break
        time.sleep(max(interval_minutes, 0.01) * 60)
    return all_profiles


def normalize_source_key(source: str) -> str:
    aliases = {
        "openmeteo_air_quality": "openmeteo",
        "londonair_monitoring": "londonair",
        "openaq_measurements": "openaq",
        "ncei_cdo_climate": "ncei",
        "waqi_air_quality": "waqi",
        "openweather_current": "openweather",
        "stats19_collisions": "stats19",
        "naptan_stops": "naptan",
        "dft_road_traffic": "dft",
        "tfl": "tfl_status",
        "tfl_transport_status": "tfl_status",
    }
    key = aliases.get(source, source)
    if key not in SOURCE_REGISTRY:
        raise ValueError(f"Unknown source: {source}")
    return key


def source_choices() -> list[str]:
    return sorted(SOURCE_REGISTRY)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download Greater London NEXUS demo data locally.")
    parser.add_argument("--source", action="append", help="Source key to download. Repeat for multiple sources.")
    parser.add_argument("--list", action="store_true", help="List available sources.")
    parser.add_argument("--mode", choices=["small_demo", "full_demo"], help="Download mode. Defaults to config.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="Downloader YAML config.")
    parser.add_argument("--output-dir", type=Path, help="Output directory. Defaults to config output_dir.")
    parser.add_argument("--run-id", help="Run id. Use the same value to resume a partial run.")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting files for the same run_id.")
    parser.add_argument("--no-resume", action="store_true", help="Ignore existing checkpoint for this run_id.")
    parser.add_argument(
        "--resume-latest",
        action="store_true",
        help="When --run-id is omitted, reuse the newest existing run_id in the output directory.",
    )
    parser.add_argument("--poll", action="store_true", help="Run repeated snapshots for realtime sources.")
    parser.add_argument("--duration-days", type=float, default=7, help="Polling duration when --poll is set.")
    parser.add_argument("--interval-minutes", type=float, default=15, help="Polling interval when --poll is set.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.list:
        print("Available sources:")
        for key in source_choices():
            spec = SOURCE_REGISTRY[key]
            realtime = " realtime" if spec.realtime else ""
            print(f"  {key:18s} {spec.source_id:28s} {spec.description}{realtime}")
        return 0
    try:
        if args.poll:
            run_polling(
                source_keys=args.source,
                mode_name=args.mode,
                run_id=args.run_id,
                output_dir=args.output_dir,
                config_path=args.config,
                duration_days=args.duration_days,
                interval_minutes=args.interval_minutes,
                resume=not args.no_resume,
                resume_latest=args.resume_latest,
                overwrite=args.overwrite,
            )
        else:
            run_once(
                source_keys=args.source,
                mode_name=args.mode,
                run_id=args.run_id,
                output_dir=args.output_dir,
                config_path=args.config,
                resume=not args.no_resume,
                resume_latest=args.resume_latest,
                overwrite=args.overwrite,
            )
    except KeyboardInterrupt:
        print("Interrupted.")
        return 130
    return 0


SOURCE_REGISTRY: dict[str, SourceSpec] = {
    "openmeteo": SourceSpec(
        key="openmeteo",
        source_id="openmeteo_air_quality",
        description="Open-Meteo historical air quality and weather for borough centroids",
        func=download_openmeteo,
    ),
    "londonair": SourceSpec(
        key="londonair",
        source_id="londonair_monitoring",
        description="LondonAir hourly monitoring data by site/species/month",
        func=download_londonair,
    ),
    "openaq": SourceSpec(
        key="openaq",
        source_id="openaq_measurements",
        description="OpenAQ discovered London sensors and hourly measurements",
        func=download_openaq,
        required_env=("OPENAQ_API_KEY",),
    ),
    "ncei": SourceSpec(
        key="ncei",
        source_id="ncei_cdo_climate",
        description="NCEI daily climate data for discovered London stations",
        func=download_ncei,
        required_env=("NCEI_API_TOKEN",),
    ),
    "waqi": SourceSpec(
        key="waqi",
        source_id="waqi_air_quality",
        description="WAQI London station snapshot/feed",
        func=download_waqi_snapshot,
        required_env=("WAQI_API_TOKEN",),
        realtime=True,
    ),
    "openweather": SourceSpec(
        key="openweather",
        source_id="openweather_current",
        description="OpenWeather current weather and air-pollution snapshot",
        func=download_openweather_snapshot,
        required_env=("OPENWEATHER_API_KEY",),
        realtime=True,
    ),
    "stats19": SourceSpec(
        key="stats19",
        source_id="stats19_collisions",
        description="STATS19 collisions, vehicles, and casualties last-5-years files",
        func=download_stats19,
    ),
    "naptan": SourceSpec(
        key="naptan",
        source_id="naptan_stops",
        description="NaPTAN London ATCO 490 access nodes snapshot",
        func=download_naptan,
    ),
    "london_journeys": SourceSpec(
        key="london_journeys",
        source_id="london_journeys",
        description="London Datastore public transport journeys CSV",
        func=download_london_journeys,
    ),
    "dft": SourceSpec(
        key="dft",
        source_id="dft_road_traffic",
        description="DfT road traffic London count points and traffic counts",
        func=download_dft_road_traffic,
    ),
    "tfl_status": SourceSpec(
        key="tfl_status",
        source_id="tfl_transport_status",
        description="TfL line status, routes, and disruptions snapshot",
        func=download_tfl_status,
        required_env=("TFL_API_KEY",),
        realtime=True,
    ),
    "tfl_arrivals": SourceSpec(
        key="tfl_arrivals",
        source_id="tfl_arrivals",
        description="TfL arrivals snapshot for selected stops",
        func=download_tfl_arrivals,
        required_env=("TFL_API_KEY",),
        realtime=True,
    ),
}


if __name__ == "__main__":
    sys.exit(main())
