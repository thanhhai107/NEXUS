from __future__ import annotations

import os
import random
import re
import time
from pathlib import Path
from typing import Any

import requests

from ingestion.downloaders.core import SourceFailure, SourceRun
from ingestion.downloaders.utils import estimate_record_count


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


def mask_sensitive_text(text: str) -> str:
    if not text:
        return text
    key_pattern = "|".join(re.escape(key) for key in sorted(SENSITIVE_QUERY_KEYS, key=len, reverse=True))
    return re.sub(rf"(?i)([?&]({key_pattern})=)[^&\s)]+", r"\1***", text)


def mask_exception(exc: Exception, masked_url: str) -> str:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        status_code = exc.response.status_code
        reason = exc.response.reason or "HTTP Error"
        return f"{status_code} {reason} for url: {masked_url}"

    message = str(exc) or exc.__class__.__name__
    response = getattr(exc, "response", None)
    response_url = getattr(response, "url", None)
    if response_url:
        message = message.replace(response_url, mask_url(response_url))
    return mask_sensitive_text(message)


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
            non_retryable_status = bool(
                status_code is not None
                and status_code >= 400
                and status_code not in RETRY_STATUS_CODES
            )
            is_last = non_retryable_status or attempt >= max_retries
            masked_error = mask_exception(exc, masked)
            run.log_request(
                url=masked,
                status_code=status_code,
                record_count=0,
                duration_ms=duration_ms,
                retry_count=attempt,
                error=masked_error if is_last else f"retryable: {masked_error}",
            )
            if is_last:
                raise SourceFailure(masked_error) from exc
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
            non_retryable_status = bool(
                status_code is not None
                and status_code >= 400
                and status_code not in RETRY_STATUS_CODES
            )
            is_last = non_retryable_status or attempt >= max_retries
            masked_error = mask_exception(exc, masked)
            run.log_request(
                url=masked,
                status_code=status_code,
                record_count=0,
                duration_ms=duration_ms,
                retry_count=attempt,
                error=masked_error if is_last else f"retryable: {masked_error}",
                bytes_downloaded=bytes_downloaded,
            )
            if is_last:
                raise SourceFailure(masked_error) from exc
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
