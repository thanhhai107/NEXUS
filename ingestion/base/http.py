from __future__ import annotations

import os
import random
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable

import requests

from ingestion.base.contracts import RateLimitPolicy, RetryPolicy, TimeoutPolicy
from ingestion.base.core import SourceFailure, SourceRun
from ingestion.base.utils import estimate_record_count

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

DEFAULT_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


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


def retry_policy(config: dict[str, Any]) -> RetryPolicy:
    legacy = dict(config.get("retry") or {})
    runtime = dict((config.get("resilient_runtime") or {}).get("retry_policy") or {})
    max_attempts = runtime.get("max_attempts")
    if max_attempts is None:
        max_attempts = int(legacy.get("max_retries", 5)) + 1
    retryable = runtime.get("retryable_status_codes") or legacy.get("retryable_status_codes")
    return RetryPolicy(
        max_attempts=max(1, int(max_attempts)),
        retryable_status_codes=tuple(int(code) for code in (retryable or sorted(DEFAULT_RETRYABLE_STATUS_CODES))),
        backoff_base_seconds=float(runtime.get("backoff_base_seconds", legacy.get("backoff_base_seconds", 1.0))),
        backoff_max_seconds=float(runtime.get("backoff_max_seconds", legacy.get("backoff_max_seconds", 60.0))),
        jitter_seconds=float(runtime.get("jitter_seconds", legacy.get("jitter_seconds", 0.5))),
    )


def timeout_policy(config: dict[str, Any], timeout: int | float | None = None) -> TimeoutPolicy:
    legacy = dict(config.get("retry") or {})
    runtime = dict((config.get("resilient_runtime") or {}).get("timeout_policy") or {})
    read_timeout = float(timeout or runtime.get("read_timeout_seconds") or legacy.get("timeout_seconds", 60))
    return TimeoutPolicy(
        connect_timeout_seconds=float(runtime.get("connect_timeout_seconds", min(read_timeout, 10.0))),
        read_timeout_seconds=read_timeout,
        total_timeout_seconds=(
            float(runtime["total_timeout_seconds"])
            if runtime.get("total_timeout_seconds") is not None
            else None
        ),
    )


def rate_limit_policy(config: dict[str, Any], source_key: str, delay_seconds: float | None = None) -> RateLimitPolicy:
    runtime = dict((config.get("resilient_runtime") or {}).get("rate_limit_policy") or {})
    if delay_seconds is None:
        delay_seconds = source_delay(config, source_key)
    return RateLimitPolicy(
        delay_seconds=float(delay_seconds or 0.0),
        min_delay_on_429_seconds=float(runtime.get("min_delay_on_429_seconds", 2.0)),
        max_concurrency=max(1, int(runtime.get("max_concurrency", 1))),
    )


def classify_failure(exc: Exception, status_code: int | None) -> tuple[str, bool]:
    if isinstance(exc, requests.Timeout):
        return "timeout", True
    if isinstance(exc, requests.ConnectionError):
        return "connection_error", True
    if isinstance(exc, requests.HTTPError):
        if status_code == 429:
            return "rate_limited", True
        if status_code in DEFAULT_RETRYABLE_STATUS_CODES:
            return "http_transient", True
        return "http_permanent", False
    if isinstance(exc, ValueError):
        return "payload_decode", False
    return type(exc).__name__, False


def request_json(
    run: SourceRun,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int | None = None,
    delay_seconds: float | None = None,
) -> Any:
    def call() -> tuple[Any, int, int | None]:
        response = requests.get(
            url,
            params=params or {},
            headers=headers or {},
            timeout=timeout_policy(run.context.config, timeout).requests_timeout,
        )
        status_code = response.status_code
        if status_code in retry_policy(run.context.config).retryable_status_codes:
            raise requests.HTTPError(f"HTTP {status_code}", response=response)
        response.raise_for_status()
        payload = response.json()
        return payload, estimate_record_count(payload), None

    return _request_with_retries(
        run,
        url,
        params=params,
        call=call,
        record_count_on_success=lambda payload: estimate_record_count(payload),
        timeout=timeout,
        delay_seconds=delay_seconds,
    )


def request_text(
    run: SourceRun,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int | None = None,
    delay_seconds: float | None = None,
) -> str:
    def call() -> tuple[str, int, int | None]:
        response = requests.get(
            url,
            params=params or {},
            headers=headers or {},
            timeout=timeout_policy(run.context.config, timeout).requests_timeout,
        )
        status_code = response.status_code
        if status_code in retry_policy(run.context.config).retryable_status_codes:
            raise requests.HTTPError(f"HTTP {status_code}", response=response)
        response.raise_for_status()
        return response.text, 1, len(response.content)

    return _request_with_retries(
        run,
        url,
        params=params,
        call=call,
        record_count_on_success=lambda _payload: 1,
        timeout=timeout,
        delay_seconds=delay_seconds,
    )


def _request_with_retries(
    run: SourceRun,
    url: str,
    *,
    params: dict[str, Any] | None,
    call: Callable[[], tuple[Any, int, int | None]],
    record_count_on_success: Callable[[Any], int],
    timeout: int | None,
    delay_seconds: float | None,
) -> Any:
    retry = retry_policy(run.context.config)
    masked = mask_url(url, params)
    for attempt in range(1, retry.max_attempts + 1):
        started = time.perf_counter()
        status_code: int | None = None
        try:
            payload, record_count, bytes_downloaded = call()
            duration_ms = int((time.perf_counter() - started) * 1000)
            run.log_request(
                url=masked,
                status_code=200,
                record_count=record_count_on_success(payload) if record_count is None else record_count,
                duration_ms=duration_ms,
                retry_count=attempt - 1,
                bytes_downloaded=bytes_downloaded,
                retryable=False,
            )
            sleep_after(run, delay_seconds)
            return payload
        except Exception as exc:
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
            retry_after_seconds = retry_after_delay_seconds(response)
            error_class, retryable = classify_failure(exc, status_code)
            should_retry = retryable and attempt < retry.max_attempts
            duration_ms = int((time.perf_counter() - started) * 1000)
            masked_error = mask_exception(exc, masked)
            run.log_request(
                url=masked,
                status_code=status_code,
                record_count=0,
                duration_ms=duration_ms,
                retry_count=attempt - 1,
                error=masked_error if not should_retry else f"retryable: {masked_error}",
                error_class=error_class,
                retryable=should_retry,
            )
            if not should_retry:
                raise SourceFailure(masked_error) from exc
            sleep_for_retry(run.context.config, attempt - 1, status_code, retry_after_seconds)
    raise SourceFailure(f"Request failed after {retry.max_attempts} attempts: {masked}")


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
    retry = retry_policy(run.context.config)
    masked = mask_url(url, params)
    for attempt in range(1, retry.max_attempts + 1):
        started = time.perf_counter()
        status_code: int | None = None
        bytes_downloaded = 0
        row_count = 0
        path = run._resolve_output_path(relative_path)
        tmp_path = run._staging_path(path)
        try:
            with requests.get(
                url,
                params=params or {},
                headers=headers or {},
                stream=True,
                timeout=timeout_policy(run.context.config, timeout).requests_timeout,
            ) as response:
                status_code = response.status_code
                if status_code in retry.retryable_status_codes:
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
            run._atomic_publish(tmp_path, path)
            if path.suffix.lower() == ".csv" and row_count > 0:
                row_count = max(row_count - 1, 0)
            run._record_output(path, row_count, {})
            duration_ms = int((time.perf_counter() - started) * 1000)
            run.log_request(
                url=masked,
                status_code=status_code,
                record_count=row_count,
                duration_ms=duration_ms,
                retry_count=attempt - 1,
                bytes_downloaded=bytes_downloaded,
                retryable=False,
            )
            sleep_after(run, delay_seconds)
            return path, row_count
        except Exception as exc:
            tmp_path.unlink(missing_ok=True)
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", status_code)
            retry_after_seconds = retry_after_delay_seconds(response)
            error_class, retryable = classify_failure(exc, status_code)
            should_retry = retryable and attempt < retry.max_attempts
            duration_ms = int((time.perf_counter() - started) * 1000)
            masked_error = mask_exception(exc, masked)
            run.log_request(
                url=masked,
                status_code=status_code,
                record_count=0,
                duration_ms=duration_ms,
                retry_count=attempt - 1,
                error=masked_error if not should_retry else f"retryable: {masked_error}",
                error_class=error_class,
                retryable=should_retry,
                bytes_downloaded=bytes_downloaded,
            )
            if not should_retry:
                raise SourceFailure(masked_error) from exc
            sleep_for_retry(run.context.config, attempt - 1, status_code, retry_after_seconds)
    raise SourceFailure(f"Download failed after {retry.max_attempts} attempts: {masked}")


def sleep_after(run: SourceRun, delay_seconds: float | None) -> None:
    policy = rate_limit_policy(run.context.config, run.source_key, delay_seconds)
    if policy.delay_seconds > 0:
        time.sleep(policy.delay_seconds)


def source_delay(config: dict[str, Any], source_key: str) -> float:
    rate_limits = config.get("rate_limits", {})
    return float(
        rate_limits.get(f"{source_key}_delay_seconds")
        or rate_limits.get("default_delay_seconds")
        or 0
    )


def retry_after_delay_seconds(response: Any) -> float | None:
    headers = getattr(response, "headers", None)
    if not headers:
        return None
    value = headers.get("Retry-After") if hasattr(headers, "get") else None
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        pass
    try:
        parsed = parsedate_to_datetime(str(value))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


def sleep_for_retry(
    config: dict[str, Any],
    attempt: int,
    status_code: int | None,
    retry_after_seconds: float | None = None,
) -> None:
    retry = retry_policy(config)
    rate = rate_limit_policy(config, source_key="")
    delay = min(retry.backoff_max_seconds, retry.backoff_base_seconds * (2**attempt))
    if retry.jitter_seconds > 0:
        delay += random.uniform(0, retry.jitter_seconds)
    if status_code == 429:
        delay = max(delay, rate.min_delay_on_429_seconds)
    if retry_after_seconds is not None:
        delay = max(delay, retry_after_seconds)
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
                error_class="missing_env",
                retryable=False,
            )
        raise SourceFailure(f"Missing required environment variable(s): {', '.join(missing)}")
    return values
