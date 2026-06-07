"""
TPC-DI date/time dimension parsers — type-coerced iterators.

Sources handled:
* ``date`` — 18 fields, surrogate key + calendar attributes
* ``time`` — 10 fields, surrogate key + time-of-day attributes
"""

from __future__ import annotations

from datetime import date
from typing import Any, Iterator

from common.tpcdi_io import iter_tpcdi_records


def parse_date_dim(
    source_name: str = "date",
    batch_id: str = "batch1",
) -> Iterator[dict[str, Any]]:
    """Yield type-coerced Date dimension records.

    Raises ``ValueError`` if *source_name* is not ``"date"``.
    """
    if source_name != "date":
        raise ValueError(
            f"parse_date_dim only supports source_name='date', got '{source_name}'"
        )
    for rec in iter_tpcdi_records(source_name, batch_id):
        yield _coerce_date(rec)


def parse_time_dim(
    source_name: str = "time",
    batch_id: str = "batch1",
) -> Iterator[dict[str, Any]]:
    """Yield type-coerced Time dimension records.

    Raises ``ValueError`` if *source_name* is not ``"time"``.
    """
    if source_name != "time":
        raise ValueError(
            f"parse_time_dim only supports source_name='time', got '{source_name}'"
        )
    for rec in iter_tpcdi_records(source_name, batch_id):
        yield _coerce_time(rec)


# ── Date coercion ────────────────────────────────────────────────────────────

_DATE_FIELDS_INT = [
    "sk_date",
    "year",
    "month_num",
    "month_of_year",
    "quarter",
    "month_of_year_2",
    "day_of_week_num",
    "fiscal_year",
    "fiscal_quarter",
    "fiscal_year_quarter",
]
_DATE_FIELDS_STRING = [
    "date_desc",
    "quarter_name",
    "month_name",
    "week_of_year",
    "day_of_week_name",
    "fiscal_period",
]
_DATE_FIELDS_BOOL = ["holiday_flag"]


def _coerce_date(rec: dict[str, Any]) -> dict[str, Any]:
    meta = {
        "_source_name": rec.pop("_source_name", "date"),
        "_batch_id": rec.pop("_batch_id", "batch1"),
        "_source_file": rec.pop("_source_file", ""),
        "_record_number": rec.pop("_record_number", 0),
    }
    if "_parse_error" in rec:
        rec["_parse_errors"] = [rec.pop("_parse_error")]
        rec.update(meta)
        return rec

    errors: list[str] = []

    for field in _DATE_FIELDS_INT:
        if field in rec:
            rec[field] = _to_int(rec[field], field, errors)

    if "date_value" in rec:
        rec["date_value"] = _to_date(rec["date_value"], "date_value", errors)

    if "holiday_flag" in rec:
        rec["holiday_flag"] = _to_bool(rec["holiday_flag"], "holiday_flag", errors)

    rec["sk_date"] = rec.get("sk_date")
    if rec["sk_date"] is None and "sk_date" in rec:
        errors.append("sk_date is required")

    if errors:
        rec["_parse_errors"] = errors
    rec.update(meta)
    return rec


# ── Time coercion ────────────────────────────────────────────────────────────

_TIME_FIELDS_INT = ["sk_time", "hour", "minute", "second", "minute_of_day"]
_TIME_FIELDS_BOOL = ["is_work_hour", "is_night_hour"]


def _coerce_time(rec: dict[str, Any]) -> dict[str, Any]:
    meta = {
        "_source_name": rec.pop("_source_name", "time"),
        "_batch_id": rec.pop("_batch_id", "batch1"),
        "_source_file": rec.pop("_source_file", ""),
        "_record_number": rec.pop("_record_number", 0),
    }
    if "_parse_error" in rec:
        rec["_parse_errors"] = [rec.pop("_parse_error")]
        rec.update(meta)
        return rec

    errors: list[str] = []

    for field in _TIME_FIELDS_INT:
        if field in rec:
            rec[field] = _to_int(rec[field], field, errors)

    for field in _TIME_FIELDS_BOOL:
        if field in rec:
            rec[field] = _to_bool(rec[field], field, errors)

    rec["sk_time"] = rec.get("sk_time")
    if rec["sk_time"] is None:
        errors.append("sk_time is required")

    if errors:
        rec["_parse_errors"] = errors
    rec.update(meta)
    return rec


# ── Type coercion helpers ────────────────────────────────────────────────────

def _to_int(value: Any, field: str, errors: list[str]) -> int | None:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        errors.append(f"{field}: cannot convert '{value}' to int")
        return None


def _to_date(value: Any, field: str, errors: list[str]) -> date | None:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    try:
        raw = str(value).strip()
        parts = raw.split("-")
        return date(int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, TypeError, IndexError):
        errors.append(f"{field}: cannot convert '{value}' to date")
        return None


def _to_bool(value: Any, field: str, errors: list[str]) -> bool | None:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    raw = str(value).strip().lower()
    if raw in ("true", "1", "t", "yes"):
        return True
    if raw in ("false", "0", "f", "no"):
        return False
    errors.append(f"{field}: cannot convert '{value}' to bool")
    return None


__all__ = ["parse_date_dim", "parse_time_dim"]
