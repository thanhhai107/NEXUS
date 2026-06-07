"""
TPC-DI Group 2 parsers — pipe-delimited and CSV sources.

Sources handled:
* ``hr``              — 9 fields, comma-delimited, employee records
* ``daily_market``    — 6 fields, pipe-delimited, daily security prices
* ``prospect``        — 22 fields, comma-delimited, prospect list
* ``trade``           — 14 fields, pipe-delimited, trade records
* ``cash_transaction``— 4 fields, pipe-delimited, cash balance txns
* ``holding_history`` — 4 fields, pipe-delimited, holdings snapshot
* ``watch_history``   — 4 fields, pipe-delimited, customer watch lists
"""

from __future__ import annotations

from datetime import date
from typing import Any, Iterator

from common.tpcdi_io import iter_tpcdi_records


# ══════════════════════════════════════════════════════════════════════════════
# HR
# ══════════════════════════════════════════════════════════════════════════════

def parse_hr(
    source_name: str = "hr",
    batch_id: str = "batch1",
) -> Iterator[dict[str, Any]]:
    for rec in iter_tpcdi_records(source_name, batch_id):
        yield _coerce_hr(rec)


def _coerce_hr(rec: dict[str, Any]) -> dict[str, Any]:
    meta = _pop_meta(rec, "hr")
    if "_parse_error" in rec:
        rec["_parse_errors"] = [rec.pop("_parse_error")]; rec.update(meta); return rec
    errors: list[str] = []
    rec["employee_id"] = _to_int(rec.get("employee_id"), "employee_id", errors)
    rec["manager_id"] = _to_int(rec.get("manager_id"), "manager_id", errors)
    rec["employee_job_code"] = _to_int(rec.get("employee_job_code"), "employee_job_code", errors)
    if errors:
        rec["_parse_errors"] = errors
    rec.update(meta)
    return rec


# ══════════════════════════════════════════════════════════════════════════════
# Daily Market
# ══════════════════════════════════════════════════════════════════════════════

def parse_daily_market(
    source_name: str = "daily_market",
    batch_id: str = "batch1",
) -> Iterator[dict[str, Any]]:
    for rec in iter_tpcdi_records(source_name, batch_id):
        yield _coerce_daily_market(rec)


def _coerce_daily_market(rec: dict[str, Any]) -> dict[str, Any]:
    meta = _pop_meta(rec, "daily_market")
    if "_parse_error" in rec:
        rec["_parse_errors"] = [rec.pop("_parse_error")]; rec.update(meta); return rec
    errors: list[str] = []
    rec["dm_date"] = _to_date(rec.get("dm_date"), "dm_date", errors)
    rec["dm_close"] = _to_float(rec.get("dm_close"), "dm_close", errors)
    rec["dm_high"] = _to_float(rec.get("dm_high"), "dm_high", errors)
    rec["dm_low"] = _to_float(rec.get("dm_low"), "dm_low", errors)
    rec["dm_volume"] = _to_int(rec.get("dm_volume"), "dm_volume", errors)
    if errors:
        rec["_parse_errors"] = errors
    rec.update(meta)
    return rec


# ══════════════════════════════════════════════════════════════════════════════
# Prospect
# ══════════════════════════════════════════════════════════════════════════════

def parse_prospect(
    source_name: str = "prospect",
    batch_id: str = "batch1",
) -> Iterator[dict[str, Any]]:
    for rec in iter_tpcdi_records(source_name, batch_id):
        yield _coerce_prospect(rec)


def _coerce_prospect(rec: dict[str, Any]) -> dict[str, Any]:
    meta = _pop_meta(rec, "prospect")
    if "_parse_error" in rec:
        rec["_parse_errors"] = [rec.pop("_parse_error")]; rec.update(meta); return rec
    errors: list[str] = []
    for field in ["income", "number_cars", "number_children", "age", "credit_rating", "number_credit_cards", "net_worth"]:
        if field in rec:
            rec[field] = _to_int(rec[field], field, errors)
    if errors:
        rec["_parse_errors"] = errors
    rec.update(meta)
    return rec


# ══════════════════════════════════════════════════════════════════════════════
# Trade
# ══════════════════════════════════════════════════════════════════════════════

def parse_trade(
    source_name: str = "trade",
    batch_id: str = "batch1",
) -> Iterator[dict[str, Any]]:
    for rec in iter_tpcdi_records(source_name, batch_id):
        yield _coerce_trade(rec)


def _coerce_trade(rec: dict[str, Any]) -> dict[str, Any]:
    meta = _pop_meta(rec, "trade")
    if "_parse_error" in rec:
        rec["_parse_errors"] = [rec.pop("_parse_error")]; rec.update(meta); return rec
    errors: list[str] = []
    rec["trade_id"] = _to_int(rec.get("trade_id"), "trade_id", errors)
    rec["is_cash"] = _to_int(rec.get("is_cash"), "is_cash", errors)
    rec["trade_quantity"] = _to_int(rec.get("trade_quantity"), "trade_quantity", errors)
    rec["trade_price"] = _to_float(rec.get("trade_price"), "trade_price", errors)
    rec["execution_id"] = _to_int(rec.get("execution_id"), "execution_id", errors)
    rec["execution_broker_id"] = _to_int(rec.get("execution_broker_id"), "execution_broker_id", errors)
    rec["cash_amount"] = _to_float(rec.get("cash_amount"), "cash_amount", errors)
    rec["fee"] = _to_float(rec.get("fee"), "fee", errors)
    rec["commission"] = _to_float(rec.get("commission"), "commission", errors)
    rec["tax"] = _to_float(rec.get("tax"), "tax", errors)
    if errors:
        rec["_parse_errors"] = errors
    rec.update(meta)
    return rec


# ══════════════════════════════════════════════════════════════════════════════
# Cash Transaction
# ══════════════════════════════════════════════════════════════════════════════

def parse_cash_transaction(
    source_name: str = "cash_transaction",
    batch_id: str = "batch1",
) -> Iterator[dict[str, Any]]:
    for rec in iter_tpcdi_records(source_name, batch_id):
        yield _coerce_cash_transaction(rec)


def _coerce_cash_transaction(rec: dict[str, Any]) -> dict[str, Any]:
    meta = _pop_meta(rec, "cash_transaction")
    if "_parse_error" in rec:
        rec["_parse_errors"] = [rec.pop("_parse_error")]; rec.update(meta); return rec
    errors: list[str] = []
    rec["ct_ca_id"] = _to_int(rec.get("ct_ca_id"), "ct_ca_id", errors)
    rec["ct_amt"] = _to_float(rec.get("ct_amt"), "ct_amt", errors)
    if errors:
        rec["_parse_errors"] = errors
    rec.update(meta)
    return rec


# ══════════════════════════════════════════════════════════════════════════════
# Holding History
# ══════════════════════════════════════════════════════════════════════════════

def parse_holding_history(
    source_name: str = "holding_history",
    batch_id: str = "batch1",
) -> Iterator[dict[str, Any]]:
    for rec in iter_tpcdi_records(source_name, batch_id):
        yield _coerce_holding_history(rec)


def _coerce_holding_history(rec: dict[str, Any]) -> dict[str, Any]:
    meta = _pop_meta(rec, "holding_history")
    if "_parse_error" in rec:
        rec["_parse_errors"] = [rec.pop("_parse_error")]; rec.update(meta); return rec
    errors: list[str] = []
    rec["hh_h_t_id"] = _to_int(rec.get("hh_h_t_id"), "hh_h_t_id", errors)
    rec["hh_t_id"] = _to_int(rec.get("hh_t_id"), "hh_t_id", errors)
    rec["hh_before_qty"] = _to_int(rec.get("hh_before_qty"), "hh_before_qty", errors)
    rec["hh_after_qty"] = _to_int(rec.get("hh_after_qty"), "hh_after_qty", errors)
    if errors:
        rec["_parse_errors"] = errors
    rec.update(meta)
    return rec


# ══════════════════════════════════════════════════════════════════════════════
# Watch History
# ══════════════════════════════════════════════════════════════════════════════

def parse_watch_history(
    source_name: str = "watch_history",
    batch_id: str = "batch1",
) -> Iterator[dict[str, Any]]:
    for rec in iter_tpcdi_records(source_name, batch_id):
        yield _coerce_watch_history(rec)


def _coerce_watch_history(rec: dict[str, Any]) -> dict[str, Any]:
    meta = _pop_meta(rec, "watch_history")
    if "_parse_error" in rec:
        rec["_parse_errors"] = [rec.pop("_parse_error")]; rec.update(meta); return rec
    errors: list[str] = []
    rec["wh_c_id"] = _to_int(rec.get("wh_c_id"), "wh_c_id", errors)
    if errors:
        rec["_parse_errors"] = errors
    rec.update(meta)
    return rec


# ══════════════════════════════════════════════════════════════════════════════
# Shared helpers
# ══════════════════════════════════════════════════════════════════════════════

def _pop_meta(rec: dict[str, Any], source_name: str) -> dict[str, Any]:
    return {
        "_source_name": rec.pop("_source_name", source_name),
        "_batch_id": rec.pop("_batch_id", "batch1"),
        "_source_file": rec.pop("_source_file", ""),
        "_record_number": rec.pop("_record_number", 0),
    }


def _to_int(value: Any, field: str, errors: list[str]) -> int | None:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        errors.append(f"{field}: cannot convert '{value}' to int")
        return None


def _to_float(value: Any, field: str, errors: list[str]) -> float | None:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        errors.append(f"{field}: cannot convert '{value}' to float")
        return None


def _to_date(value: Any, field: str, errors: list[str]) -> date | None:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    try:
        parts = str(value).strip().split("-")
        return date(int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, TypeError, IndexError):
        errors.append(f"{field}: cannot convert '{value}' to date")
        return None
