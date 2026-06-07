"""
TPC-DI reference table parsers — type-coerced iterators for small static sources.

Sources handled:
* ``status_type`` — 2 fields: st_id, st_name
* ``trade_type``  — 4 fields: tt_id, tt_name, tt_is_sell, tt_is_mrkt
* ``tax_rate``    — 3 fields: tx_id, tx_name, tx_rate
* ``industry``    — 3 fields: in_id, in_name, in_sector

Memory: O(1) — wraps ``iter_tpcdi_records``, yields coerced dicts on the fly.
"""

from __future__ import annotations

from typing import Any, Iterator

from common.tpcdi_io import iter_tpcdi_records


_ALLOWED_REFERENCE_SOURCES = {"status_type", "trade_type", "tax_rate", "industry"}


def parse_reference(
    source_name: str,
    batch_id: str = "batch1",
) -> Iterator[dict[str, Any]]:
    """Yield type-coerced records for a TPC-DI reference source.

    Raises ``ValueError`` if *source_name* is not a known reference source.
    """
    if source_name not in _ALLOWED_REFERENCE_SOURCES:
        raise ValueError(
            f"Unsupported reference source: '{source_name}'. "
            f"Allowed: {sorted(_ALLOWED_REFERENCE_SOURCES)}"
        )
    for rec in iter_tpcdi_records(source_name, batch_id):
        yield _coerce_reference(source_name, rec)


def _coerce_reference(source_name: str, rec: dict[str, Any]) -> dict[str, Any]:
    meta = {
        "_source_name": rec.pop("_source_name", source_name),
        "_batch_id": rec.pop("_batch_id", "batch1"),
        "_source_file": rec.pop("_source_file", ""),
        "_record_number": rec.pop("_record_number", 0),
    }
    if "_parse_error" in rec:
        rec["_parse_errors"] = [rec.pop("_parse_error")]
        rec.update(meta)
        return rec

    errors: list[str] = []

    if source_name == "status_type":
        rec.setdefault("st_id", "")
        rec.setdefault("st_name", "")

    elif source_name == "trade_type":
        rec["tt_is_sell"] = _to_int(rec.get("tt_is_sell"), "tt_is_sell", errors)
        rec["tt_is_mrkt"] = _to_int(rec.get("tt_is_mrkt"), "tt_is_mrkt", errors)

    elif source_name == "tax_rate":
        rec["tx_rate"] = _to_float(rec.get("tx_rate"), "tx_rate", errors)

    elif source_name == "industry":
        pass  # all string, no coercion needed

    if errors:
        rec["_parse_errors"] = errors
    rec.update(meta)
    return rec


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


__all__ = ["parse_reference"]
