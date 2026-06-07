"""
TPC-DI Group 3 parsers — complex formats.

* ``customer_mgmt`` — XML multi-record (Customer + Account)
* ``finwire`` — fixed-width sectioned text (CMP/SEC/FIN)
* ``customer_update`` / ``account_update`` — pipe-delimited, I/U action prefix
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Iterator

from common.tpcdi_io import iter_tpcdi_records


# ══════════════════════════════════════════════════════════════════════════════
# Customer Mgmt XML
# ══════════════════════════════════════════════════════════════════════════════

NS = {"tpcdi": "http://www.tpc.org/tpc-di"}

CUSTOMER_FIELDS = [
    "c_id", "c_tax_id", "c_gndr", "c_tier", "c_dob",
    "c_l_name", "c_f_name", "c_m_name",
    "c_adline1", "c_adline2", "c_zipcode", "c_city", "c_state_prov", "c_ctry",
    "c_prim_email", "c_alt_email",
    "c_ctry_code_1", "c_area_code_1", "c_local_1", "c_ext_1",
    "c_ctry_code_2", "c_area_code_2", "c_local_2", "c_ext_2",
]

ACCOUNT_FIELDS = [
    "ca_id", "ca_b_id", "ca_c_id", "ca_name", "ca_tax_st",
]

def parse_customer_mgmt(
    source_name: str = "customer_mgmt",
    batch_id: str = "batch1",
) -> Iterator[dict[str, Any]]:
    """Parse CustomerMgmt.xml → yields one record per action."""
    from common.tpcdi_sources import list_source_files
    files = list_source_files(source_name, batch_id)
    record_number = 0

    for filepath in files:
        tree = ET.parse(str(filepath))
        root = tree.getroot()

        for action in root.findall("tpcdi:Action", NS):
            action_type = action.get("ActionType", "")
            action_ts = action.get("ActionTS", "")

            for customer in action.findall("tpcdi:Customer", NS):
                record_number += 1
                yield _extract_customer(customer, action_type, action_ts, source_name, batch_id, str(filepath), record_number)

        # Account elements are NOT in the TPCDI namespace
        for account in tree.findall(".//Account"):
            record_number += 1
            yield _extract_account(account, "", "", source_name, batch_id, str(filepath), record_number)


def _extract_customer(elem, action_type, action_ts, source_name, batch_id, source_file, rec_num):
    rec = {
        "_source_name": source_name, "_batch_id": batch_id,
        "_source_file": source_file, "_record_number": rec_num,
        "_record_type": "customer", "_action_type": action_type, "_action_ts": action_ts,
    }

    for attr in ["C_ID", "C_TAX_ID", "C_GNDR", "C_TIER", "C_DOB"]:
        val = elem.get(attr, "")
        rec[attr.lower()] = val

    name = elem.find("Name")
    if name is not None:
        for tag in ["C_L_NAME", "C_F_NAME", "C_M_NAME"]:
            el = name.find(tag)
            rec[tag.lower()] = el.text if el is not None and el.text else ""

    addr = elem.find("Address")
    if addr is not None:
        for tag in ["C_ADLINE1", "C_ADLINE2", "C_ZIPCODE", "C_CITY", "C_STATE_PROV", "C_CTRY"]:
            el = addr.find(tag)
            rec[tag.lower()] = el.text if el is not None and el.text else ""

    ci = elem.find("ContactInfo")
    if ci is not None:
        for tag in ["C_PRIM_EMAIL", "C_ALT_EMAIL"]:
            el = ci.find(tag)
            rec[tag.lower()] = el.text if el is not None and el.text else ""
        for idx, phone_tag in enumerate(["C_PHONE_1", "C_PHONE_2"], 1):
            phone = ci.find(phone_tag)
            if phone is not None:
                for sub in ["C_CTRY_CODE", "C_AREA_CODE", "C_LOCAL", "C_EXT"]:
                    el = phone.find(sub)
                    rec[f"{sub.lower()}_{idx}"] = el.text if el is not None and el.text else ""

    rec["c_tier"] = _to_int(rec.get("c_tier"), "c_tier")
    return rec


def _extract_account(elem, action_type, action_ts, source_name, batch_id, source_file, rec_num):
    rec = {
        "_source_name": source_name, "_batch_id": batch_id,
        "_source_file": source_file, "_record_number": rec_num,
        "_record_type": "account", "_action_type": action_type, "_action_ts": action_ts,
    }
    for attr in ["CA_ID", "CA_B_ID", "CA_C_ID", "CA_NAME", "CA_TAX_ST"]:
        val = elem.get(attr, "")
        rec[attr.lower()] = val
    return rec


# ══════════════════════════════════════════════════════════════════════════════
# FINWIRE — fixed-width sectioned text
# ══════════════════════════════════════════════════════════════════════════════

def parse_finwire(
    source_name: str = "finwire",
    batch_id: str = "batch1",
) -> Iterator[dict[str, Any]]:
    """Parse FINWIRE* fixed-width files.

    Each line format (fixed-width):
      0-14: datetime (YYYYMMDD-HHMMSS)
     15-17: record type (CMP, SEC, FIN)
        18+: fixed-width body per record type

    Yields one record per line with ``_record_type``, ``_finwire_body``,
    and extracted datetime.
    """
    from common.tpcdi_sources import list_source_files
    files = list_source_files(source_name, batch_id)
    record_number = 0

    for filepath in files:
        with filepath.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n\r")
                if not line:
                    continue
                record_number += 1
                dt_part = line[:15]
                rec_type = line[15:18] if len(line) > 18 else ""
                body = line[18:] if len(line) > 18 else ""

                rec = {
                    "_source_name": source_name,
                    "_batch_id": batch_id,
                    "_source_file": str(filepath),
                    "_record_number": record_number,
                    "_record_type": rec_type,
                    "_finwire_dts": dt_part,
                    "_finwire_body": body,
                }
                yield rec


# ══════════════════════════════════════════════════════════════════════════════
# Incremental Customer Update (Customer.txt)
# ══════════════════════════════════════════════════════════════════════════════

def parse_customer_update(
    source_name: str = "customer_update",
    batch_id: str = "batch1",
) -> Iterator[dict[str, Any]]:
    """Parse Customer.txt (incremental I/U actions, no header)."""
    from common.tpcdi_sources import get_source_config
    cfg = get_source_config(source_name)
    col_names = cfg.get("columns", [])

    for rec in iter_tpcdi_records(source_name, batch_id):
        meta = {
            "_source_name": rec.pop("_source_name", source_name),
            "_batch_id": rec.pop("_batch_id", batch_id),
            "_source_file": rec.pop("_source_file", ""),
            "_record_number": rec.pop("_record_number", 0),
        }
        if "_parse_error" in rec:
            rec["_parse_errors"] = [rec.pop("_parse_error")]
            rec.update(meta)
            yield rec
            continue

        action = rec.get("action_type", "")
        for fld in ["customer_id"]:
            if fld in rec:
                rec[fld] = _to_int(rec[fld], fld)
        rec.update(meta)
        yield rec


# ══════════════════════════════════════════════════════════════════════════════
# Incremental Account Update (Account.txt)
# ══════════════════════════════════════════════════════════════════════════════

def parse_account_update(
    source_name: str = "account_update",
    batch_id: str = "batch1",
) -> Iterator[dict[str, Any]]:
    """Parse Account.txt (incremental I/U actions, no header)."""
    for rec in iter_tpcdi_records(source_name, batch_id):
        meta = {
            "_source_name": rec.pop("_source_name", source_name),
            "_batch_id": rec.pop("_batch_id", batch_id),
            "_source_file": rec.pop("_source_file", ""),
            "_record_number": rec.pop("_record_number", 0),
        }
        if "_parse_error" in rec:
            rec["_parse_errors"] = [rec.pop("_parse_error")]
            rec.update(meta)
            yield rec
            continue

        for fld in ["account_id", "customer_id", "broker_id"]:
            if fld in rec:
                rec[fld] = _to_int(rec[fld], fld)
        rec.update(meta)
        yield rec


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _to_int(value: Any, field: str) -> int | None:
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return None
