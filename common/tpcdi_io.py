"""
TPC-DI DIGen data I/O — streaming record reader for DIGen-generated source files.

Provides memory-efficient iterators (record-level and chunk-level) that
respect the source configuration from ``tpcdi_sources.yml`` (delimiter,
columns, has_header, skip_blank_lines, etc.).

Usage::

    from common.tpcdi_io import iter_tpcdi_records

    for record in iter_tpcdi_records("trade", "batch1"):
        print(record["trade_id"])
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterator

from common.tpcdi_sources import get_source_config, list_source_files


SUPPORTED_FORMATS = {"delimited", "csv"}


def _get_skip_blank(source_name: str) -> bool:
    cfg = get_source_config(source_name)
    return cfg.get("skip_blank_lines", True)


def iter_tpcdi_records(
    source_name: str,
    batch_id: str,
) -> Iterator[dict[str, Any]]:
    """Yield records from a DIGen source file, one at a time.

    Memory: O(1).

    Raises ``NotImplementedError`` for XML or sectioned formats — use
    dedicated parsers from ``ingestion/tpcdi/parsers/`` instead.
    """
    cfg = get_source_config(source_name)
    fmt = cfg.get("format", "delimited")
    if fmt not in SUPPORTED_FORMATS:
        raise NotImplementedError(
            f"Cannot iter_tpcdi_records('{source_name}'): format='{fmt}' is not "
            f"supported. Use a dedicated parser from ingestion/tpcdi/parsers/."
        )

    delimiter = cfg.get("delimiter", ",")
    has_header = cfg.get("has_header", True)
    columns = cfg.get("columns", [])
    skip_blank = _get_skip_blank(source_name)

    for filepath in list_source_files(source_name, batch_id):
        record_number = 0
        # errors='replace' prevents UnicodeDecodeError on binary/non-UTF-8 content
        # (e.g. poison_record injection).  Replacement chars cause field-count or
        # coercion failures which the bronze validator catches as parse errors.
        with filepath.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
            if has_header:
                reader = csv.DictReader(f, delimiter=delimiter)
                for row in reader:
                    record_number += 1
                    if skip_blank and _is_blank_row(row):
                        continue
                    row["_source_name"] = source_name
                    row["_batch_id"] = batch_id
                    row["_source_file"] = str(filepath)
                    row["_record_number"] = record_number
                    yield row
            else:
                reader = csv.reader(f, delimiter=delimiter)
                for raw_row in reader:
                    record_number += 1
                    if skip_blank and all(col.strip() == "" for col in raw_row):
                        continue
                    if len(raw_row) != len(columns):
                        row = {
                            "_parse_error": "field_count_mismatch",
                            "_expected_fields": len(columns),
                            "_actual_fields": len(raw_row),
                            "_raw": delimiter.join(raw_row),
                            "_source_name": source_name,
                            "_batch_id": batch_id,
                            "_source_file": str(filepath),
                            "_record_number": record_number,
                        }
                        yield row
                        continue
                    row = dict(zip(columns, raw_row))
                    row["_source_name"] = source_name
                    row["_batch_id"] = batch_id
                    row["_source_file"] = str(filepath)
                    row["_record_number"] = record_number
                    yield row


def _is_blank_row(row: dict[str, Any]) -> bool:
    return all(v is None or (isinstance(v, str) and v.strip() == "") for v in row.values())


def iter_tpcdi_chunks(
    source_name: str,
    batch_id: str,
    chunk_size: int = 10000,
) -> Iterator[list[dict[str, Any]]]:
    chunk: list[dict[str, Any]] = []
    for record in iter_tpcdi_records(source_name, batch_id):
        chunk.append(record)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def count_tpcdi_records(source_name: str, batch_id: str) -> int:
    cfg = get_source_config(source_name)
    skip_blank = _get_skip_blank(source_name)
    total = 0
    for filepath in list_source_files(source_name, batch_id):
        with filepath.open("r", encoding="utf-8-sig", newline="") as f:
            for line in f:
                if skip_blank and line.strip() == "":
                    continue
                total += 1
    if cfg.get("has_header", True):
        total -= len(list_source_files(source_name, batch_id))
    return max(total, 0)


def read_tpcdi_dataframe(
    source_name: str,
    batch_id: str,
    spark_session: Any = None,
    allow_collect: bool = False,
) -> Any:
    """Return a DataFrame for the given source + batch.

    * Requires ``spark_session`` for large TPC-DI sources.
    * Falls back to ``list[dict]`` only when ``allow_collect=True`` (small data).
    """
    cfg = get_source_config(source_name)
    fmt = cfg.get("format", "delimited")
    if fmt not in SUPPORTED_FORMATS:
        raise NotImplementedError(
            f"read_tpcdi_dataframe('{source_name}'): format='{fmt}' not supported."
        )

    delimiter = cfg.get("delimiter", ",")
    files = list_source_files(source_name, batch_id)
    paths = [str(p) for p in files]

    if spark_session is not None:
        reader = spark_session.read.option("delimiter", delimiter).option("header", str(cfg.get("has_header", True)).lower())
        if cfg.get("has_header") is False:
            reader = reader.option("header", "false")
            col_names = cfg.get("columns", [])
            if col_names:
                reader = reader.option("columnNameOfCorruptRecord", "_corrupt")
                reader = reader.schema(",".join(f"{c} string" for c in col_names))
        return reader.csv(paths)

    if allow_collect:
        return list(iter_tpcdi_records(source_name, batch_id))

    raise RuntimeError(
        "read_tpcdi_dataframe requires spark_session for large TPC-DI sources. "
        "Use iter_tpcdi_records / iter_tpcdi_chunks for streaming."
    )
