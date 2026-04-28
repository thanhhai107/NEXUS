from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from common.config import RUNTIME_DIR

LOCAL_RAW_DIR = RUNTIME_DIR / "raw"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_col_name(name: str) -> str:
    """Convert source column names into stable snake_case names for Bronze."""
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower())
    return normalized.strip("_")


def normalize_record(record: Mapping[str, object]) -> dict[str, object]:
    return {clean_col_name(str(key)): value for key, value in record.items()}


def raw_dataset_dir(dataset: str) -> Path:
    path = LOCAL_RAW_DIR / dataset
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_jsonl(dataset: str, records: Iterable[Mapping[str, object]], source: str) -> Path:
    """Write raw ingested records locally before they are uploaded to object storage."""
    output_dir = raw_dataset_dir(dataset)
    output_path = output_dir / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.jsonl"

    with output_path.open("w", encoding="utf-8", newline="\n") as file:
        for record in records:
            envelope = {
                "_nexus_ingested_at": utc_now_iso(),
                "_nexus_source": source,
                "payload": normalize_record(record),
            }
            file.write(json.dumps(envelope, ensure_ascii=False) + "\n")

    return output_path


def read_csv_records(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))
