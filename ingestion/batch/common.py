from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Mapping

from common.config import RUNTIME_DIR
from ingestion.canonical.envelope import EnvelopeContext, clean_field_name, normalize_record
from ingestion.canonical.writer import default_raw_path, write_raw_envelopes

LOCAL_RAW_DIR = RUNTIME_DIR / "raw"


def clean_col_name(name: str) -> str:
    """Backward-compatible alias for canonical field normalization."""

    return clean_field_name(name)


def raw_dataset_dir(dataset: str) -> Path:
    path = LOCAL_RAW_DIR / dataset
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_jsonl(
    dataset: str,
    records: Iterable[Mapping[str, object]],
    source: str,
    *,
    ingestion_type: str = "batch",
    source_key: str | None = None,
    run_id: str | None = None,
    chunk_id: str | None = None,
) -> Path:
    """Write records into the canonical raw envelope contract.

    The function preserves the existing batch/streaming call shape while moving
    envelope creation into ``ingestion.canonical``.
    """

    context = EnvelopeContext(
        dataset_id=dataset,
        source_id=source,
        source_key=source_key,
        ingestion_type=ingestion_type,
        run_id=run_id,
        chunk_id=chunk_id,
        source_path=source,
    )
    return write_raw_envelopes(
        records,
        context,
        output_path=default_raw_path(dataset, LOCAL_RAW_DIR),
        normalize_payload=True,
    )


def read_csv_records(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        return list(csv.DictReader(file))
