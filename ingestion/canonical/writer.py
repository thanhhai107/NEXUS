from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from common.config import RAW_DIR
from ingestion.canonical.envelope import EnvelopeContext, build_raw_envelope

LOCAL_RAW_DIR = RAW_DIR


def raw_dataset_dir(dataset_id: str, output_dir: Path | None = None) -> Path:
    root = output_dir or LOCAL_RAW_DIR
    path = root / dataset_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def default_raw_path(dataset_id: str, output_dir: Path | None = None, prefix: str | None = None) -> Path:
    output_dir_for_dataset = raw_dataset_dir(dataset_id, output_dir)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"{prefix}_{stamp}.jsonl" if prefix else f"{stamp}.jsonl"
    return output_dir_for_dataset / filename


def write_raw_envelopes(
    records: Iterable[Mapping[str, object]],
    context: EnvelopeContext,
    *,
    output_path: Path | None = None,
    output_dir: Path | None = None,
    normalize_payload: bool = False,
) -> Path:
    target = output_path or default_raw_path(context.dataset_id, output_dir)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_suffix(target.suffix + ".part")
    try:
        with tmp_path.open("w", encoding="utf-8", newline="\n") as file:
            for index, record in enumerate(records):
                file.write(
                    json.dumps(
                        build_raw_envelope(
                            record,
                            context,
                            record_index=index,
                            normalize_payload=normalize_payload,
                        ),
                        ensure_ascii=False,
                    )
                    + "\n"
                )
        tmp_path.replace(target)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return target
