from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common.config import RUNTIME_DIR, SILVER_DIR
from ingestion.canonical.envelope import EnvelopeContext, build_raw_envelope
from ingestion.canonical.parser import iter_artifact_records, resolve_artifact_path


def published_run_to_raw_envelope(
    published_manifest_path: Path,
    *,
    output_dir: Path | None = None,
    actor: str = "downloader",
) -> dict[str, Any]:
    """Convert a published downloader run into canonical raw envelopes.

    This adapter intentionally does not perform schema validation, quarantine
    routing, governance decisions or Bronze loading. Those are post-envelope
    stages shared by batch, download and streaming ingestion paths.
    """

    manifest = _read_json(published_manifest_path)
    source_id = str(manifest["source_id"])
    dataset_id = str(manifest.get("dataset_id") or manifest.get("dataset_name") or source_id)
    source_key = manifest.get("source_key")
    run_id = str(manifest["run_id"])
    published_at = str(manifest.get("published_at") or "")
    output_root = output_dir or SILVER_DIR
    output_path = output_root / dataset_id / f"{source_id}_{run_id}.jsonl"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".part")

    parser_failures: list[dict[str, str]] = []
    written_count = 0

    try:
        with tmp_path.open("w", encoding="utf-8", newline="\n") as output:
            for chunk in manifest.get("chunks", []):
                chunk_id = str(chunk.get("chunk_id") or "unknown")
                for source_path in chunk.get("paths") or []:
                    artifact_path = resolve_artifact_path(source_path)
                    context = EnvelopeContext(
                        dataset_id=dataset_id,
                        source_id=source_id,
                        source_key=str(source_key) if source_key else None,
                        ingestion_type="download",
                        run_id=run_id,
                        chunk_id=chunk_id,
                        source_path=artifact_path,
                        published_at=published_at or None,
                    )
                    try:
                        for record_index, record in enumerate(iter_artifact_records(artifact_path)):
                            output.write(
                                json.dumps(
                                    build_raw_envelope(record, context, record_index=record_index),
                                    ensure_ascii=False,
                                )
                                + "\n"
                            )
                            written_count += 1
                    except Exception as exc:  # noqa: BLE001 - report, do not route governance here
                        parser_failures.append(
                            {
                                "path": str(artifact_path),
                                "chunk_id": chunk_id,
                                "error_type": type(exc).__name__,
                                "error": str(exc),
                            }
                        )
        tmp_path.replace(output_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    _stamp_downstream_path(published_manifest_path, output_path)
    return {
        "dataset_id": dataset_id,
        "dataset": dataset_id,
        "source_id": source_id,
        "run_id": run_id,
        "raw_path": str(output_path),
        "valid_records": written_count,
        "record_count": written_count,
        "parser_failures": len(parser_failures),
        "parser_failure_details": parser_failures,
        "actor": actor,
    }


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected object JSON manifest at {path}")
    return payload


def _stamp_downstream_path(published_manifest_path: Path, raw_path: Path) -> None:
    manifest = _read_json(published_manifest_path)
    manifest["downstream_raw_path"] = str(raw_path)
    manifest["raw_envelope_published_at"] = datetime.now(timezone.utc).isoformat()
    published_manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    metadata_manifest = published_manifest_path.parents[1] / "metadata" / "published_manifest.json"
    if metadata_manifest.exists():
        metadata_manifest.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    run_manifest_path = published_manifest_path.parents[1] / "metadata" / "run_manifest.json"
    if run_manifest_path.exists():
        run_manifest_payload = _read_json(run_manifest_path)
        run_manifest_payload["downstream_raw_path"] = str(raw_path)
        run_manifest_path.write_text(
            json.dumps(run_manifest_payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
