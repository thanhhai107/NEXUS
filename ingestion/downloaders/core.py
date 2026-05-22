from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from ingestion.downloaders.utils import (
    earliest_timestamp,
    estimate_record_count,
    iter_timestamp_strings,
    latest_timestamp,
    now_iso,
    profile_files,
)


class SourceFailure(RuntimeError):
    """Raised when a single source cannot be downloaded safely."""


@dataclass(frozen=True)
class SourceSpec:
    key: str
    source_id: str
    description: str
    func: Callable[["SourceRun", "DownloadContext"], None]
    required_env: tuple[str, ...] = ()
    realtime: bool = False


@dataclass
class DownloadContext:
    config: dict[str, Any]
    mode_name: str
    mode: dict[str, Any]
    output_dir: Path
    run_id: str
    resume: bool = True
    overwrite: bool = False
    poll_time: datetime | None = None

    @property
    def spatial_scope(self) -> dict[str, Any]:
        return self.config.get("spatial_scope", {})

    @property
    def bbox(self) -> dict[str, float]:
        return self.spatial_scope.get("bbox", {})

class SourceRun:
    def __init__(self, source_id: str, context: DownloadContext, source_key: str) -> None:
        self.source_id = source_id
        self.source_key = source_key
        self.context = context
        self.run_id = context.run_id
        self.started_at = now_iso()
        self.base_dir = context.output_dir / source_id / f"run_id={self.run_id}"
        self.raw_dir = self.base_dir / "raw"
        self.metadata_dir = self.base_dir / "metadata"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.request_log_path = self.metadata_dir / "request_log.jsonl"
        self.checkpoint_path = self.metadata_dir / "checkpoint.json"
        self.profile_path = self.metadata_dir / "profile.json"
        self.manifest_path = self.metadata_dir / "source_manifest.json"
        self.row_count = 0
        self.failed_requests = 0
        self.first_timestamp: str | None = None
        self.last_timestamp: str | None = None
        self.previous_profile = self._load_previous_profile()
        self.checkpoint = self._load_checkpoint()
        self.initial_checkpoint_row_count = self._checkpoint_row_count()
        self.write_manifest()

    def _load_previous_profile(self) -> dict[str, Any]:
        if self.context.overwrite or not self.profile_path.exists():
            return {}
        try:
            return json.loads(self.profile_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def _load_checkpoint(self) -> dict[str, Any]:
        if not self.context.resume or self.context.overwrite or not self.checkpoint_path.exists():
            return {
                "source_id": self.source_id,
                "run_id": self.run_id,
                "completed_chunks": {},
                "failed_chunks": {},
                "last_run_at": None,
            }
        try:
            checkpoint = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            checkpoint = {}
        checkpoint.setdefault("source_id", self.source_id)
        checkpoint.setdefault("run_id", self.run_id)
        checkpoint.setdefault("completed_chunks", {})
        checkpoint.setdefault("failed_chunks", {})
        checkpoint.setdefault("last_run_at", None)
        return checkpoint

    def write_manifest(self) -> None:
        manifest = {
            "source_id": self.source_id,
            "source_key": self.source_key,
            "run_id": self.run_id,
            "mode": self.context.mode_name,
            "started_at": self.started_at,
            "spatial_scope": self.context.spatial_scope,
            "date_ranges": {
                "core_start": self.context.mode.get("core_start"),
                "core_end": self.context.mode.get("core_end"),
                "transport_start": self.context.mode.get("transport_start"),
                "transport_end": self.context.mode.get("transport_end"),
            },
        }
        self.manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    def should_skip(self, chunk_id: str) -> bool:
        return bool(
            self.context.resume
            and not self.context.overwrite
            and chunk_id in self.checkpoint.get("completed_chunks", {})
        )

    def mark_complete(self, chunk_id: str, metadata: dict[str, Any] | None = None) -> None:
        completed = self.checkpoint.setdefault("completed_chunks", {})
        completed[chunk_id] = {
            "completed_at": now_iso(),
            **(metadata or {}),
        }
        self.checkpoint.get("failed_chunks", {}).pop(chunk_id, None)
        self._write_checkpoint()
        self._write_profile("running")

    def mark_failed(self, chunk_id: str, error: str) -> None:
        failed = self.checkpoint.setdefault("failed_chunks", {})
        failed[chunk_id] = {"failed_at": now_iso(), "error": error}
        self._write_checkpoint()
        self._write_profile("partial")

    def _write_checkpoint(self) -> None:
        self.checkpoint["last_run_at"] = now_iso()
        self.checkpoint_path.write_text(json.dumps(self.checkpoint, indent=2), encoding="utf-8")

    def _checkpoint_row_count(self) -> int:
        total = 0
        for metadata in self.checkpoint.get("completed_chunks", {}).values():
            if not isinstance(metadata, dict):
                continue
            try:
                total += int(metadata.get("record_count") or 0)
            except (TypeError, ValueError):
                continue
        return total

    def log_request(
        self,
        *,
        url: str,
        status_code: int | None,
        record_count: int,
        duration_ms: int,
        retry_count: int,
        error: str | None = None,
        bytes_downloaded: int | None = None,
    ) -> None:
        event = {
            "timestamp": now_iso(),
            "source_id": self.source_id,
            "url": url,
            "status_code": status_code,
            "record_count": record_count,
            "duration_ms": duration_ms,
            "retry_count": retry_count,
        }
        if bytes_downloaded is not None:
            event["bytes_downloaded"] = bytes_downloaded
        if error:
            event["error"] = error
            self.failed_requests += 1
        elif status_code and status_code >= 400:
            self.failed_requests += 1
        with self.request_log_path.open("a", encoding="utf-8", newline="\n") as file:
            file.write(json.dumps(event, ensure_ascii=False) + "\n")

    def write_json(self, relative_path: str, payload: Any, record_count: int | None = None) -> Path:
        path = self._resolve_output_path(relative_path)
        tmp_path = path.with_suffix(path.suffix + ".part")
        try:
            tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            tmp_path.replace(path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        count = estimate_record_count(payload) if record_count is None else record_count
        self._record_output(path, count, payload)
        return path

    def write_jsonl(self, relative_path: str, records: Iterable[dict[str, Any]]) -> Path:
        path = self._resolve_output_path(relative_path)
        tmp_path = path.with_suffix(path.suffix + ".part")
        rows = list(records)
        try:
            with tmp_path.open("w", encoding="utf-8", newline="\n") as file:
                for record in rows:
                    file.write(json.dumps(record, ensure_ascii=False) + "\n")
            tmp_path.replace(path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        self._record_output(path, len(rows), rows)
        return path

    def _resolve_output_path(self, relative_path: str) -> Path:
        path = self.raw_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _record_output(self, path: Path, record_count: int, payload: Any) -> None:
        self.row_count += max(record_count, 0)
        self._update_timestamps(payload)

    def _update_timestamps(self, payload: Any) -> None:
        for timestamp in iter_timestamp_strings(payload):
            if self.first_timestamp is None or timestamp < self.first_timestamp:
                self.first_timestamp = timestamp
            if self.last_timestamp is None or timestamp > self.last_timestamp:
                self.last_timestamp = timestamp

    def _write_profile(self, status: str, error: str | None = None) -> dict[str, Any]:
        previous_row_count = 0 if self.context.overwrite else int(self.previous_profile.get("row_count") or 0)
        previous_row_count = max(previous_row_count, 0 if self.context.overwrite else self.initial_checkpoint_row_count)
        previous_failed = 0 if self.context.overwrite else int(self.previous_profile.get("failed_requests") or 0)
        first_timestamp = earliest_timestamp(self.previous_profile.get("first_timestamp"), self.first_timestamp)
        last_timestamp = latest_timestamp(self.previous_profile.get("last_timestamp"), self.last_timestamp)
        file_count, size_mb = profile_files(self.raw_dir)
        updated_at = now_iso()
        profile = {
            "source_id": self.source_id,
            "run_id": self.run_id,
            "mode": self.context.mode_name,
            "date_from": self.context.mode.get("core_start"),
            "date_to": self.context.mode.get("core_end"),
            "transport_date_from": self.context.mode.get("transport_start"),
            "transport_date_to": self.context.mode.get("transport_end"),
            "spatial_scope": self.context.spatial_scope.get("name", "Greater London"),
            "row_count": previous_row_count + self.row_count,
            "file_count": file_count,
            "size_mb": size_mb,
            "first_timestamp": first_timestamp,
            "last_timestamp": last_timestamp,
            "failed_requests": previous_failed + self.failed_requests,
            "status": status,
            "started_at": self.started_at,
            "updated_at": updated_at,
            "finished_at": None if status == "running" else updated_at,
        }
        if error:
            profile["error"] = error
        self.profile_path.write_text(json.dumps(profile, indent=2), encoding="utf-8")
        return profile

    def finish(self, status: str, error: str | None = None) -> dict[str, Any]:
        return self._write_profile(status, error)
