from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from ingestion.base.contracts import (
    CHUNK_FAILED,
    CHUNK_SKIPPED,
    CHUNK_SUCCESS,
    COVERAGE_COMPLETE,
    COVERAGE_FAILED,
    COVERAGE_PARTIAL,
    PUBLISH_PUBLISHED,
    PUBLISH_UNPUBLISHED,
    PUBLISH_WITH_WARNING,
    ChunkResult,
    CoveragePolicy,
    DownloadChunk,
    DownloadPlan,
    PublishedManifest,
    RunManifest,
    SourceSpec,
)
from ingestion.base.utils import (
    earliest_timestamp,
    estimate_record_count,
    iter_timestamp_strings,
    latest_timestamp,
    now_iso,
    profile_files,
)


class SourceFailure(RuntimeError):
    """Raised when a single source cannot be downloaded safely."""


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
    """Runtime state for one source run.

    The public methods stay backward-compatible with existing source adapters:
    ``should_skip``, ``write_json``, ``write_jsonl``, ``mark_complete`` and
    ``mark_failed``. Internally they now maintain resilient runtime artifacts:
    checkpoint, chunk output index, run manifest and published manifest.
    """

    def __init__(
        self,
        source_id: str,
        context: DownloadContext,
        source_key: str,
        dataset_name: str | None = None,
    ) -> None:
        self.source_id = source_id
        self.source_key = source_key
        self.dataset_name = dataset_name or source_id
        self.context = context
        self.run_id = context.run_id
        self.started_at = now_iso()
        self.base_dir = context.output_dir / source_id / f"run_id={self.run_id}"
        self.raw_dir = self.base_dir / "raw"
        self.staging_dir = self.base_dir / "staging"
        self.published_dir = self.base_dir / "published"
        self.metadata_dir = self.base_dir / "metadata"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.published_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.request_log_path = self.metadata_dir / "request_log.jsonl"
        self.checkpoint_path = self.metadata_dir / "checkpoint.json"
        self.profile_path = self.metadata_dir / "profile.json"
        self.manifest_path = self.metadata_dir / "source_manifest.json"
        self.run_manifest_path = self.metadata_dir / "run_manifest.json"
        self.published_manifest_path = self.published_dir / "published_manifest.json"
        self.metadata_published_manifest_path = self.metadata_dir / "published_manifest.json"
        self.row_count = 0
        self.failed_requests = 0
        self.first_timestamp: str | None = None
        self.last_timestamp: str | None = None
        self.previous_profile = self._load_previous_profile()
        self.checkpoint = self._load_checkpoint()
        self._chunk_outputs = self._load_output_index()
        self.initial_checkpoint_row_count = self._checkpoint_row_count()
        self._active_chunk_id: str | None = None
        self._chunk_attempts: dict[str, int] = {}
        self._chunk_first_attempt_at: dict[str, str] = {}
        self._publish_status = PUBLISH_UNPUBLISHED
        self.write_manifest()
        self.write_run_manifest()

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
                "source_key": self.source_key,
                "dataset_name": self.dataset_name,
                "run_id": self.run_id,
                "completed_chunks": {},
                "failed_chunks": {},
                "skipped_chunks": {},
                "chunk_outputs": {},
                "last_run_at": None,
            }
        try:
            checkpoint = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            checkpoint = {}
        checkpoint.setdefault("source_id", self.source_id)
        checkpoint.setdefault("source_key", self.source_key)
        checkpoint.setdefault("dataset_name", self.dataset_name)
        checkpoint.setdefault("run_id", self.run_id)
        checkpoint.setdefault("completed_chunks", {})
        checkpoint.setdefault("failed_chunks", {})
        checkpoint.setdefault("skipped_chunks", {})
        checkpoint.setdefault("expected_chunks", {})
        checkpoint.setdefault("chunk_outputs", {})
        checkpoint.setdefault("last_run_at", None)
        return checkpoint

    def _load_output_index(self) -> dict[str, list[dict[str, Any]]]:
        raw_index = self.checkpoint.get("chunk_outputs", {})
        if not isinstance(raw_index, dict):
            return {}
        output: dict[str, list[dict[str, Any]]] = {}
        for chunk_id, rows in raw_index.items():
            if not isinstance(rows, list):
                continue
            output[str(chunk_id)] = [dict(row) for row in rows if isinstance(row, dict)]
        return output

    def write_manifest(self) -> None:
        manifest = {
            "source_id": self.source_id,
            "source_key": self.source_key,
            "dataset_name": self.dataset_name,
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

    def register_plan(self, plan: DownloadPlan) -> None:
        """Register expected chunks before execution so coverage can detect gaps."""
        for chunk in plan.chunks:
            self.expect_chunk(chunk)

    def expect_chunk(
        self,
        chunk: str | DownloadChunk,
        *,
        required: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if isinstance(chunk, DownloadChunk):
            chunk_id = chunk.chunk_id
            required = chunk.required
            chunk_metadata = {
                "logical_window": dict(chunk.logical_window),
                "relative_path": chunk.relative_path,
            }
            if metadata:
                chunk_metadata.update(metadata)
        else:
            chunk_id = str(chunk)
            chunk_metadata = dict(metadata or {})

        expected = self.checkpoint.setdefault("expected_chunks", {})
        current = expected.get(chunk_id) if isinstance(expected.get(chunk_id), dict) else {}
        expected[chunk_id] = {
            "planned_at": current.get("planned_at") or now_iso(),
            "required": bool(current.get("required", required) or required),
            "metadata": {**dict(current.get("metadata") or {}), **chunk_metadata},
        }
        self._write_checkpoint()

    def expect_chunks(self, chunks: Iterable[str | DownloadChunk]) -> None:
        for chunk in chunks:
            self.expect_chunk(chunk)

    def should_skip(self, chunk_id: str) -> bool:
        self.expect_chunk(chunk_id)
        self._active_chunk_id = chunk_id
        self._chunk_first_attempt_at.setdefault(chunk_id, now_iso())
        completed = self.checkpoint.get("completed_chunks", {})
        if (
            self.context.resume
            and not self.context.overwrite
            and chunk_id in completed
            and self._completed_chunk_outputs_exist(chunk_id)
        ):
            self.mark_skipped(chunk_id, "already_completed")
            self._active_chunk_id = None
            return True
        self._chunk_attempts[chunk_id] = self._chunk_attempts.get(chunk_id, 0) + 1
        return False

    def mark_complete(self, chunk_id: str, metadata: dict[str, Any] | None = None) -> None:
        metadata = dict(metadata or {})
        outputs = self._chunk_outputs.get(chunk_id, [])
        record_count = metadata.get("record_count")
        if record_count is None:
            record_count = sum(int(output.get("record_count") or 0) for output in outputs)
        if outputs:
            metadata.setdefault("paths", [output["path"] for output in outputs])
            metadata.setdefault(
                "checksums",
                {
                    output["path"]: output.get("checksum")
                    for output in outputs
                    if output.get("checksum")
                },
            )
        completed = self.checkpoint.setdefault("completed_chunks", {})
        completed[chunk_id] = {
            "completed_at": now_iso(),
            "first_attempt_at": self._chunk_first_attempt_at.get(chunk_id),
            "attempts": self._chunk_attempts.get(chunk_id, 1),
            "record_count": record_count,
            **metadata,
        }
        self.checkpoint.setdefault("failed_chunks", {}).pop(chunk_id, None)
        self.checkpoint.setdefault("skipped_chunks", {}).pop(chunk_id, None)
        self._active_chunk_id = None
        self._write_checkpoint()
        self._write_profile("running")
        self.write_run_manifest()

    def mark_failed(self, chunk_id: str, error: str) -> None:
        failed = self.checkpoint.setdefault("failed_chunks", {})
        failed[chunk_id] = {
            "failed_at": now_iso(),
            "first_attempt_at": self._chunk_first_attempt_at.get(chunk_id),
            "attempts": self._chunk_attempts.get(chunk_id, 1),
            "error": error,
        }
        self._active_chunk_id = None
        self._write_checkpoint()
        self._write_profile("partial")
        self.write_run_manifest()

    def mark_skipped(self, chunk_id: str, reason: str) -> None:
        skipped = self.checkpoint.setdefault("skipped_chunks", {})
        skipped[chunk_id] = {
            "skipped_at": now_iso(),
            "first_attempt_at": self._chunk_first_attempt_at.get(chunk_id),
            "reason": reason,
        }
        self._write_checkpoint()
        self.write_run_manifest()

    def _write_checkpoint(self) -> None:
        self.checkpoint["last_run_at"] = now_iso()
        self.checkpoint["chunk_outputs"] = self._chunk_outputs
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
        error_class: str | None = None,
        retryable: bool | None = None,
    ) -> None:
        event = {
            "timestamp": now_iso(),
            "run_id": self.run_id,
            "source_id": self.source_id,
            "source_key": self.source_key,
            "dataset_name": self.dataset_name,
            "chunk_id": self._active_chunk_id,
            "url": url,
            "status_code": status_code,
            "record_count": record_count,
            "duration_ms": duration_ms,
            "retry_count": retry_count,
        }
        if retryable is not None:
            event["retryable"] = retryable
        if error_class:
            event["error_class"] = error_class
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
        tmp_path = self._staging_path(path)
        try:
            tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            self._atomic_publish(tmp_path, path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        count = estimate_record_count(payload) if record_count is None else record_count
        self._record_output(path, count, payload)
        return path

    def write_jsonl(self, relative_path: str, records: Iterable[dict[str, Any]]) -> Path:
        path = self._resolve_output_path(relative_path)
        tmp_path = self._staging_path(path)
        rows = list(records)
        try:
            with tmp_path.open("w", encoding="utf-8", newline="\n") as file:
                for record in rows:
                    file.write(json.dumps(record, ensure_ascii=False) + "\n")
            self._atomic_publish(tmp_path, path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        self._record_output(path, len(rows), rows)
        return path

    def _resolve_output_path(self, relative_path: str) -> Path:
        path = self.raw_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _staging_path(self, final_path: Path) -> Path:
        relative = final_path.relative_to(self.raw_dir)
        path = self.staging_dir / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        return path.with_suffix(path.suffix + ".part")

    def _atomic_publish(self, tmp_path: Path, final_path: Path) -> None:
        final_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path.replace(final_path)

    def _record_output(self, path: Path, record_count: int, payload: Any) -> None:
        self.row_count += max(record_count, 0)
        self._update_timestamps(payload)
        chunk_id = self._active_chunk_id or "__unassigned__"
        output = {
            "path": str(path),
            "relative_path": path.relative_to(self.raw_dir).as_posix(),
            "record_count": max(record_count, 0),
            "checksum": file_sha256(path),
            "size_bytes": path.stat().st_size,
            "written_at": now_iso(),
        }
        current_outputs = self._chunk_outputs.setdefault(chunk_id, [])
        self._chunk_outputs[chunk_id] = [
            row for row in current_outputs
            if row.get("relative_path") != output["relative_path"]
        ]
        self._chunk_outputs[chunk_id].append(output)

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
            "source_key": self.source_key,
            "dataset_name": self.dataset_name,
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
        profile = self._write_profile(status, error)
        manifest_dict = self.write_run_manifest(finished_at=profile.get("finished_at"))
        manifest = self._build_run_manifest(finished_at=profile.get("finished_at"))
        if status in {"success", "partial"} and self._can_publish(manifest):
            self.publish(manifest)
        elif status == "failed":
            manifest_dict["publish_status"] = PUBLISH_UNPUBLISHED
        return profile

    def write_run_manifest(self, finished_at: str | None = None) -> dict[str, Any]:
        manifest = self._build_run_manifest(finished_at=finished_at)
        payload = manifest.to_dict()
        self.run_manifest_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return payload

    def publish(self, manifest: RunManifest | None = None) -> dict[str, Any]:
        manifest = manifest or self._build_run_manifest()
        publish_status = (
            PUBLISH_WITH_WARNING
            if manifest.coverage_status == COVERAGE_PARTIAL
            else PUBLISH_PUBLISHED
        )
        self._publish_status = publish_status
        published = PublishedManifest(
            source_id=self.source_id,
            dataset_name=self.dataset_name,
            run_id=self.run_id,
            published_at=now_iso(),
            coverage_status=manifest.coverage_status,
            publish_status=publish_status,
            chunks=tuple(
                chunk
                for chunk in manifest.chunks
                if chunk.status in {CHUNK_SUCCESS, CHUNK_SKIPPED}
            ),
            raw_dir=str(self.raw_dir),
            source_key=self.source_key,
            downstream_raw_path=manifest.downstream_raw_path,
        )
        payload = published.to_dict()
        self.published_manifest_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        shutil.copy2(self.published_manifest_path, self.metadata_published_manifest_path)
        self.write_run_manifest(finished_at=manifest.finished_at)
        return payload

    def _build_run_manifest(self, finished_at: str | None = None) -> RunManifest:
        chunks = self._chunk_results()
        counted_chunks = [chunk for chunk in chunks if chunk.chunk_id != "__unassigned__"]
        success = sum(1 for chunk in counted_chunks if chunk.status == CHUNK_SUCCESS)
        failed = sum(1 for chunk in counted_chunks if chunk.status == CHUNK_FAILED)
        skipped = sum(1 for chunk in counted_chunks if chunk.status == CHUNK_SKIPPED)
        expected = len(counted_chunks)
        covered = success + skipped
        coverage_ratio = round(covered / expected, 4) if expected else 0.0
        coverage_status = self._coverage_status(counted_chunks, coverage_ratio)
        return RunManifest(
            source_id=self.source_id,
            dataset_name=self.dataset_name,
            run_id=self.run_id,
            expected_chunks=expected,
            successful_chunks=success,
            failed_chunks=failed,
            skipped_chunks=skipped,
            coverage_ratio=coverage_ratio,
            coverage_status=coverage_status,
            publish_status=self._publish_status,
            chunks=tuple(chunks),
            started_at=self.started_at,
            updated_at=now_iso(),
            finished_at=finished_at,
            raw_dir=str(self.raw_dir),
            details={
                "mode": self.context.mode_name,
                "source_key": self.source_key,
                "request_log_path": str(self.request_log_path),
                "checkpoint_path": str(self.checkpoint_path),
                "source_manifest_path": str(self.manifest_path),
            },
        )

    def _chunk_results(self) -> list[ChunkResult]:
        results: list[ChunkResult] = []
        completed = self.checkpoint.get("completed_chunks", {})
        failed = self.checkpoint.get("failed_chunks", {})
        skipped = self.checkpoint.get("skipped_chunks", {})
        expected = self.checkpoint.get("expected_chunks", {})
        expected_ids = set(expected) if isinstance(expected, dict) else set()
        policy_required_ids = set(self._coverage_policy().required_chunks)
        chunk_ids = sorted(
            set(completed)
            | set(failed)
            | set(skipped)
            | set(self._chunk_outputs)
            | expected_ids
            | policy_required_ids
        )
        for chunk_id in chunk_ids:
            outputs = self._chunk_outputs.get(chunk_id, [])
            paths = tuple(str(output.get("path")) for output in outputs if output.get("path"))
            checksums = {
                str(output.get("path")): str(output.get("checksum"))
                for output in outputs
                if output.get("path") and output.get("checksum")
            }
            expected_metadata = (
                dict(expected.get(chunk_id) or {})
                if isinstance(expected, dict)
                else {}
            )
            required = bool(
                expected_metadata.get("required", chunk_id in policy_required_ids or True)
            )
            if chunk_id in completed:
                metadata = dict(completed[chunk_id])
                status = CHUNK_SUCCESS
                finished_at = metadata.get("completed_at")
                error = None
            elif chunk_id in failed:
                metadata = dict(failed[chunk_id])
                status = CHUNK_FAILED
                finished_at = metadata.get("failed_at")
                error = metadata.get("error")
            elif chunk_id in skipped:
                metadata = dict(skipped.get(chunk_id, {}))
                status = CHUNK_SKIPPED
                finished_at = metadata.get("skipped_at")
                error = metadata.get("reason")
            else:
                metadata = {
                    "planned_at": expected_metadata.get("planned_at"),
                    "reason": "expected_chunk_missing",
                    **dict(expected_metadata.get("metadata") or {}),
                }
                status = CHUNK_FAILED
                finished_at = None
                error = "expected_chunk_missing"
            record_count = int(
                metadata.get("record_count")
                or sum(int(output.get("record_count") or 0) for output in outputs)
            )
            results.append(
                ChunkResult(
                    chunk_id=chunk_id,
                    status=status,
                    required=required,
                    paths=paths,
                    checksums=checksums,
                    record_count=record_count,
                    quarantine_count=int(metadata.get("quarantine_count") or 0),
                    attempts=int(metadata.get("attempts") or self._chunk_attempts.get(chunk_id, 0)),
                    error=error,
                    first_attempt_at=metadata.get("first_attempt_at")
                    or self._chunk_first_attempt_at.get(chunk_id),
                    finished_at=finished_at,
                    metadata=metadata,
                )
            )
        return results

    def _coverage_status(self, chunks: list[ChunkResult], coverage_ratio: float) -> str:
        if not chunks:
            return COVERAGE_FAILED
        policy = self._coverage_policy()
        observed_chunk_ids = {chunk.chunk_id for chunk in chunks}
        missing_required = [
            chunk_id for chunk_id in policy.required_chunks
            if chunk_id not in observed_chunk_ids
        ]
        if missing_required:
            return COVERAGE_FAILED
        failed_required = [chunk for chunk in chunks if chunk.status == CHUNK_FAILED and chunk.required]
        if failed_required:
            return COVERAGE_FAILED
        if coverage_ratio >= policy.min_success_ratio:
            return COVERAGE_COMPLETE
        if policy.allow_publish_with_warnings and coverage_ratio > 0:
            return COVERAGE_PARTIAL
        return COVERAGE_FAILED

    def _coverage_policy(self) -> CoveragePolicy:
        runtime_cfg = self.context.config.get("resilient_runtime", {})
        source_cfg = self.context.config.get(self.source_key, {})
        coverage = {
            **dict(runtime_cfg.get("coverage_policy") or {}),
            **dict(source_cfg.get("coverage_policy") or {}),
        }
        return CoveragePolicy(
            min_success_ratio=float(coverage.get("min_success_ratio", 1.0)),
            allow_publish_with_warnings=bool(coverage.get("allow_publish_with_warnings", False)),
            required_chunks=tuple(str(item) for item in coverage.get("required_chunks") or ()),
        )

    def _can_publish(self, manifest: RunManifest) -> bool:
        if manifest.coverage_status == COVERAGE_COMPLETE:
            return True
        if manifest.coverage_status == COVERAGE_PARTIAL:
            return self._coverage_policy().allow_publish_with_warnings
        return False

    def _completed_chunk_outputs_exist(self, chunk_id: str) -> bool:
        outputs = self._chunk_outputs.get(chunk_id) or []
        if not outputs:
            return True
        return all(Path(str(output.get("path"))).exists() for output in outputs if output.get("path"))


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
