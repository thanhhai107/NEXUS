from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests

from ingestion.base.core import DownloadContext, SourceFailure, SourceRun
from ingestion.base.http import request_json
from ingestion.downloaders.raw_adapter import published_run_to_raw_envelope


def context(
    tmp_path: Path,
    *,
    max_attempts: int = 2,
    required_chunks: list[str] | None = None,
    raw_envelope_enabled: bool = False,
    schema_validation_enabled: bool = False,
) -> DownloadContext:
    return DownloadContext(
        config={
            "spatial_scope": {"name": "test"},
            "resilient_runtime": {
                "retry_policy": {
                    "max_attempts": max_attempts,
                    "backoff_base_seconds": 0,
                    "backoff_max_seconds": 0,
                    "jitter_seconds": 0,
                    "retryable_status_codes": [429, 500, 502, 503, 504],
                },
                "coverage_policy": {
                    "min_success_ratio": 1.0,
                    "allow_publish_with_warnings": False,
                    "required_chunks": required_chunks or [],
                },
                "publish_policy": {
                    "raw_envelope_enabled": raw_envelope_enabled,
                },
                "validation_policy": {
                    "schema_validation_enabled": schema_validation_enabled,
                    "quarantine_invalid_records": True,
                },
            },
            "rate_limits": {"default_delay_seconds": 0},
        },
        mode_name="test",
        mode={},
        output_dir=tmp_path,
        run_id="run-1",
    )


def test_source_run_writes_checkpoint_manifest_and_published_manifest(tmp_path: Path) -> None:
    run = SourceRun("demo_source", context(tmp_path), "demo", dataset_name="demo_dataset")

    assert run.should_skip("chunk-a") is False
    chunk_path = run.write_jsonl("chunk-a/data.jsonl", [{"id": "1"}, {"id": "2"}])
    run.mark_complete("chunk-a", {"record_count": 2})
    run.finish("success")

    checkpoint = json.loads(run.checkpoint_path.read_text(encoding="utf-8"))
    run_manifest = json.loads(run.run_manifest_path.read_text(encoding="utf-8"))
    published = json.loads(run.published_manifest_path.read_text(encoding="utf-8"))

    assert chunk_path.exists()
    assert not list(run.staging_dir.rglob("*.part"))
    assert checkpoint["completed_chunks"]["chunk-a"]["record_count"] == 2
    assert run_manifest["coverage_status"] == "complete"
    assert run_manifest["publish_status"] == "published"
    assert published["dataset_name"] == "demo_dataset"
    assert published["chunks"][0]["checksums"]


def test_partial_run_does_not_publish_manifest(tmp_path: Path) -> None:
    run = SourceRun("demo_source", context(tmp_path), "demo")

    assert run.should_skip("chunk-a") is False
    run.write_jsonl("chunk-a/data.jsonl", [{"id": "1"}])
    run.mark_complete("chunk-a", {"record_count": 1})
    assert run.should_skip("chunk-b") is False
    run.mark_failed("chunk-b", "timeout")
    run.finish("partial")

    run_manifest = json.loads(run.run_manifest_path.read_text(encoding="utf-8"))
    assert run_manifest["coverage_status"] == "failed"
    assert not run.published_manifest_path.exists()


def test_missing_expected_chunk_blocks_publish(tmp_path: Path) -> None:
    run = SourceRun("demo_source", context(tmp_path), "demo")
    run.expect_chunks(["chunk-a", "chunk-b"])

    assert run.should_skip("chunk-a") is False
    run.write_jsonl("chunk-a/data.jsonl", [{"id": "1"}])
    run.mark_complete("chunk-a", {"record_count": 1})
    run.finish("success")

    run_manifest = json.loads(run.run_manifest_path.read_text(encoding="utf-8"))
    missing = [chunk for chunk in run_manifest["chunks"] if chunk["chunk_id"] == "chunk-b"][0]
    assert run_manifest["coverage_status"] == "failed"
    assert missing["status"] == "failed"
    assert missing["error"] == "expected_chunk_missing"
    assert not run.published_manifest_path.exists()


def test_required_chunk_policy_blocks_publish(tmp_path: Path) -> None:
    run = SourceRun(
        "demo_source",
        context(tmp_path, required_chunks=["chunk-a", "chunk-b"]),
        "demo",
    )

    assert run.should_skip("chunk-a") is False
    run.write_jsonl("chunk-a/data.jsonl", [{"id": "1"}])
    run.mark_complete("chunk-a", {"record_count": 1})
    run.finish("success")

    run_manifest = json.loads(run.run_manifest_path.read_text(encoding="utf-8"))
    assert run_manifest["coverage_status"] == "failed"
    assert not run.published_manifest_path.exists()


def test_resume_skips_completed_chunk_without_duplicate_outputs(tmp_path: Path) -> None:
    first = SourceRun("demo_source", context(tmp_path), "demo")
    assert first.should_skip("chunk-a") is False
    first.write_jsonl("chunk-a/data.jsonl", [{"id": "1"}])
    first.mark_complete("chunk-a", {"record_count": 1})
    first.finish("success")

    second = SourceRun("demo_source", context(tmp_path), "demo")
    assert second.should_skip("chunk-a") is True
    second.finish("success")

    raw_files = list((tmp_path / "demo_source" / "run_id=run-1" / "raw").rglob("*.jsonl"))
    run_manifest = json.loads(second.run_manifest_path.read_text(encoding="utf-8"))
    assert len(raw_files) == 1
    assert run_manifest["coverage_status"] == "complete"


def test_published_run_converts_to_raw_envelope_idempotently(tmp_path: Path) -> None:
    run = SourceRun("demo_source", context(tmp_path), "demo", dataset_name="demo_dataset")
    assert run.should_skip("chunk-a") is False
    run.write_jsonl("chunk-a/data.jsonl", [{"id": "1"}])
    run.mark_complete("chunk-a", {"record_count": 1})
    run.finish("success")

    result = published_run_to_raw_envelope(run.published_manifest_path, output_dir=tmp_path / "raw")
    result_again = published_run_to_raw_envelope(run.published_manifest_path, output_dir=tmp_path / "raw")

    raw_path = Path(result["raw_path"])
    rows = [json.loads(line) for line in raw_path.read_text(encoding="utf-8").splitlines()]
    assert result["raw_path"] == result_again["raw_path"]
    assert len(rows) == 1
    assert rows[0]["_nexus_dataset"] == "demo_dataset"
    assert rows[0]["_nexus_chunk_id"] == "chunk-a"
    assert rows[0]["payload"] == {"id": "1"}


def test_downloader_publish_routes_parser_failures_to_dlq(monkeypatch, tmp_path: Path) -> None:
    dlq_dir = tmp_path / "dlq"
    monkeypatch.setattr("governance.dlq.DEFAULT_DLQ_DIR", dlq_dir)
    monkeypatch.setattr("ingestion.downloaders.raw_adapter.RAW_OUTPUT_DIR", tmp_path / "raw")
    run = SourceRun(
        "demo_source",
        context(tmp_path, raw_envelope_enabled=True),
        "demo",
        dataset_name="demo_dataset",
    )

    assert run.should_skip("chunk-a") is False
    chunk_path = run.write_jsonl("chunk-a/data.jsonl", [{"id": "1"}])
    chunk_path.write_text('{"id": "1"}\nnot-json\n', encoding="utf-8")
    run.mark_complete("chunk-a", {"record_count": 2})
    run.finish("success")

    result = maybe_publish_raw_envelope(run, run.context)

    assert result is not None
    assert result["parser_failures"] == 1
    dlq_events = [
        json.loads(line)
        for path in dlq_dir.glob("*.jsonl")
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert dlq_events
    assert dlq_events[0]["category"] == "download_parser_failed"


def test_downloader_validation_quarantines_invalid_payloads(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("ingestion.downloaders.raw_adapter.RAW_OUTPUT_DIR", tmp_path / "raw")
    monkeypatch.setattr(
        "ingestion.downloaders.validation.dataset_schema",
        lambda _dataset: {
            "type": "object",
            "required": ["id"],
            "properties": {"id": {"type": "integer"}},
        },
    )
    quarantined: list[dict[str, object]] = []

    def fake_quarantine(_dataset, invalid_records, **_kwargs):
        quarantined.extend(list(invalid_records))
        return tmp_path / "quarantine.jsonl"

    monkeypatch.setattr("ingestion.downloaders.validation.quarantine_records", fake_quarantine)
    run = SourceRun(
        "demo_source",
        context(tmp_path, raw_envelope_enabled=True, schema_validation_enabled=True),
        "demo",
        dataset_name="demo_dataset",
    )

    assert run.should_skip("chunk-a") is False
    run.write_jsonl("chunk-a/data.jsonl", [{"id": 1}, {"name": "bad"}])
    run.mark_complete("chunk-a", {"record_count": 2})
    run.finish("success")

    result = maybe_publish_raw_envelope(run, run.context)

    assert result is not None
    assert result["validation"]["schema_invalid_records"] == 1
    assert len(quarantined) == 1
    assert quarantined[0]["payload"] == {"name": "bad"}


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: dict[str, object],
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.reason = "error" if status_code >= 400 else "ok"
        self.content = json.dumps(payload).encode("utf-8")
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def json(self) -> dict[str, object]:
        return self._payload


def test_request_json_retries_transient_status(monkeypatch, tmp_path: Path) -> None:
    run = SourceRun("demo_source", context(tmp_path, max_attempts=2), "demo")
    calls = [FakeResponse(500, {"error": "temporary"}), FakeResponse(200, {"results": [{"id": "1"}]})]

    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("requests.get", lambda *_args, **_kwargs: calls.pop(0))

    payload = request_json(run, "https://example.test/data")

    events = [
        json.loads(line)
        for line in run.request_log_path.read_text(encoding="utf-8").splitlines()
    ]
    assert payload == {"results": [{"id": "1"}]}
    assert events[0]["error_class"] == "http_transient"
    assert events[0]["retryable"] is True
    assert events[1]["record_count"] == 1


def test_request_json_honors_retry_after_for_429(monkeypatch, tmp_path: Path) -> None:
    run = SourceRun("demo_source", context(tmp_path, max_attempts=2), "demo")
    calls = [
        FakeResponse(429, {"error": "slow down"}, headers={"Retry-After": "7"}),
        FakeResponse(200, {"results": [{"id": "1"}]}),
    ]
    sleeps: list[float] = []

    monkeypatch.setattr("time.sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr("requests.get", lambda *_args, **_kwargs: calls.pop(0))

    payload = request_json(run, "https://example.test/data")

    assert payload == {"results": [{"id": "1"}]}
    assert sleeps and sleeps[0] == 7


def test_request_json_bounded_timeout_retries(monkeypatch, tmp_path: Path) -> None:
    run = SourceRun("demo_source", context(tmp_path, max_attempts=2), "demo")

    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    def fail_timeout(*_args, **_kwargs):
        raise requests.Timeout("too slow")

    monkeypatch.setattr("requests.get", fail_timeout)

    with pytest.raises(SourceFailure):
        request_json(run, "https://example.test/data")

    events = [
        json.loads(line)
        for line in run.request_log_path.read_text(encoding="utf-8").splitlines()
    ]
    assert len(events) == 2
    assert all(event["error_class"] == "timeout" for event in events)
    assert events[-1]["retryable"] is False
