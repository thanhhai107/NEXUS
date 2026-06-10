from __future__ import annotations

import json

import pytest

from governance.lineage import record_lineage


def test_record_lineage_writes_openlineage_event(tmp_path) -> None:
    lineage_log = tmp_path / "lineage.jsonl"

    record_lineage(
        "tpcdi_trade_job",
        ["bronze.tpcdi_trade"],
        ["silver.tpcdi_trade"],
        batch_id="batch-1",
        run_id="run-1",
        source_path="runtime/tpcdi/sf3/Batch1/Trade.txt",
        actor="tester",
        lineage_log=lineage_log,
    )

    event = json.loads(lineage_log.read_text(encoding="utf-8"))
    assert event["eventType"] == "COMPLETE"
    assert event["run"]["runId"] == "run-1"
    assert event["inputs"][0]["name"] == "bronze.tpcdi_trade"
    assert event["outputs"][0]["name"] == "silver.tpcdi_trade"
    assert event["batch_id"] == "batch-1"
    assert event["actor"] == "tester"


def test_record_lineage_emits_openlineage_event_when_configured(tmp_path, monkeypatch) -> None:
    lineage_log = tmp_path / "lineage.jsonl"
    captured = {}

    class Response:
        def raise_for_status(self) -> None:
            return None

    def fake_post(url, json, timeout):
        captured["url"] = url
        captured["json"] = json
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setenv("OPENLINEAGE_URL", "http://lineage:5000")
    monkeypatch.setenv("OPENLINEAGE_NAMESPACE", "test-namespace")
    monkeypatch.setenv("OPENLINEAGE_TIMEOUT_SECONDS", "1.5")
    monkeypatch.setattr("requests.post", fake_post)

    record_lineage(
        "tpcdi_trade_job",
        ["bronze.tpcdi_trade"],
        ["silver.tpcdi_trade"],
        batch_id="batch-1",
        run_id="run-1",
        lineage_log=lineage_log,
    )

    assert captured["url"] == "http://lineage:5000/api/v1/lineage"
    assert captured["timeout"] == 1.5
    assert captured["json"]["job"]["namespace"] == "test-namespace"
    assert captured["json"]["inputs"][0]["name"] == "bronze.tpcdi_trade"
    assert "job_name" not in captured["json"]


def test_record_lineage_strict_mode_raises_on_openlineage_error(tmp_path, monkeypatch) -> None:
    lineage_log = tmp_path / "lineage.jsonl"

    def fake_post(url, json, timeout):
        raise RuntimeError("cannot connect")

    monkeypatch.setenv("OPENLINEAGE_URL", "http://lineage:5000")
    monkeypatch.setenv("OPENLINEAGE_STRICT", "true")
    monkeypatch.setattr("requests.post", fake_post)

    with pytest.raises(RuntimeError, match="Failed to emit OpenLineage event"):
        record_lineage(
            "tpcdi_trade_job",
            ["bronze.tpcdi_trade"],
            ["silver.tpcdi_trade"],
            lineage_log=lineage_log,
        )
