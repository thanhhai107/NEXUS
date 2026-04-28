from __future__ import annotations

import json

from governance.agents import tools


def test_quality_report_reads_streaming_quality_events(tmp_path, monkeypatch) -> None:
    audit_log = tmp_path / "audit.jsonl"
    audit_log.write_text(
        json.dumps({
            "event_type": "streaming_quality_check",
            "dataset": "transport_events",
            "status": "passed",
            "details": {"readiness_score": 1.0},
            "timestamp": "2026-04-28T00:00:00+00:00",
        })
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(tools, "AUDIT_LOG", audit_log)

    report = tools.load_quality_report("transport_events")

    assert report["status"] == "passed"
    assert report["details"]["readiness_score"] == 1.0
