from __future__ import annotations

import json

from governance.lineage import record_lineage


def test_record_lineage_writes_openlineage_event(tmp_path) -> None:
    lineage_log = tmp_path / "lineage.jsonl"

    record_lineage(
        "demo_job",
        ["raw.demo"],
        ["silver.demo"],
        batch_id="batch-1",
        run_id="run-1",
        source_path="samples/demo.csv",
        actor="tester",
        lineage_log=lineage_log,
    )

    event = json.loads(lineage_log.read_text(encoding="utf-8"))
    assert event["eventType"] == "COMPLETE"
    assert event["run"]["runId"] == "run-1"
    assert event["inputs"][0]["name"] == "raw.demo"
    assert event["outputs"][0]["name"] == "silver.demo"
    assert event["batch_id"] == "batch-1"
    assert event["actor"] == "tester"
