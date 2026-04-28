from __future__ import annotations

from governance.quality.metrics import load_quality_history, write_quality_metric


def test_quality_metric_history_roundtrip(tmp_path) -> None:
    metrics_log = tmp_path / "quality.jsonl"

    write_quality_metric(
        dataset="demo",
        batch_id="batch-1",
        status="passed",
        quality={
            "record_count": 3,
            "readiness_score": 0.98,
            "missing_ratio": 0.0,
            "duplicate_ratio": 0.0,
            "freshness_score": 1.0,
            "schema_valid": True,
            "issues": [],
        },
        auto_fix={"changed_record_count": 1},
        metrics_log=metrics_log,
    )

    history = load_quality_history("demo", metrics_log=metrics_log)
    assert len(history) == 1
    assert history[0]["batch_id"] == "batch-1"
    assert history[0]["auto_fix"]["changed_record_count"] == 1
