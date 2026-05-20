import pytest

from governance import dlq as dlq_module


@pytest.fixture
def tmp_dlq_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("NEXUS_GOVERNANCE_STORAGE", "local")
    return tmp_path / "dlq"


def test_record_dlq_event_writes_jsonl(tmp_dlq_dir):
    output = dlq_module.record_dlq_event(
        category="streaming_publish_failed",
        payload={"event_id": "abc", "value": 1},
        source="transport",
        error="boom",
        error_type="RuntimeError",
        attempts=3,
        topic="transport-events",
        dataset="transport_events",
        dlq_dir=tmp_dlq_dir,
    )
    assert output.exists()
    events = dlq_module.list_dlq_events(tmp_dlq_dir)
    assert len(events) == 1
    event = events[0]
    assert event["category"] == "streaming_publish_failed"
    assert event["source"] == "transport"
    assert event["attempts"] == 3
    assert event["payload"]["event_id"] == "abc"


def test_replay_dlq_events_filters_and_calls_handler(tmp_dlq_dir):
    dlq_module.record_dlq_event(
        category="streaming_publish_failed",
        payload={"event_id": "a"},
        source="transport",
        error="boom",
        topic="transport-events",
        dataset="transport_events",
        dlq_dir=tmp_dlq_dir,
    )
    dlq_module.record_dlq_event(
        category="job_failed",
        payload={"event_id": "b"},
        source="openaq",
        error="timeout",
        topic="environment-openaq",
        dataset="openaq_measurements",
        dlq_dir=tmp_dlq_dir,
    )

    seen: list[str] = []

    def handler(event):
        seen.append(event["payload"]["event_id"])
        return True

    summary = dlq_module.replay_dlq_events(
        handler,
        category="streaming_publish_failed",
        dlq_dir=tmp_dlq_dir,
    )
    assert summary == {"matched": 1, "succeeded": 1, "failed": []}
    assert seen == ["a"]


def test_replay_dlq_events_collects_failures(tmp_dlq_dir):
    dlq_module.record_dlq_event(
        category="streaming_publish_failed",
        payload={"event_id": "a"},
        source="transport",
        error="boom",
        dlq_dir=tmp_dlq_dir,
    )

    def handler(event):
        raise RuntimeError("replay failed")

    summary = dlq_module.replay_dlq_events(handler, dlq_dir=tmp_dlq_dir)
    assert summary["matched"] == 1
    assert summary["succeeded"] == 0
    assert len(summary["failed"]) == 1
    assert "replay failed" in summary["failed"][0]["error"]