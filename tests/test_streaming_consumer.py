import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from ingestion.streaming import consumer as consumer_module


class FakeMessage:
    def __init__(self, value, topic="test-topic", partition=0, offset=0, timestamp=None):
        self.value = value
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp


class FakeConsumer:
    def __init__(self, messages):
        self._messages = list(messages)
        self.committed = False
        self.closed = False

    def poll(self, timeout=1.0):
        if self._messages:
            return self._messages.pop(0)
        return None

    def commit(self, **kwargs):
        self.committed = True

    def close(self):
        self.closed = True


@pytest.fixture
def redirect_runtime(monkeypatch, tmp_path):
    raw_dir = tmp_path / "raw"
    dlq_dir = tmp_path / "dlq"
    raw_dir.mkdir(parents=True)
    dlq_dir.mkdir()
    monkeypatch.setattr("ingestion.batch.common.LOCAL_RAW_DIR", raw_dir)
    monkeypatch.setattr("governance.dlq.DEFAULT_DLQ_DIR", dlq_dir)
    monkeypatch.setattr("governance.storage.using_postgres_storage", lambda: False)

    def fake_write_events(events, dataset, source, run_id=None):
        import json
        import uuid
        from datetime import datetime, timezone
        from pathlib import Path

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out_dir = tmp_path / f"streaming_{dataset}"
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"{ts}_{uuid.uuid4().hex[:8]}.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for ev in events:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
        return path

    monkeypatch.setattr(
        "ingestion.streaming.consumer.write_events_to_raw", fake_write_events
    )
    monkeypatch.setattr(
        "ingestion.streaming.consumer._publish_streaming_raw_envelope",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        "ingestion.streaming.consumer._route_to_dlq",
        lambda *a, **kw: None,
    )
    return SimpleNamespace(raw_dir=raw_dir, dlq_dir=dlq_dir)


def _install_fake_kafka(monkeypatch, messages):
    fake_consumer = FakeConsumer(messages)

    def fake_create(*args, **kwargs):
        return fake_consumer

    def fake_consume(consumer, max_msgs, timeout):
        for msg in messages:
            yield msg

    monkeypatch.setattr("ingestion.streaming.consumer.create_kafka_consumer", fake_create)
    monkeypatch.setattr("ingestion.streaming.consumer.consume_kafka_messages", fake_consume)
    return fake_consumer


def test_consumer_lands_records_to_raw(monkeypatch, redirect_runtime):
    messages = [
        FakeMessage(json.dumps({"event_id": "a", "value": 1}).encode("utf-8")),
        FakeMessage(json.dumps({"event_id": "b", "value": 2}).encode("utf-8")),
    ]
    _install_fake_kafka(monkeypatch, messages)

    summary = consumer_module.consume_to_raw(
        topic="transport-events",
        dataset="transport_events",
        max_messages=10,
    )
    assert summary["consumed"] == 2
    assert summary["landed"] == 2
    assert summary["dlq"] == 0
    assert summary["raw_path"] is not None
    raw_path_obj = Path(summary["raw_path"])
    assert raw_path_obj.exists()
    lines = raw_path_obj.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2


def test_consumer_routes_invalid_payload_to_dlq(monkeypatch, redirect_runtime):
    messages = [
        FakeMessage(b"not-json"),
        FakeMessage(json.dumps({"event_id": "ok"}).encode("utf-8")),
    ]
    _install_fake_kafka(monkeypatch, messages)

    summary = consumer_module.consume_to_raw(
        topic="transport-events",
        dataset="transport_events",
        max_messages=10,
    )
    assert summary["consumed"] == 2
    assert summary["landed"] == 1
    assert summary["dlq"] == 1