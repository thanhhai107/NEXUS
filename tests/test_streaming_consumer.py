import json
from types import SimpleNamespace

import pytest

from ingestion.streaming import consumer as consumer_module


class FakeMessage:
    def __init__(self, value):
        self.value = value


class FakeConsumer:
    def __init__(self, messages):
        self._messages = list(messages)
        self.committed = False
        self.closed = False

    def __iter__(self):
        return iter(self._messages)

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


@pytest.fixture
def redirect_runtime(monkeypatch, tmp_path):
    raw_dir = tmp_path / "raw"
    dlq_dir = tmp_path / "dlq"
    raw_dir.mkdir()
    dlq_dir.mkdir()
    monkeypatch.setattr("ingestion.batch.common.LOCAL_RAW_DIR", raw_dir)
    monkeypatch.setattr("governance.dlq.DEFAULT_DLQ_DIR", dlq_dir)
    return SimpleNamespace(raw_dir=raw_dir, dlq_dir=dlq_dir)


def _install_fake_kafka(monkeypatch, messages):
    fake_consumer = FakeConsumer(messages)

    def factory(*args, **kwargs):
        return fake_consumer

    fake_module = SimpleNamespace(KafkaConsumer=factory)
    monkeypatch.setitem(__import__("sys").modules, "kafka", fake_module)
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
    raw_files = list((redirect_runtime.raw_dir / "transport_events").glob("*.jsonl"))
    assert len(raw_files) == 1
    lines = raw_files[0].read_text(encoding="utf-8").splitlines()
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
    dlq_files = list(redirect_runtime.dlq_dir.glob("*.jsonl"))
    assert dlq_files, "expected a DLQ file to be written"
    payloads = [json.loads(line) for line in dlq_files[0].read_text(encoding="utf-8").splitlines() if line.strip()]
    assert payloads[0]["category"] == "streaming_consume_failed"
    assert payloads[0]["topic"] == "transport-events"