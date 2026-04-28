from __future__ import annotations

from ingestion.streaming.producer import event_stream


def test_transport_fallback() -> None:
    event = event_stream("transport", None, None, 1)[0]
    assert event["source"] == "simulated-transport-producer"
    assert event["event_id"]
    assert event["event_time"]


def test_environment_fallback() -> None:
    event = event_stream("openaq", None, None, 1)[0]
    assert event["source"] == "simulated-environment-producer"
    assert event["parameter"] in {"pm25", "pm10", "no2", "o3"}


def test_education_fallback() -> None:
    event = event_stream("education_sim", None, None, 1)[0]
    assert event["source"] == "simulated-education-producer"
    assert event["event_type"] in {"attendance_update", "enrollment_update"}
