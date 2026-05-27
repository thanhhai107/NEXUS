from __future__ import annotations

from ingestion.streaming import producer
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


def test_tfl_line_status_normalization_keeps_reason() -> None:
    events = producer._normalize_payload(
        "tfl_line_status",
        [
            {
                "id": "victoria",
                "name": "Victoria",
                "lineStatuses": [
                    {
                        "statusSeverity": 9,
                        "statusSeverityDescription": "Minor Delays",
                        "reason": "Signal failure",
                    }
                ],
            }
        ],
        limit=10,
    )

    assert events[0]["source"] == "tfl"
    assert events[0]["line_id"] == "victoria"
    assert events[0]["severity"] == 9
    assert events[0]["reason"] == "Signal failure"


def test_tfl_arrivals_normalization_uses_composite_event_id() -> None:
    events = producer._normalize_payload(
        "tfl_arrivals",
        [
            {
                "naptanId": "940GZZLUKSX",
                "stationName": "King's Cross St. Pancras Underground Station",
                "lineId": "victoria",
                "lineName": "Victoria",
                "vehicleId": "105",
                "destinationName": "Brixton Underground Station",
                "expectedArrival": "2026-05-27T17:44:11Z",
                "timeToStation": 145,
            }
        ],
        limit=10,
    )

    assert events[0]["event_id"] == "105:victoria:2026-05-27T17:44:11Z"
    assert events[0]["source"] == "tfl_arrivals"
    assert events[0]["stop_id"] == "940GZZLUKSX"
    assert events[0]["time_to_station"] == 145


def test_tfl_query_app_key_auth(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_raw_request(url, headers):
        captured["url"] = url
        captured["headers"] = headers
        return []

    monkeypatch.setattr(producer, "_raw_request", fake_raw_request)

    producer.fetch_api_events(
        "tfl_arrivals",
        "https://api.tfl.gov.uk/StopPoint/940GZZLUKSX/Arrivals?foo=bar",
        "secret",
        auth_header="query-app_key",
        limit=10,
    )

    assert captured["headers"] == {}
    assert captured["url"] == (
        "https://api.tfl.gov.uk/StopPoint/940GZZLUKSX/Arrivals?foo=bar&app_key=secret"
    )
