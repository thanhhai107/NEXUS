"""
Kafka Producer for NEXUS Streaming.

Produces events to Kafka topics with retry and DLQ support.

Usage:
    python -m ingestion.streaming.producer --source openaq --events 10

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS - Kafka broker address (default: localhost:29092)
    KAFKA_SECURITY_PROTOCOL - Security protocol (PLAINTEXT, SASL_SSL, etc.)
    NEXUS_DLQ_TOPIC         - DLQ topic name (default: nexus.dlq)
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ingestion.base.core import DownloadContext, SourceRun
from ingestion.base.http import download_file, request_json
from ingestion.base.utils import load_config, resolve_output_dir, run_id_now, sanitize_segment
from ingestion.streaming.kafka_config import (
    DEFAULT_STREAM_SOURCE,
    DLQ_TOPIC,
    KafkaConfig,
    ProducerConfig,
    StreamSourceConfig,
    STREAM_TOPICS,
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ============================================================================
# Event Generators (Simulated + Real API)
# ============================================================================

def sim_transport() -> dict[str, object]:
    """Generate simulated transport event."""
    import random
    return {
        "event_id": str(uuid.uuid4()),
        "source": "simulated-transport-producer",
        "event_type": random.choice(["traffic_accident", "road_closure", "slow_traffic"]),
        "event_time": now_iso(),
        "severity": random.randint(1, 4),
        "state": random.choice(["CA", "NY", "IL", "TX", "FL"]),
        "city": random.choice(["Los Angeles", "New York", "Chicago", "Houston", "Miami"]),
        "latitude": round(random.uniform(25.0, 48.0), 6),
        "longitude": round(random.uniform(-124.0, -71.0), 6),
    }


def sim_env() -> dict[str, object]:
    """Generate simulated environment/air quality event."""
    import random
    return {
        "event_id": str(uuid.uuid4()),
        "source": "simulated-environment-producer",
        "event_type": "air_quality_measurement",
        "event_time": now_iso(),
        "location": random.choice(["Hanoi", "Singapore", "Bangkok", "Jakarta"]),
        "parameter": random.choice(["pm25", "pm10", "no2", "o3"]),
        "value": round(random.uniform(5.0, 120.0), 2),
        "unit": "ug/m3",
    }


def sim_event(source: str, count: int) -> list[dict[str, object]]:
    """Generate simulated events for a source."""
    if source in {"openaq", "waqi", "londonair", "openmeteo", "openweather"}:
        return [sim_env() for _ in range(count)]
    return [sim_transport() for _ in range(count)]


# ============================================================================
# API Event Fetchers
# ============================================================================

def fetch_api_events(
    source: str,
    api_url: str | None,
    api_key: str | None,
    auth_header: str = "Authorization",
    limit: int = 100,
) -> list[dict[str, object]]:
    """Fetch events from real API and normalize them."""
    if not api_url:
        return []

    headers = {}
    if api_key:
        if auth_header == "X-API-Key":
            headers[auth_header] = api_key
        else:
            headers[auth_header] = f"Bearer {api_key}"

    try:
        payload = request_json.__wrapped__(None, api_url, headers=headers, timeout=20) if hasattr(request_json, '__wrapped__') else _raw_request(api_url, headers)
    except Exception:
        return []

    return _normalize_payload(source, payload, limit)


def _raw_request(url: str, headers: dict[str, str]) -> Any:
    """Direct HTTP request without SourceRun logging."""
    import requests
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()
    return response.json()


def _normalize_payload(source: str, payload: Any, limit: int) -> list[dict[str, object]]:
    """Normalize API payload into standard event format."""
    records = _extract_records(payload)

    if source == "waqi":
        return [_normalize_waqi(payload)][:limit]
    if source == "londonair":
        return _normalize_londonair(payload)[:limit]
    if source == "openmeteo":
        return _normalize_openmeteo(payload)[:limit]
    if source == "openweather":
        return [_normalize_openweather(payload)][:limit]
    if source == "tfl":
        events = []
        for record in records[:limit]:
            events.extend(_normalize_tfl(record))
        return events[:limit]

    return [_normalize_generic(record) for record in records[:limit]]


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("results", "data", "items", "events", "incidents"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _normalize_waqi(payload: dict[str, Any]) -> dict[str, object]:
    data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else payload
    city = data.get("city", {}) if isinstance(data.get("city"), dict) else {}
    time_data = data.get("time", {}) if isinstance(data.get("time"), dict) else {}
    geo = city.get("geo", [None, None]) if isinstance(city.get("geo"), list) else [None, None]
    return {
        "event_id": str(data.get("idx") or uuid.uuid4()),
        "source": "waqi",
        "event_type": "aqi_measurement",
        "event_time": str(time_data.get("iso") or now_iso()),
        "station_uid": data.get("idx"),
        "station_name": city.get("name"),
        "aqi": int(data.get("aqi")) if str(data.get("aqi", "")).isdigit() else None,
        "latitude": geo[0],
        "longitude": geo[1],
    }


def _normalize_londonair(payload: Any) -> list[dict[str, object]]:
    import random
    authorities = payload if isinstance(payload, list) else [payload]
    events = []
    for auth in authorities:
        if not isinstance(auth, dict):
            continue
        sites = auth.get("Site", [])
        if isinstance(sites, dict):
            sites = [sites]
        for site in sites:
            if not isinstance(site, dict):
                continue
            events.append({
                "event_id": str(uuid.uuid4()),
                "source": "londonair",
                "event_type": "air_quality_index",
                "event_time": str(site.get("@BulletinDate") or now_iso()),
                "site_code": site.get("@SiteCode", ""),
                "site_name": site.get("@SiteName", ""),
                "borough_code": auth.get("@LocalAuthorityCode", ""),
                "borough_name": auth.get("@LocalAuthorityName", ""),
                "latitude": site.get("@Latitude"),
                "longitude": site.get("@Longitude"),
            })
    return events


def _normalize_openmeteo(payload: dict[str, Any]) -> list[dict[str, object]]:
    current = payload.get("current", {}) or {}
    return [{
        "event_id": str(uuid.uuid4()),
        "source": "openmeteo",
        "event_type": "air_quality_current",
        "event_time": str(current.get("time") or now_iso()),
        "latitude": payload.get("latitude"),
        "longitude": payload.get("longitude"),
        "pm10": current.get("pm10"),
        "pm2_5": current.get("pm2_5"),
        "european_aqi": current.get("european_aqi"),
        "us_aqi": current.get("us_aqi"),
    }]


def _normalize_openweather(payload: dict[str, Any]) -> dict[str, object]:
    coord = payload.get("coord", {}) or {}
    main = payload.get("main", {}) or {}
    wind = payload.get("wind", {}) or {}
    weather = (payload.get("weather") or [{}])[0] if payload.get("weather") else {}
    return {
        "event_id": str(uuid.uuid4()),
        "source": "openweather",
        "event_type": "current_weather",
        "event_time": now_iso(),
        "city_name": payload.get("name", ""),
        "latitude": coord.get("lat"),
        "longitude": coord.get("lon"),
        "temp": main.get("temp"),
        "humidity": main.get("humidity"),
        "wind_speed": wind.get("speed"),
        "weather_main": weather.get("main"),
        "weather_description": weather.get("description"),
    }


def _normalize_tfl(record: dict[str, Any]) -> list[dict[str, object]]:
    import uuid
    statuses = record.get("lineStatuses", []) or [{}]
    events = []
    for status in statuses:
        events.append({
            "event_id": str(uuid.uuid4()),
            "source": "tfl",
            "event_type": "line_status",
            "event_time": now_iso(),
            "line_id": record.get("id"),
            "line_name": record.get("name"),
            "status": status.get("statusSeverityDescription"),
            "severity": status.get("statusSeverity"),
        })
    return events


def _normalize_generic(record: dict[str, Any]) -> dict[str, object]:
    return {
        "event_id": str(record.get("id") or record.get("event_id") or uuid.uuid4()),
        "source": "api",
        "event_type": record.get("type") or record.get("event_type") or "unknown",
        "event_time": record.get("timestamp") or record.get("event_time") or now_iso(),
        "data": record,
    }


# ============================================================================
# Kafka Producer
# ============================================================================

def create_kafka_producer(
    config: KafkaConfig | None = None,
    producer_config: ProducerConfig | None = None,
):
    """Create a Kafka producer with the given configuration."""
    try:
        from kafka import KafkaProducer as _KafkaProducer
    except ImportError:
        raise ImportError(
            "kafka-python is required for Kafka streaming. "
            "Install with: pip install kafka-python"
        )

    kafka_config = (config or KafkaConfig()).to_kafka_config()
    prod_config = producer_config or ProducerConfig()

    return _KafkaProducer(
        **kafka_config,
        acks=prod_config.acks,
        retries=prod_config.retries,
        retry_backoff_ms=prod_config.retry_backoff_ms,
        max_in_flight_requests_per_connection=prod_config.max_in_flight_requests_per_connection,
        compression_type=prod_config.compression_type,
        linger_ms=prod_config.linger_ms,
        batch_size=prod_config.batch_size,
        max_block_ms=prod_config.max_block_ms,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def produce_events(
    producer,
    topic: str,
    events: list[dict[str, object]],
    key: str | None = None,
) -> int:
    """Produce a batch of events to a Kafka topic."""
    from ingestion.base.core import SourceFailure

    sent = 0
    for event in events:
        event_key = key or event.get("source") or event.get("event_id")
        try:
            future = producer.send(topic, value=event, key=event_key)
            future.get(timeout=10)
            sent += 1
        except Exception as exc:
            print(f"Failed to produce event {event.get('event_id')}: {exc}")
            raise SourceFailure(f"Kafka produce failed: {exc}") from exc
    return sent


def publish_to_dlq(
    producer,
    event: dict[str, object],
    *,
    original_topic: str,
    source: str,
    error: str,
    attempts: int,
    dlq_topic: str = DLQ_TOPIC,
) -> None:
    """Publish failed event to DLQ topic."""
    from governance.dlq import record_dlq_event

    dlq_payload = {
        "original_topic": original_topic,
        "source": source,
        "error": error,
        "attempts": attempts,
        "event": event,
        "failed_at": now_iso(),
    }

    try:
        producer.send(dlq_topic, value=dlq_payload)
        producer.flush(timeout=5)
    except Exception as dlq_exc:
        print(f"Failed to publish to DLQ {dlq_topic}: {dlq_exc}")

    record_dlq_event(
        category="streaming_publish_failed",
        payload=event,
        source=source,
        error=error,
        error_type="KafkaProduceError",
        attempts=attempts,
        topic=original_topic,
    )


# ============================================================================
# Main Producer Runner
# ============================================================================

@dataclass
class ProducerResult:
    """Result of a producer run."""
    attempted: int = 0
    produced: int = 0
    dlq: int = 0
    events: list[dict[str, object]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def run_producer(
    source: str,
    topic: str,
    events: int,
    delay_seconds: float = 0.5,
    api_url: str | None = None,
    api_key: str | None = None,
    auth_header: str = "Authorization",
    bootstrap_servers: str | None = None,
    max_retries: int = 3,
    retry_backoff: float = 0.5,
    use_simulated: bool = False,
) -> ProducerResult:
    """Run the producer for a given source.

    Args:
        source: Source identifier (openaq, waqi, tfl, etc.)
        topic: Kafka topic to publish to
        events: Number of events to produce
        delay_seconds: Delay between events
        api_url: API URL (if using real API)
        api_key: API key (if required)
        auth_header: Auth header name
        bootstrap_servers: Kafka bootstrap servers
        max_retries: Max retries per event
        retry_backoff: Backoff between retries
        use_simulated: Use simulated events instead of real API

    Returns:
        ProducerResult with counts
    """
    result = ProducerResult()

    # Get events
    if use_simulated:
        result.events = sim_event(source, events)
    else:
        result.events = fetch_api_events(source, api_url, api_key, auth_header, events)
        if not result.events:
            print(f"No events from API {api_url}, falling back to simulated")
            result.events = sim_event(source, events)

    if not result.events:
        result.errors.append("No events available")
        return result

    # Create producer
    from ingestion.streaming.kafka_config import KafkaConfig
    kafka_config = KafkaConfig(
        bootstrap_servers=bootstrap_servers or KafkaConfig().bootstrap_servers
    )
    producer = create_kafka_producer(kafka_config)

    try:
        for event in result.events:
            result.attempted += 1
            last_error = None

            for attempt in range(1, max_retries + 1):
                try:
                    produce_events(producer, topic, [event])
                    result.produced += 1
                    print(f"Produced: {event.get('event_id')} source={source} topic={topic}")
                    last_error = None
                    break
                except Exception as exc:
                    last_error = exc
                    wait = retry_backoff * (2 ** (attempt - 1))
                    print(f"Retry {attempt}/{max_retries} for {event.get('event_id')}: {exc}")
                    time.sleep(wait)

            if last_error:
                result.dlq += 1
                publish_to_dlq(
                    producer,
                    event,
                    original_topic=topic,
                    source=source,
                    error=str(last_error),
                    attempts=max_retries,
                )

            time.sleep(delay_seconds)
    finally:
        producer.flush()
        producer.close()

    return result


# ============================================================================
# CLI Entry Point
# ============================================================================

def parse_args():
    """Parse command line arguments."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Produce domain streaming events into Kafka."
    )
    parser.add_argument(
        "--source",
        choices=sorted(STREAM_TOPICS.keys()),
        default=os.getenv("NEXUS_STREAM_SOURCE", "transport"),
        help="Data source to stream from",
    )
    parser.add_argument(
        "--topic",
        help=f"Kafka topic (default: from STREAM_TOPICS config)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--events",
        type=int,
        default=25,
        help="Number of events to produce",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.5,
        help="Delay between events",
    )
    parser.add_argument(
        "--api-url",
        help="Override API URL",
    )
    parser.add_argument(
        "--api-key",
        help="Override API key",
    )
    parser.add_argument(
        "--simulated",
        action="store_true",
        help="Use simulated events instead of real API",
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    # Get source config
    source_config = STREAM_TOPICS.get(args.source)
    topic = args.topic or (source_config.topic if source_config else None)
    if not topic:
        print(f"Error: No topic specified for source {args.source}")
        return 1

    api_url = args.api_url or (source_config.api_url if source_config else None)
    api_key = args.api_key or (source_config.api_key if source_config else None)
    auth_header = source_config.auth_header if source_config else "Authorization"

    print(f"Starting producer: source={args.source} topic={topic} events={args.events}")
    print(f"API URL: {api_url or 'N/A'}")
    print(f"Bootstrap: {args.bootstrap_servers}")

    result = run_producer(
        source=args.source,
        topic=topic,
        events=args.events,
        delay_seconds=args.delay_seconds,
        api_url=api_url,
        api_key=api_key,
        auth_header=auth_header,
        bootstrap_servers=args.bootstrap_servers,
        use_simulated=args.simulated,
    )

    print(f"\nResult: attempted={result.attempted} produced={result.produced} dlq={result.dlq}")
    if result.errors:
        print(f"Errors: {result.errors}")

    return 0 if result.produced > 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
