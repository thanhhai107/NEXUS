"""
Kafka Consumer for NEXUS Streaming.

Consumes events from Kafka topics and lands them into the raw layer.

Usage:
    python -m ingestion.streaming.consumer --topic transport-events --dataset transport

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS - Kafka broker address (default: localhost:29092)
    NEXUS_CONSUMER_GROUP    - Consumer group ID (default: nexus-streaming)
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ingestion.streaming.kafka_config import (
    ConsumerConfig,
    DLQ_TOPIC,
    KafkaConfig,
)

# Default values
DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
DEFAULT_GROUP_ID = os.getenv("NEXUS_CONSUMER_GROUP", "nexus-streaming")


# ============================================================================
# Kafka Consumer
# ============================================================================

def create_kafka_consumer(
    topics: list[str] | str,
    config: KafkaConfig | None = None,
    consumer_config: ConsumerConfig | None = None,
) -> Any:
    """Create a Kafka consumer with the given configuration.

    Args:
        topics: Topic(s) to subscribe to
        config: Kafka connection config
        consumer_config: Consumer behavior config

    Returns:
        KafkaConsumer instance
    """
    try:
        from kafka import KafkaConsumer as _KafkaConsumer
    except ImportError:
        raise ImportError(
            "kafka-python is required for Kafka streaming. "
            "Install with: pip install kafka-python"
        )

    kafka_config = (config or KafkaConfig()).to_kafka_config()
    cons_config = consumer_config or ConsumerConfig()

    consumer = _KafkaConsumer(
        *([topics] if isinstance(topics, str) else topics),
        **kafka_config,
        group_id=cons_config.group_id,
        auto_offset_reset=cons_config.auto_offset_reset,
        enable_auto_commit=cons_config.enable_auto_commit,
        auto_commit_interval_ms=cons_config.auto_commit_interval_ms,
        max_poll_records=cons_config.max_poll_records,
        max_poll_interval_ms=cons_config.max_poll_interval_ms,
        session_timeout_ms=cons_config.session_timeout_ms,
        heartbeat_interval_ms=cons_config.heartbeat_interval_ms,
        fetch_min_bytes=cons_config.fetch_min_bytes,
        fetch_max_wait_ms=cons_config.fetch_max_wait_ms,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    return consumer


# ============================================================================
# Event Processing
# ============================================================================

def decode_event(value: bytes | str | Any) -> dict[str, Any]:
    """Decode and validate a Kafka event payload."""
    if isinstance(value, dict):
        return value

    try:
        if isinstance(value, bytes):
            return json.loads(value.decode("utf-8"))
        if isinstance(value, str):
            return json.loads(value)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Invalid Kafka payload: {exc}")

    raise ValueError(f"Unsupported payload type: {type(value)}")


# ============================================================================
# Raw Layer Writer
# ============================================================================

def write_events_to_raw(
    events: list[dict[str, Any]],
    dataset: str,
    source: str,
) -> Path:
    """Write events to the raw landing zone using canonical format."""
    from ingestion.canonical.envelope import EnvelopeContext, build_raw_envelope
    from ingestion.canonical.writer import default_raw_path, write_raw_envelopes
    from common.config import RUNTIME_DIR

    LOCAL_RAW_DIR = RUNTIME_DIR / "raw"
    context = EnvelopeContext(
        dataset_id=dataset,
        source_id=source,
        ingestion_type="streaming",
        source_key=source,
    )

    output_path = default_raw_path(dataset, LOCAL_RAW_DIR, prefix="streaming")
    return write_raw_envelopes(
        events,
        context,
        output_path=output_path,
        normalize_payload=True,
    )


# ============================================================================
# Consumer Result
# ============================================================================

@dataclass
class ConsumerResult:
    """Result of a consumer run."""
    consumed: int = 0
    landed: int = 0
    dlq: int = 0
    raw_path: str | None = None
    errors: list[str] = field(default_factory=list)


# ============================================================================
# Main Consumer Functions
# ============================================================================

def consume_events(
    topic: str,
    dataset: str,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP,
    group_id: str = DEFAULT_GROUP_ID,
    max_messages: int = 100,
    consume_timeout_ms: int = 10_000,
    auto_offset_reset: str = "earliest",
    write_to_raw: bool = True,
) -> ConsumerResult:
    """Consume events from a Kafka topic and optionally write to raw layer.

    Args:
        topic: Kafka topic to consume from
        dataset: Dataset name for raw layer
        bootstrap_servers: Kafka bootstrap servers
        group_id: Consumer group ID
        max_messages: Maximum messages to consume
        consume_timeout_ms: Consumer timeout
        auto_offset_reset: Where to start if no offset
        write_to_raw: Whether to write to raw layer

    Returns:
        ConsumerResult with counts
    """
    result = ConsumerResult()

    # Setup configs
    kafka_config = KafkaConfig(bootstrap_servers=bootstrap_servers)
    consumer_config = ConsumerConfig(
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
    )

    # Create consumer
    consumer = create_kafka_consumer(
        topics=topic,
        config=kafka_config,
        consumer_config=consumer_config,
    )

    landed_records: list[dict[str, Any]] = []

    try:
        for message in consumer:
            result.consumed += 1

            try:
                event = decode_event(message.value)

                if not isinstance(event, dict):
                    raise ValueError(f"Event is not a dict: {type(event)}")

                # Add metadata from Kafka
                event["_kafka_topic"] = message.topic
                event["_kafka_partition"] = message.partition
                event["_kafka_offset"] = message.offset
                event["_kafka_timestamp"] = message.timestamp

                landed_records.append(event)
                result.landed += 1

            except Exception as exc:
                result.dlq += 1
                _route_to_dlq(
                    raw_payload=message.value,
                    source=topic,
                    dataset=dataset,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )

            if result.consumed >= max_messages:
                break

    finally:
        # Commit offsets
        try:
            consumer.commit()
        except Exception:
            pass  # Best effort
        consumer.close()

    # Write to raw layer
    if write_to_raw and landed_records:
        try:
            raw_path = write_events_to_raw(landed_records, dataset, f"kafka://{topic}")
            result.raw_path = str(raw_path)
        except Exception as exc:
            result.errors.append(f"Failed to write to raw: {exc}")

    return result


def _route_to_dlq(
    raw_payload: bytes,
    source: str,
    dataset: str,
    error: str,
    error_type: str,
    topic: str = DLQ_TOPIC,
) -> None:
    """Route failed messages to DLQ."""
    from governance.dlq import record_dlq_event

    try:
        raw_text = raw_payload.decode("utf-8", errors="replace") if isinstance(raw_payload, bytes) else str(raw_payload)
    except Exception:
        raw_text = "<decode_failed>"

    record_dlq_event(
        category="streaming_consume_failed",
        payload={"raw": raw_text},
        source=source,
        error=error,
        error_type=error_type,
        topic=topic,
        dataset=dataset,
    )


# ============================================================================
# Batch Consumer (Process Multiple Topics)
# ============================================================================

@dataclass
class BatchConsumerResult:
    """Result of batch consumer run."""
    topics: dict[str, ConsumerResult] = field(default_factory=dict)

    @property
    def total_consumed(self) -> int:
        return sum(r.consumed for r in self.topics.values())

    @property
    def total_landed(self) -> int:
        return sum(r.landed for r in self.topics.values())

    @property
    def total_dlq(self) -> int:
        return sum(r.dlq for r in self.topics.values())


def consume_batch(
    topic_configs: list[tuple[str, str]],
    bootstrap_servers: str = DEFAULT_BOOTSTRAP,
    group_id: str = DEFAULT_GROUP_ID,
    max_messages_per_topic: int = 100,
) -> BatchConsumerResult:
    """Consume from multiple topics in parallel.

    Args:
        topic_configs: List of (topic, dataset) tuples
        bootstrap_servers: Kafka bootstrap servers
        group_id: Consumer group ID
        max_messages_per_topic: Max messages per topic

    Returns:
        BatchConsumerResult with per-topic results
    """
    result = BatchConsumerResult()

    for topic, dataset in topic_configs:
        print(f"Consuming from topic={topic} dataset={dataset}")
        topic_result = consume_events(
            topic=topic,
            dataset=dataset,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            max_messages=max_messages_per_topic,
        )
        result.topics[topic] = topic_result
        print(f"  consumed={topic_result.consumed} landed={topic_result.landed} dlq={topic_result.dlq}")

    return result


# ============================================================================
# CLI Entry Point
# ============================================================================

def parse_args():
    """Parse command line arguments."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Consume Kafka events into the Nexus raw layer."
    )
    parser.add_argument(
        "--topic",
        required=True,
        help="Kafka topic to consume from",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="Dataset name for raw layer",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP,
        help=f"Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP})",
    )
    parser.add_argument(
        "--group-id",
        default=DEFAULT_GROUP_ID,
        help=f"Consumer group ID (default: {DEFAULT_GROUP_ID})",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=100,
        help="Maximum messages to consume",
    )
    parser.add_argument(
        "--consume-timeout-ms",
        type=int,
        default=10_000,
        help="Consumer timeout in milliseconds",
    )
    parser.add_argument(
        "--auto-offset-reset",
        default="earliest",
        choices=["earliest", "latest"],
        help="Where to start if no offset",
    )
    parser.add_argument(
        "--no-raw",
        action="store_true",
        help="Don't write to raw layer",
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    print(f"Starting consumer: topic={args.topic} dataset={args.dataset}")
    print(f"Bootstrap: {args.bootstrap_servers}")
    print(f"Group ID: {args.group_id}")

    result = consume_events(
        topic=args.topic,
        dataset=args.dataset,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        max_messages=args.max_messages,
        consume_timeout_ms=args.consume_timeout_ms,
        auto_offset_reset=args.auto_offset_reset,
        write_to_raw=not args.no_raw,
    )

    print(f"\nResult:")
    print(f"  consumed: {result.consumed}")
    print(f"  landed:   {result.landed}")
    print(f"  dlq:      {result.dlq}")
    print(f"  raw_path: {result.raw_path or 'N/A'}")

    if result.errors:
        print(f"  errors:   {result.errors}")

    return 0 if result.landed > 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
