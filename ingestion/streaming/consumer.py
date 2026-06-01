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
import time
from dataclasses import dataclass, field
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
        from confluent_kafka import Consumer
    except ImportError:
        raise ImportError(
            "confluent-kafka is required for Kafka streaming. "
            "Install with: pip install confluent-kafka"
        )

    kafka_config = (config or KafkaConfig()).to_kafka_config()
    cons_config = consumer_config or ConsumerConfig()

    # Build confluent-kafka config
    c_config = {
        "bootstrap.servers": kafka_config.get("bootstrap_servers", "localhost:29092"),
        "group.id": cons_config.group_id,
        "auto.offset.reset": cons_config.auto_offset_reset,
        "enable.auto.commit": cons_config.enable_auto_commit,
        "auto.commit.interval.ms": cons_config.auto_commit_interval_ms,
        "session.timeout.ms": cons_config.session_timeout_ms,
        "heartbeat.interval.ms": cons_config.heartbeat_interval_ms,
    }

    consumer = Consumer(c_config)
    
    # Subscribe to topics
    topic_list = [topics] if isinstance(topics, str) else topics
    consumer.subscribe(topic_list)

    return consumer


class KafkaMessage:
    """Wrapper to match kafka-python message interface."""
    def __init__(self, msg):
        self._msg = msg
        self.topic = msg.topic()
        self.partition = msg.partition()
        self.offset = msg.offset()
        self.timestamp = msg.timestamp()[1] if msg.timestamp() else None
        self.value = json.loads(msg.value().decode("utf-8")) if msg.value() else {}
        self.key = msg.key().decode("utf-8") if msg.key() else None


def consume_kafka_messages(consumer, max_messages: int = 100, timeout_ms: int = 10000):
    """Generator that yields messages from confluent-kafka consumer."""
    from confluent_kafka import KafkaError

    messages = []
    start_time = time.time() * 1000
    
    while len(messages) < max_messages:
        elapsed = (time.time() * 1000) - start_time
        remaining = max(0, timeout_ms - elapsed)
        
        msg = consumer.poll(timeout=min(remaining / 1000, 1.0))
        
        if msg is None:
            if elapsed >= timeout_ms:
                break
            continue
            
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise Exception(f"Kafka error: {msg.error()}")
            
        messages.append(KafkaMessage(msg))
        
        if elapsed >= timeout_ms:
            break
    
    return messages


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
    run_id: str | None = None,
) -> Path:
    """Write events to the bronze layer using canonical format.
    
    Args:
        events: List of events to write
        dataset: Dataset name
        source: Source identifier
        run_id: Optional run_id, defaults to streaming_{timestamp}
    
    Returns:
        Path to the written file
    """
    from ingestion.canonical.envelope import EnvelopeContext
    from ingestion.canonical.writer import write_raw_envelopes
    from common.config import BRONZE_DIR
    from datetime import datetime, timezone
    import uuid

    # Generate run_id if not provided
    if run_id is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_id = f"streaming_{timestamp}_{uuid.uuid4().hex[:8]}"

    # Bronze path: runtime/lake/bronze/{dataset}/run_id={run_id}/raw/streaming.jsonl
    bronze_base = BRONZE_DIR / dataset / f"run_id={run_id}"
    bronze_base.mkdir(parents=True, exist_ok=True)
    output_path = bronze_base / "raw" / "streaming.jsonl"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    context = EnvelopeContext(
        dataset_id=dataset,
        source_id=source,
        ingestion_type="streaming",
        source_key=source,
        run_id=run_id,
    )

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

def consume_to_raw(
    topic: str,
    dataset: str,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP,
    group_id: str = DEFAULT_GROUP_ID,
    max_messages: int = 100,
) -> dict[str, Any]:
    """Backward-compatible path delegating to consume_events (confluent-kafka)."""
    result = consume_events(
        topic=topic,
        dataset=dataset,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        max_messages=max_messages,
        consume_timeout_ms=15_000,
        write_to_raw=True,
    )
    return {
        "consumed": result.consumed,
        "landed": result.landed,
        "dlq": result.dlq,
        "raw_path": result.raw_path,
    }


def _validate_streaming_records(
    records: list[dict[str, Any]],
    dataset: str,
    topic: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str, dict[str, Any]]:
    """Run quality gate on streaming records before writing to raw.

    Returns (valid_records, invalid_records, status, details).
    Records that fail quality are NOT written to raw.
    """
    try:
        from common.config import load_quality_config, load_dataset_catalog
        from governance.quality.auto_fix import apply_auto_fix, normalize_field_name, normalize_field_names
        from governance.quality.schema import coerce_records_to_schema, records_failing_json_schema
        from governance.quality.checks import run_quality_checks, evaluate_quality_status
        from governance.schema_drift import compare_schema_drift
        from common.data_contract import load_data_contract

        quality_config = load_quality_config()
        rules = quality_config.get("datasets", {}).get(dataset)
        if not rules:
            return records, [], "passed", {"warning": "no quality rules configured for streaming dataset"}

        dataset_config = load_dataset_catalog().get("datasets", {}).get(dataset, {})
        auto_fix_result = apply_auto_fix(records, rules.get("auto_fix"))
        schema = _load_schema_for_stream(dataset_config, rules)
        coercion = coerce_records_to_schema(auto_fix_result.records, schema)
        checked = coercion.records

        required = normalize_field_names(rules["required_columns"], rules.get("auto_fix"))
        primary = normalize_field_names(dataset_config.get("primary_keys", ["event_id"]), rules.get("auto_fix"))
        freshness = normalize_field_name(rules["freshness_column"], rules.get("auto_fix"))

        quality = run_quality_checks(
            dataset=dataset,
            records=checked,
            required_columns=required,
            primary_keys=primary,
            freshness_column=freshness,
            max_age_hours=int(dataset_config.get("freshness_hours", 1)),
            json_schema=schema,
        )

        thresholds = dict(quality_config.get("default_rules", {}))
        status, violations = evaluate_quality_status(quality, thresholds)

        invalid: list[dict[str, Any]] = [
            rec for rec in checked
            if any(rec.get(col) in (None, "") for col in required)
        ]
        invalid.extend(records_failing_json_schema(checked, schema))

        valid = [rec for rec in checked if rec not in invalid]
        return valid, invalid, status, {
            "record_count": quality.record_count,
            "missing_ratio": quality.missing_ratio,
            "duplicate_ratio": quality.duplicate_ratio,
            "freshness_score": quality.freshness_score,
            "schema_valid": quality.schema_valid,
            "threshold_violations": violations,
            "invalid_count": len(invalid),
        }
    except Exception as exc:
        return records, [], "skipped", {"validation_error": str(exc)}


def _load_schema_for_stream(dataset_config: dict, rules: dict) -> dict | None:
    try:
        from governance.quality.schema import normalize_json_schema
        from cli.nexus import load_schema
        return normalize_json_schema(load_schema(dataset_config.get("schema_path")), rules.get("auto_fix"))
    except Exception:
        return None


def consume_events(
    topic: str,
    dataset: str,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP,
    group_id: str = DEFAULT_GROUP_ID,
    max_messages: int = 100,
    consume_timeout_ms: int = 10_000,
    auto_offset_reset: str = "earliest",
    write_to_raw: bool = True,
    validate_quality: bool = False,
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
        validate_quality: Run quality gate before writing to raw.
            Invalid records are routed to quarantine, not written.

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
        # Use confluent-kafka polling pattern
        for message in consume_kafka_messages(consumer, max_messages, consume_timeout_ms):
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
        # Commit offsets (confluent-kafka)
        try:
            consumer.commit(asynchronous=False)
        except Exception as exc:
            print(f"Warning: consumer async commit failed: {exc}")
        consumer.close()

    # Write to raw layer (with optional quality gate)
    if write_to_raw and landed_records:
        records_to_write = landed_records

        if validate_quality:
            valid_records, invalid_records, q_status, q_details = _validate_streaming_records(
                landed_records, dataset, topic,
            )
            records_to_write = valid_records
            if invalid_records:
                try:
                    from governance.quality.quarantine import quarantine_records
                    quarantine_records(
                        dataset, invalid_records,
                        reason="streaming_quality_validation_failed",
                        source_path=f"kafka://{topic}",
                    )
                except Exception as exc:
                    result.errors.append(f"Quarantine failed: {exc}")
            if q_status == "failed":
                result.errors.append(
                    f"Streaming quality gate failed: {q_details.get('threshold_violations', [])}"
                )

        if records_to_write:
            try:
                raw_path = write_events_to_raw(records_to_write, dataset, f"kafka://{topic}")
                result.raw_path = str(raw_path)
                _publish_streaming_raw_envelope(raw_path, dataset)
            except Exception as exc:
                result.errors.append(f"Failed to write to raw: {exc}")

    return result


def _publish_streaming_raw_envelope(raw_path: Path, dataset: str) -> str | None:
    """Publish a streaming artifact to the shared raw envelope zone."""
    try:
        from ingestion.downloaders.raw_adapter import published_run_to_raw_envelope
        
        # Create a minimal published manifest for streaming data
        import json
        from datetime import datetime, timezone
        import hashlib
        
        run_path = raw_path.parents[1]  # bronze/{dataset}/run_id={run_id}
        run_id = run_path.name.replace("run_id=", "")
        
        # Generate checksum for the raw file
        checksum = hashlib.sha256(raw_path.read_bytes()).hexdigest()
        
        # Create published manifest
        published_manifest = run_path / "published" / "published_manifest.json"
        published_manifest.parent.mkdir(parents=True, exist_ok=True)
        
        manifest_data = {
            "source_id": dataset,
            "dataset_name": dataset,
            "run_id": run_id,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "coverage_status": "complete",
            "publish_status": "published",
            "chunks": [{
                "chunk_id": "streaming",
                "status": "success",
                "required": True,
                "paths": [str(raw_path)],
                "checksums": {str(raw_path): checksum},
                "record_count": _count_jsonl_records(raw_path)
            }],
            "raw_dir": str(run_path / "raw"),
            "source_key": dataset,
            "downstream_raw_path": None,
            "raw_envelope_published_at": datetime.now(timezone.utc).isoformat()
        }
        
        published_manifest.write_text(json.dumps(manifest_data, indent=2, ensure_ascii=False), encoding="utf-8")
        
        # Convert the published artifact into the canonical raw envelope landing zone.
        result = published_run_to_raw_envelope(published_manifest)
        
        envelope_path = result.get("raw_path", "N/A")
        encoded = envelope_path.encode("ascii", "replace").decode("ascii") if envelope_path else "N/A"
        print(f"  raw_envelope: {encoded}")
        return result.get("raw_path")
        
    except Exception as exc:
        print(f"  raw envelope publish failed: {exc}")
        return None


def _count_jsonl_records(path: Path) -> int:
    with path.open("r", encoding="utf-8") as file:
        return sum(1 for line in file if line.strip())


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

    print("\nResult:")
    print(f"  consumed: {result.consumed}")
    print(f"  landed:   {result.landed}")
    print(f"  dlq:      {result.dlq}")
    encoded_path = result.raw_path.encode('ascii', 'replace').decode('ascii') if result.raw_path else 'N/A'
    print(f"  raw_path: {encoded_path}")

    if result.errors:
        print(f"  errors:   {result.errors}")

    return 0 if result.landed > 0 else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
