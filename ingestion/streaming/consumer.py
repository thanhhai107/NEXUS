from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from common.config import RUNTIME_DIR
from governance.dlq import record_dlq_event
from ingestion.batch.common import write_jsonl

DEFAULT_GROUP_ID = os.getenv("NEXUS_CONSUMER_GROUP", "nexus-streaming")
DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")


def _decode(value: bytes) -> Any:
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid Kafka payload: {exc}")


def consume_to_raw(
    topic: str,
    dataset: str,
    *,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP,
    group_id: str = DEFAULT_GROUP_ID,
    max_messages: int = 100,
    consume_timeout_ms: int = 10_000,
    auto_offset_reset: str = "earliest",
) -> dict[str, Any]:
    """Consume up to ``max_messages`` events and land them into the raw layer.

    Returns a summary dict with `consumed`, `landed`, `dlq` and `raw_path`.
    Operational failures (decode errors, write failures) go to the DLQ; bad data
    records (failing schema/quality) are handled by the validation gate later.
    """
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,
        consumer_timeout_ms=consume_timeout_ms,
    )

    summary: dict[str, Any] = {"consumed": 0, "landed": 0, "dlq": 0, "raw_path": None}
    landed_records: list[dict[str, Any]] = []

    try:
        for message in consumer:
            summary["consumed"] += 1
            try:
                event = _decode(message.value)
                if not isinstance(event, dict):
                    raise ValueError("Kafka payload is not a JSON object")
                landed_records.append(event)
                summary["landed"] += 1
            except Exception as exc:  # noqa: BLE001 - operational failure
                summary["dlq"] += 1
                record_dlq_event(
                    category="streaming_consume_failed",
                    payload={"raw": message.value.decode("utf-8", errors="replace") if message.value else None},
                    source=topic,
                    error=str(exc),
                    error_type=type(exc).__name__,
                    topic=topic,
                    dataset=dataset,
                )
            if summary["consumed"] >= max_messages:
                break
    finally:
        try:
            consumer.commit()
        except Exception:  # noqa: BLE001 - best effort commit
            pass
        consumer.close()

    if landed_records:
        raw_path = write_jsonl(dataset=dataset, records=landed_records, source=f"kafka://{topic}")
        summary["raw_path"] = str(raw_path)

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Kafka events into the Nexus raw layer.")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--group-id", default=DEFAULT_GROUP_ID)
    parser.add_argument("--max-messages", type=int, default=100)
    parser.add_argument("--consume-timeout-ms", type=int, default=10_000)
    parser.add_argument("--auto-offset-reset", default="earliest")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    summary = consume_to_raw(
        topic=args.topic,
        dataset=args.dataset,
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        max_messages=args.max_messages,
        consume_timeout_ms=args.consume_timeout_ms,
        auto_offset_reset=args.auto_offset_reset,
    )
    print(json.dumps(summary, indent=2))