"""Kafka Hook for Airflow.

Provides hooks for Kafka producer and consumer operations.
"""

from __future__ import annotations

import json
import os
from typing import Any

from airflow.hooks.base import BaseHook


class KafkaProducerHook(BaseHook):
    """Hook for producing messages to Kafka.
    
    Usage:
        hook = KafkaProducerHook(bootstrap_servers="kafka:9092")
        hook.produce(
            topic="my-topic",
            messages=[{"key": "value"}],
        )
    """

    conn_name_attr = "kafka_conn_id"
    default_conn_name = "kafka_default"

    def __init__(
        self,
        kafka_conn_id: str | None = None,
        bootstrap_servers: str | None = None,
    ):
        """Initialize the Kafka producer hook.
        
        Args:
            kafka_conn_id: Airflow connection ID for Kafka
            bootstrap_servers: Kafka bootstrap servers (overrides connection)
        """
        super().__init__(kafka_conn_id=kafka_conn_id)
        
        if bootstrap_servers:
            self.bootstrap_servers = bootstrap_servers
        else:
            conn = self.get_connection(kafka_conn_id or self.default_conn_name)
            self.bootstrap_servers = conn.host or "localhost:9092"

    def produce(
        self,
        topic: str,
        messages: list[dict[str, Any]],
        key: str | None = None,
    ) -> dict[str, Any]:
        """Produce messages to a Kafka topic.
        
        Args:
            topic: Kafka topic name
            messages: List of message dicts to produce
            key: Optional message key
        
        Returns:
            Dict with production results
        """
        try:
            from confluent_kafka import Producer
        except ImportError:
            return {
                "success": False,
                "error": "confluent-kafka not installed. Run: pip install confluent-kafka",
            }

        producer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "airflow-producer",
        }

        producer = Producer(producer_config)
        delivered = {"count": 0, "errors": []}

        def delivery_callback(err, msg):
            if err:
                delivered["errors"].append(str(err))
            else:
                delivered["count"] += 1

        for msg in messages:
            value = json.dumps(msg).encode("utf-8")
            producer.produce(
                topic=topic,
                value=value,
                key=key.encode("utf-8") if key else None,
                callback=delivery_callback,
            )

        producer.flush()

        return {
            "success": len(delivered["errors"]) == 0,
            "produced": delivered["count"],
            "errors": delivered["errors"],
        }


class KafkaConsumerHook(BaseHook):
    """Hook for consuming messages from Kafka.
    
    Usage:
        hook = KafkaConsumerHook(bootstrap_servers="kafka:9092")
        messages = hook.consume(
            topic="my-topic",
            max_messages=100,
        )
    """

    conn_name_attr = "kafka_conn_id"
    default_conn_name = "kafka_default"

    def __init__(
        self,
        kafka_conn_id: str | None = None,
        bootstrap_servers: str | None = None,
        group_id: str = "airflow-consumer",
    ):
        """Initialize the Kafka consumer hook.
        
        Args:
            kafka_conn_id: Airflow connection ID for Kafka
            bootstrap_servers: Kafka bootstrap servers (overrides connection)
            group_id: Consumer group ID
        """
        super().__init__(kafka_conn_id=kafka_conn_id)
        
        if bootstrap_servers:
            self.bootstrap_servers = bootstrap_servers
        else:
            conn = self.get_connection(kafka_conn_id or self.default_conn_name)
            self.bootstrap_servers = conn.host or "localhost:9092"
        
        self.group_id = group_id

    def consume(
        self,
        topic: str,
        max_messages: int = 100,
        timeout_ms: int = 10000,
        auto_offset_reset: str = "earliest",
    ) -> dict[str, Any]:
        """Consume messages from a Kafka topic.
        
        Args:
            topic: Kafka topic name
            max_messages: Maximum messages to consume
            timeout_ms: Consumer timeout in milliseconds
            auto_offset_reset: Where to start if no offset ('earliest' or 'latest')
        
        Returns:
            Dict with consumed messages and metadata
        """
        try:
            from confluent_kafka import Consumer, KafkaError
        except ImportError:
            return {
                "success": False,
                "error": "confluent-kafka not installed. Run: pip install confluent-kafka",
                "messages": [],
            }

        consumer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,
        }

        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])

        messages = []
        errors = []
        start_time = __import__("time").time() * 1000

        try:
            while len(messages) < max_messages:
                elapsed = (__import__("time").time() * 1000) - start_time
                remaining = max(0, timeout_ms - elapsed)
                
                if remaining <= 0:
                    break

                msg = consumer.poll(timeout=min(remaining / 1000, 1.0))
                
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    errors.append(str(msg.error()))
                    continue

                messages.append({
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "timestamp": msg.timestamp()[1] if msg.timestamp() else None,
                    "key": msg.key().decode("utf-8") if msg.key() else None,
                    "value": json.loads(msg.value().decode("utf-8")),
                })
        finally:
            consumer.commit()
            consumer.close()

        return {
            "success": True,
            "consumed": len(messages),
            "errors": errors,
            "messages": messages,
        }
