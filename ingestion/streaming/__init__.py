"""
Streaming Ingestion Pipeline for NEXUS.

Provides Kafka streaming capabilities:
- Producer for publishing events to Kafka topics
- Consumer for consuming events from Kafka topics
- API Stream for polling REST APIs and publishing to Kafka
- GTFS Realtime for ingesting transit feed data

Usage:
    # Producer
    from ingestion.streaming import run_producer, STREAM_TOPICS
    run_producer(source="openaq", topic="environment-openaq", events=10)

    # Consumer
    from ingestion.streaming import consume_events
    result = consume_events(topic="transport-events", dataset="transport")

    # API Stream
    from ingestion.streaming import run_api_stream
    result = run_api_stream(source="weather", interval=60)

    # GTFS Realtime
    from ingestion.streaming import run_gtfs_stream
    result = run_gtfs_stream(feed_url="https://example.com/gtfs-rt.pb", interval=30)

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS - Kafka broker address (default: localhost:29092)
    KAFKA_SECURITY_PROTOCOL - Security protocol (PLAINTEXT, SASL_SSL, etc.)
    KAFKA_SASL_USERNAME     - SASL username (if using SASL)
    KAFKA_SASL_PASSWORD     - SASL password (if using SASL)
    NEXUS_CONSUMER_GROUP    - Consumer group ID (default: nexus-streaming)
    NEXUS_DLQ_TOPIC         - DLQ topic name (default: nexus.dlq)

For production use, configure Apache Kafka and set the appropriate environment variables.
"""

# Kafka configuration
from ingestion.streaming.kafka_config import (
    KafkaConfig,
    ProducerConfig,
    ConsumerConfig,
    StreamSourceConfig,
    STREAM_TOPICS,
    DLQ_TOPIC,
)

# Producer functions
from ingestion.streaming.producer import (
    # Simulated event generators
    sim_transport,
    sim_env,
    sim_event,
    # Event normalization
    fetch_api_events,
    # Kafka producer
    create_kafka_producer,
    produce_events,
    publish_to_dlq,
    # Main runner
    run_producer,
    ProducerResult,
    # CLI
    parse_args as producer_parse_args,
    main as producer_main,
)

# Consumer functions
from ingestion.streaming.consumer import (
    # Kafka consumer
    create_kafka_consumer,
    # Event processing
    decode_event,
    write_events_to_raw,
    # Main consumer
    consume_to_raw,
    consume_events,
    consume_batch,
    ConsumerResult,
    BatchConsumerResult,
    # CLI
    parse_args as consumer_parse_args,
    main as consumer_main,
)

# API Stream functions
from ingestion.streaming.api_stream import (
    ApiStreamConfig,
    ApiStreamResult,
    fetch_api_records,
    poll_api_stream,
    run_api_stream,
)

# GTFS Realtime functions
from ingestion.streaming.gtfs_realtime import (
    GTFSRealtimeConfig,
    GTFSRealtimeResult,
    parse_gtfs_feed,
    fetch_gtfs_feed,
    poll_gtfs_stream,
    run_gtfs_stream,
)

# Backward compatibility
TOPICS = {k: v.topic for k, v in STREAM_TOPICS.items()}


__all__ = [
    # Configuration
    "KafkaConfig",
    "ProducerConfig",
    "ConsumerConfig",
    "StreamSourceConfig",
    "STREAM_TOPICS",
    "DLQ_TOPIC",
    "TOPICS",
    # Producer
    "sim_transport",
    "sim_env",
    "sim_event",
    "fetch_api_events",
    "create_kafka_producer",
    "produce_events",
    "publish_to_dlq",
    "run_producer",
    "ProducerResult",
    "producer_parse_args",
    "producer_main",
    # Consumer
    "create_kafka_consumer",
    "decode_event",
    "write_events_to_raw",
    "consume_to_raw",
    "consume_events",
    "consume_batch",
    "ConsumerResult",
    "BatchConsumerResult",
    "consumer_parse_args",
    "consumer_main",
    # API Stream
    "ApiStreamConfig",
    "ApiStreamResult",
    "fetch_api_records",
    "poll_api_stream",
    "run_api_stream",
    # GTFS Realtime
    "GTFSRealtimeConfig",
    "GTFSRealtimeResult",
    "parse_gtfs_feed",
    "fetch_gtfs_feed",
    "poll_gtfs_stream",
    "run_gtfs_stream",
]
