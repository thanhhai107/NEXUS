"""
Kafka Configuration and Utilities for NEXUS Streaming.

Provides shared Kafka configuration and utilities for both producer and consumer.
Reads configuration from environment variables (see .env for examples).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any


# Default NEXUS streaming topics (can be overridden via KAFKA_TOPIC_* env vars)
KAFKA_TOPIC_PREFIX = "KAFKA_TOPIC_"
TFL_LINE_IDS = (
    "bakerloo,central,circle,district,hammersmith-city,jubilee,metropolitan,"
    "northern,piccadilly,victoria,waterloo-city,dlr,elizabeth,liberty,lioness,"
    "mildmay,suffragette,weaver,windrush"
)
TFL_DEFAULT_LINE_STATUS_URL = f"https://api.tfl.gov.uk/Line/{TFL_LINE_IDS}/Status"
TFL_DEFAULT_ARRIVALS_STOP_ID = "940GZZLUKSX"

def _get_topic_env(source_key: str) -> str:
    """Get topic from environment variable KAFKA_TOPIC_{SOURCE_KEY}."""
    env_var = f"{KAFKA_TOPIC_PREFIX}{source_key.upper()}"
    return os.getenv(env_var, "")


@dataclass(frozen=True)
class KafkaConfig:
    """Kafka connection configuration.
    
    Reads defaults from environment variables:
    - KAFKA_BOOTSTRAP_SERVERS: Kafka broker address (default: localhost:29092)
    - KAFKA_SECURITY_PROTOCOL: Security protocol (default: PLAINTEXT)
    - KAFKA_SASL_MECHANISM: SASL mechanism (default: PLAIN)
    - KAFKA_SASL_USERNAME: SASL username
    - KAFKA_SASL_PASSWORD: SASL password
    - KAFKA_SSL_CAFILE: SSL CA certificate path
    - KAFKA_SSL_CERTFILE: SSL certificate path
    - KAFKA_SSL_KEYFILE: SSL key file path
    """

    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    )
    security_protocol: str = field(
        default_factory=lambda: os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    )
    sasl_mechanism: str = field(
        default_factory=lambda: os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
    )
    sasl_plain_username: str | None = field(
        default_factory=lambda: os.getenv("KAFKA_SASL_USERNAME")
    )
    sasl_plain_password: str | None = field(
        default_factory=lambda: os.getenv("KAFKA_SASL_PASSWORD")
    )
    ssl_cafile: str | None = field(
        default_factory=lambda: os.getenv("KAFKA_SSL_CAFILE")
    )
    ssl_certfile: str | None = field(
        default_factory=lambda: os.getenv("KAFKA_SSL_CERTFILE")
    )
    ssl_keyfile: str | None = field(
        default_factory=lambda: os.getenv("KAFKA_SSL_KEYFILE")
    )

    @property
    def is_secure(self) -> bool:
        return self.security_protocol in {"SASL_SSL", "SASL_PLAINTEXT", "SSL"}

    @property
    def has_sasl(self) -> bool:
        return self.security_protocol in {"SASL_SSL", "SASL_PLAINTEXT"}

    def to_kafka_config(self) -> dict[str, Any]:
        """Convert to kafka-python configuration dict."""
        config = {
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": self.security_protocol,
        }
        if self.has_sasl and self.sasl_plain_username and self.sasl_plain_password:
            config.update({
                "sasl_mechanism": self.sasl_mechanism,
                "sasl_plain_username": self.sasl_plain_username,
                "sasl_plain_password": self.sasl_plain_password,
            })
        if self.ssl_cafile:
            config["ssl_cafile"] = self.ssl_cafile
        if self.ssl_certfile:
            config["ssl_certfile"] = self.ssl_certfile
        if self.ssl_keyfile:
            config["ssl_keyfile"] = self.ssl_keyfile
        return config


@dataclass(frozen=True)
class ProducerConfig:
    """Kafka producer configuration."""

    acks: int = 1  # 0=none, 1=leader, -1=all
    retries: int = 3
    retry_backoff_ms: int = 100
    max_in_flight_requests_per_connection: int = 5
    compression_type: str = "gzip"
    linger_ms: int = 5
    batch_size: int = 16384
    max_block_ms: int = 1000
    enable_idempotence: bool = False


@dataclass(frozen=True)
class ConsumerConfig:
    """Kafka consumer configuration."""

    group_id: str = field(
        default_factory=lambda: os.getenv("NEXUS_CONSUMER_GROUP", "nexus-streaming")
    )
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    auto_commit_interval_ms: int = 5000
    max_poll_records: int = 100
    max_poll_interval_ms: int = 300000
    session_timeout_ms: int = 10000
    heartbeat_interval_ms: int = 3000
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500
    max_partition_fetch_bytes: int = 1048576  # 1MB


@dataclass(frozen=True)
class StreamSourceConfig:
    """Configuration for a streaming data source."""

    source_key: str
    topic: str
    api_url: str | None = None
    api_key: str | None = None
    auth_header: str = "Authorization"
    poll_interval_seconds: float = 60.0
    batch_size: int = 100
    enabled: bool = True


# Default NEXUS streaming topics
# Topics can be overridden via environment variables:
# KAFKA_TOPIC_TRANSPORT_EVENTS, KAFKA_TOPIC_TRANSPORT_TFL, etc.
STREAM_TOPICS = {
    "transport": StreamSourceConfig(
        source_key="transport",
        topic=os.getenv("KAFKA_TOPIC_TRANSPORT_EVENTS", "transport-events"),
        api_url=os.getenv("TRANSPORT_EVENTS_API_URL"),
        api_key=os.getenv("TRANSPORT_EVENTS_API_KEY"),
    ),
    "openaq": StreamSourceConfig(
        source_key="openaq",
        topic=os.getenv("KAFKA_TOPIC_OPENAQ", "environment-openaq"),
        api_url=os.getenv("OPENAQ_API_URL", "https://api.openaq.org/v3/measurements"),
        api_key=os.getenv("OPENAQ_API_KEY"),
        auth_header="X-API-Key",
    ),
    "waqi": StreamSourceConfig(
        source_key="waqi",
        topic=os.getenv("KAFKA_TOPIC_WAQI", "environment-waqi"),
        api_url=os.getenv("WAQI_API_URL"),
        api_key=os.getenv("WAQI_API_TOKEN"),
    ),
    "tfl": StreamSourceConfig(
        source_key="tfl",
        topic=os.getenv("KAFKA_TOPIC_TRANSPORT_TFL", "transport-tfl"),
        api_url=os.getenv("TFL_API_URL", TFL_DEFAULT_LINE_STATUS_URL),
        api_key=os.getenv("TFL_API_KEY"),
        auth_header="query-app_key",
        poll_interval_seconds=300.0,
    ),
    "tfl_line_status": StreamSourceConfig(
        source_key="tfl_line_status",
        topic=os.getenv("KAFKA_TOPIC_TFL_LINE_STATUS", "transport-tfl-line-status"),
        api_url=os.getenv("TFL_LINE_STATUS_API_URL", TFL_DEFAULT_LINE_STATUS_URL),
        api_key=os.getenv("TFL_API_KEY"),
        auth_header="query-app_key",
        poll_interval_seconds=300.0,
    ),
    "tfl_arrivals": StreamSourceConfig(
        source_key="tfl_arrivals",
        topic=os.getenv("KAFKA_TOPIC_TFL_ARRIVALS", "transport-tfl-arrivals"),
        api_url=os.getenv(
            "TFL_ARRIVALS_API_URL",
            f"https://api.tfl.gov.uk/StopPoint/{TFL_DEFAULT_ARRIVALS_STOP_ID}/Arrivals",
        ),
        api_key=os.getenv("TFL_API_KEY"),
        auth_header="query-app_key",
        poll_interval_seconds=60.0,
    ),
    "gtfs": StreamSourceConfig(
        source_key="gtfs",
        topic=os.getenv("KAFKA_TOPIC_TRANSPORT_GTFS", "transport-gtfs"),
        api_url=os.getenv("GTFS_REALTIME_URL"),
        api_key=None,
    ),
    "londonair": StreamSourceConfig(
        source_key="londonair",
        topic=os.getenv("KAFKA_TOPIC_LONDONAIR", "environment-londonair"),
        api_url=os.getenv("LONDONAIR_API_BASE_URL", "https://api.erg.ic.ac.uk/AirQuality")
              + os.getenv("LONDONAIR_HOURLY_INDEX_ENDPOINT", "/Hourly/MonitoringIndex/GroupName=London/Json"),
        api_key=os.getenv("LONDONAIR_API_KEY"),
    ),
    "openmeteo": StreamSourceConfig(
        source_key="openmeteo",
        topic=os.getenv("KAFKA_TOPIC_OPENMETEO", "environment-openmeteo"),
        api_url=os.getenv("OPENMETEO_API_URL", "https://air-quality-api.open-meteo.com")
              + "/v1/air-quality?latitude=51.5074&longitude=-0.1278&current=pm10,pm2_5,european_aqi,us_aqi",
        api_key=None,
    ),
    "openweather": StreamSourceConfig(
        source_key="openweather",
        topic=os.getenv("KAFKA_TOPIC_OPENWEATHER", "environment-openweather"),
        api_url=os.getenv("OPENWEATHER_API_URL", "https://api.openweathermap.org")
              + "/data/2.5/weather?q=London&units=metric&appid=" + (os.getenv("OPENWEATHER_API_KEY") or ""),
        api_key=os.getenv("OPENWEATHER_API_KEY"),
    ),
}

# Default source (from NEXUS_STREAM_SOURCE env var)
DEFAULT_STREAM_SOURCE = os.getenv("NEXUS_STREAM_SOURCE", "transport")

# DLQ topic configuration
DLQ_TOPIC = os.getenv("NEXUS_DLQ_TOPIC", "nexus.dlq")

# Environment variables
ENV_BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS"
ENV_CONSUMER_GROUP = "NEXUS_CONSUMER_GROUP"
ENV_DLQ_TOPIC = "NEXUS_DLQ_TOPIC"
