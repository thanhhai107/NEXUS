"""Streaming Pipeline DAG.

Produces events to Kafka, consumes into raw layer, validates and emits lineage.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_SOURCE = os.getenv("NEXUS_STREAM_SOURCE", "transport")
DEFAULT_DATASET = os.getenv("NEXUS_STREAM_DATASET", "transport_events")
DEFAULT_TOPIC = os.getenv("NEXUS_STREAM_TOPIC", "transport-events")
DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")

PROJECT_ROOT = os.getenv("NEXUS_REPO_PATH", str(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))))

with DAG(
    dag_id="nexus_streaming_pipeline",
    description="Produce events to Kafka, consume into raw layer, validate and emit lineage.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nexus", "streaming", "kafka"],
) as dag:
    produce_events = BashOperator(
        task_id="produce_kafka_events",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python ingestion/streaming/producer.py "
            f"--source {DEFAULT_SOURCE} "
            f"--bootstrap-servers {DEFAULT_BOOTSTRAP} "
            "--events 100 "
            "--delay-seconds 0.1"
        ),
        pool="streaming_pool",
    )

    consume_to_raw = BashOperator(
        task_id="consume_kafka_to_raw",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python ingestion/streaming/consumer.py "
            f"--topic {DEFAULT_TOPIC} "
            f"--dataset {DEFAULT_DATASET} "
            f"--bootstrap-servers {DEFAULT_BOOTSTRAP} "
            "--max-messages 100"
        ),
        pool="streaming_pool",
    )

    streaming_quality_checkpoint = BashOperator(
        task_id="streaming_quality_checkpoint",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python -m cli.nexus quality stream "
            f"--source {DEFAULT_SOURCE} "
            "--batch-id {{ run_id }} "
            "--run-id {{ run_id }} "
            "--actor airflow "
            "--sample-events 25 "
            "--no-exit-on-fail"
        ),
    )

    update_lineage = BashOperator(
        task_id="update_streaming_lineage",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python -m cli.nexus lineage record "
            f"--job-name kafka_{DEFAULT_DATASET}_to_bronze "
            f"--inputs kafka://{DEFAULT_TOPIC} "
            f"--outputs nexus.bronze.{DEFAULT_DATASET} "
            "--batch-id {{ run_id }} "
            "--run-id {{ run_id }} "
            f"--source-path kafka://{DEFAULT_TOPIC} "
            "--actor airflow"
        ),
    )

    produce_events >> consume_to_raw >> streaming_quality_checkpoint >> update_lineage
