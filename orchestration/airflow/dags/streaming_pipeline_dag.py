from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="nexus_streaming_pipeline",
    description="Produce streaming events and record lightweight quality and lineage metadata.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nexus", "streaming", "kafka"],
) as dag:
    produce_events = BashOperator(
        task_id="produce_transport_events",
        bash_command=(
            "python /opt/airflow/ingestion/streaming/producer.py "
            "--source ${NEXUS_STREAM_SOURCE:-transport} "
            "--bootstrap-servers ${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092} "
            "--events 100 "
            "--delay-seconds 0.1"
        ),
    )

    streaming_quality_checkpoint = BashOperator(
        task_id="streaming_quality_checkpoint",
        bash_command=(
            "python -m cli.nexus quality stream "
            "--source ${NEXUS_STREAM_SOURCE:-transport} "
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
            "python -m cli.nexus lineage record "
            "--job-name kafka_transport_events_to_bronze "
            "--inputs kafka://transport-events "
            "--outputs nexus.bronze.transport_events "
            "--batch-id {{ run_id }} "
            "--run-id {{ run_id }} "
            "--source-path kafka://transport-events "
            "--actor airflow"
        ),
    )

    produce_events >> streaming_quality_checkpoint >> update_lineage
