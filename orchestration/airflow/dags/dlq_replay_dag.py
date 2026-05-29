from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

with DAG(
    dag_id="nexus_dlq_replay",
    description="Replay events from the Dead Letter Queue back to their source topic or stdout.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nexus", "dlq", "replay"],
    params={
        "target": Param("kafka", enum=["kafka", "stdout"]),
        "bootstrap_servers": Param(DEFAULT_BOOTSTRAP, type="string"),
        "topic": Param("", type="string"),
        "category": Param("", type="string"),
        "source": Param("", type="string"),
        "dataset": Param("", type="string"),
    },
) as dag:
    replay = BashOperator(
        task_id="replay_dlq_events",
        bash_command=(
            "python -m cli.nexus dlq replay "
            "--target {{ params.target }} "
            "--bootstrap-servers {{ params.bootstrap_servers }} "
            "{% if params.topic %}--topic {{ params.topic }} {% endif %}"
            "{% if params.category %}--category {{ params.category }} {% endif %}"
            "{% if params.source %}--source {{ params.source }} {% endif %}"
            "{% if params.dataset %}--dataset {{ params.dataset }} {% endif %}"
        ),
    )