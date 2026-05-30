"""DLQ Replay DAG.

Replays messages from Dead Letter Queue.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.getenv("NEXUS_REPO_PATH", str(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))))

DEFAULT_DLQ_TOPIC = os.getenv("NEXUS_DLQ_TOPIC", "nexus.dlq")
DEFAULT_DLQ_CONSUMER_GROUP = os.getenv("NEXUS_DLQ_CONSUMER_GROUP", "nexus-dlq-replay")


with DAG(
    dag_id="nexus_dlq_replay",
    description="Replay messages from Dead Letter Queue",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nexus", "dlq", "recovery"],
) as dag:
    replay_dlq = BashOperator(
        task_id="replay_dlq_messages",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python -m cli.nexus dlq replay "
            f"--topic {DEFAULT_DLQ_TOPIC} "
            f"--group {DEFAULT_DLQ_CONSUMER_GROUP} "
            "--max-messages 100 "
            "--run-id {{ ts_nodash }}"
        ),
        pool="recovery_pool",
    )
