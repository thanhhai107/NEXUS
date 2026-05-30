"""Recovery DAGs for Backfill and DLQ Replay.

Provides DAGs for recovering from failures and replaying failed chunks.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[5]
NEXUS_REPO_PATH = os.getenv("NEXUS_REPO_PATH", str(PROJECT_ROOT))

DEFAULT_SOURCES = ["openaq", "tfl_arrivals", "waqi", "openweather", "londonair"]


def get_backfill_cmd(source: str, start_date: str, end_date: str) -> str:
    """Build backfill command for a source."""
    return (
        f"cd {NEXUS_REPO_PATH} && "
        f"python ingestion/downloaders/london_downloader.py "
        f"--source {source} "
        f"--mode backfill "
        f"--start-date {start_date} "
        f"--end-date {end_date} "
        "--run-id {{ ts_nodash }}"
    )


with DAG(
    dag_id="nexus_backfill",
    description="Backfill historical data for sources",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nexus", "backfill", "recovery"],
) as dag:
    # For now, just trigger backfill for configured sources
    # In production, this would be triggered manually or via API
    for source in DEFAULT_SOURCES:
        BashOperator(
            task_id=f"backfill_{source}",
            bash_command=get_backfill_cmd(
                source,
                "{{ ti.prev_execution_date.strftime('%Y-%m-%d') if ti.prev_execution_date else '2025-01-01' }}",
                "{{ ds }}",
            ),
            pool="recovery_pool",
            retries=2,
            retry_delay=timedelta(minutes=5),
        )
