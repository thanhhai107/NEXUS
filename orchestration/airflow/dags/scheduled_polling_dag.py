"""Scheduled Polling DAGs for real-time data sources.

Generates per-source DAGs based on polling configuration in download_defaults.yml.
Each source gets its own DAG with configurable schedule and retry policies.

Usage:
    - DAGs are auto-generated from config polling settings
    - Each source polls at its own interval (e.g., tfl_arrivals: 60s, waqi: 300s)
    - Supports backfill detection on restart
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[3]
NEXUS_REPO_PATH = os.getenv("NEXUS_REPO_PATH", str(PROJECT_ROOT))

# Import config helpers
import sys
sys.path.insert(0, str(PROJECT_ROOT))

from common.config import (
    load_polling_config,
    load_backfill_config,
)


def get_downloader_cmd(source: str, mode: str = "small_demo", backfill: bool = False) -> str:
    """Build downloader command for a source."""
    cmd = (
        f"cd {NEXUS_REPO_PATH} && "
        f"python ingestion/downloaders/london_downloader.py "
        f"--source {source} "
        f"--mode {mode} "
        f"--run-id {{ ts_nodash }}"
    )
    if backfill:
        cmd += " --backfill"
    return cmd


def get_source_pool(source: str) -> str:
    """Get Airflow pool for a source."""
    env_name = f"NEXUS_AIRFLOW_POOL_{source.upper().replace('-', '_')}"
    return os.getenv(env_name) or os.getenv("NEXUS_AIRFLOW_API_POOL", "default_pool")


def get_source_timeout(source: str) -> int:
    """Get timeout in minutes for a source."""
    polling_config = load_polling_config()
    return polling_config.get(source, {}).get("timeout_minutes", 10)


def should_backfill_on_restart(source: str) -> bool:
    """Check if backfill is enabled for a source."""
    polling_config = load_polling_config()
    return polling_config.get(source, {}).get("backfill_on_restart", False)


def create_polling_dag(source: str, interval_seconds: int) -> DAG:
    """Create a DAG for a polling source."""

    default_args = {
        "owner": "nexus",
        "depends_on_past": False,
        "retries": int(os.getenv("NEXUS_AIRFLOW_DOWNLOAD_RETRIES", "3")),
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(minutes=get_source_timeout(source)),
    }

    dag_id = f"nexus_polling_{source}"

    dag = DAG(
        dag_id=dag_id,
        description=f"Polling for {source} every {interval_seconds} seconds",
        start_date=datetime(2026, 1, 1),
        schedule_interval=timedelta(seconds=interval_seconds),
        catchup=False,
        max_active_runs=1,
        tags=["nexus", "polling", source],
        default_args=default_args,
    )

    with dag:
        # Main polling task
        poll_task = BashOperator(
            task_id=f"poll_{source}",
            bash_command=get_downloader_cmd(source),
            pool=get_source_pool(source),
            pool_slots=1,
        )

        # Optional: Check for backfill gap
        if should_backfill_on_restart(source):
            backfill_check = PythonOperator(
                task_id=f"check_backfill_gap_{source}",
                python_callable=_check_backfill_gap,
                op_kwargs={"source": source},
                trigger_rule="all_done",
            )
            poll_task >> backfill_check

    return dag


def _check_backfill_gap(source: str, **context) -> dict[str, Any] | None:
    """Check if there's a gap that requires backfill."""
    backfill_config = load_backfill_config()
    gap_threshold_minutes = backfill_config.get("gap_threshold_minutes", 30)

    last_run_key = f"nexus_last_run_{source}"
    last_run_time = Variable.get(last_run_key, default_var=None)

    if not last_run_time:
        # First run, no backfill needed
        Variable.set(last_run_key, datetime.utcnow().isoformat())
        return None

    last_run = datetime.fromisoformat(last_run_time)
    now = datetime.utcnow()
    gap_minutes = (now - last_run).total_seconds() / 60

    if gap_minutes > gap_threshold_minutes:
        # Gap detected, could trigger backfill DAG
        print(f"Gap detected for {source}: {gap_minutes:.1f} minutes")
        return {
            "source": source,
            "gap_minutes": gap_minutes,
            "last_run": last_run_time,
            "trigger_backfill": True,
        }

    # Update last run time
    Variable.set(last_run_key, now.isoformat())
    return None


def generate_all_polling_dags() -> list[DAG]:
    """Generate all polling DAGs based on config."""
    polling_config = load_polling_config()
    dags = []

    for source, config in polling_config.items():
        if not isinstance(config, dict):
            continue

        enabled = config.get("enabled", False)
        interval = config.get("interval_seconds", 0)

        if enabled and interval > 0:
            dag = create_polling_dag(source, interval)
            dags.append(dag)

    return dags


# Generate DAGs at module load time
# This makes them available to Airflow automatically
globals().update({dag.dag_id: dag for dag in generate_all_polling_dags()})
