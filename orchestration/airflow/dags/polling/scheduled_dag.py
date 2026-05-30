"""Scheduled Polling DAGs.

Generates per-source DAGs based on polling configuration.
Each source polls at its own interval with configurable retry policies.
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

from orchestration.airflow.config import (
    list_enabled_sources,
    get_source_timeout,
    get_source_retry_config,
)
from orchestration.airflow.pools import get_pool_name


PROJECT_ROOT = Path(__file__).resolve().parents[5]
NEXUS_REPO_PATH = os.getenv("NEXUS_REPO_PATH", str(PROJECT_ROOT))


def get_downloader_cmd(source: str, mode: str = "small_demo", backfill: bool = False) -> str:
    """Build downloader command for a source."""
    cmd = (
        f"cd {NEXUS_REPO_PATH} && "
        f"python ingestion/downloaders/london_downloader.py "
        f"--source {source} "
        f"--mode {mode} "
        "--run-id {{ ts_nodash }}"
    )
    if backfill:
        cmd += " --backfill"
    return cmd


def get_retry_params(source: str) -> dict:
    """Get retry parameters for a source from config."""
    retry_config = get_source_retry_config(source)
    return {
        "retries": retry_config.get("max_attempts", 3),
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    }


def get_timeout(source: str) -> int:
    """Get timeout in minutes for a source."""
    return get_source_timeout(source)


def should_backfill_on_restart(source: str) -> bool:
    """Check if backfill is enabled for a source."""
    # Import from new config location
    from orchestration.airflow.config import get_source_polling_config
    config = get_source_polling_config(source)
    return config.get("backfill_on_restart", False)


def create_polling_dag(source: str, interval_seconds: int) -> DAG:
    """Create a DAG for a polling source."""

    default_args = {
        "owner": "nexus",
        "depends_on_past": False,
        **get_retry_params(source),
        "execution_timeout": timedelta(minutes=get_timeout(source)),
    }

    dag_id = f"nexus_polling_{source}"

    dag = DAG(
        dag_id=dag_id,
        description=f"Polling for {source} every {interval_seconds} seconds",
        start_date=datetime(2025, 1, 1),
        schedule_interval=timedelta(seconds=interval_seconds),
        catchup=False,
        max_active_runs=1,
        tags=["nexus", "polling", source],
        default_args=default_args,
    )

    with dag:
        poll_task = BashOperator(
            task_id=f"poll_{source}",
            bash_command=get_downloader_cmd(source),
            pool=get_pool_name(source),
            pool_slots=1,
        )

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
    from common.config import load_backfill_config
    
    backfill_config = load_backfill_config()
    gap_threshold_minutes = backfill_config.get("gap_threshold_minutes", 30)

    last_run_key = f"nexus_last_run_{source}"
    last_run_time = Variable.get(last_run_key, default_var=None)

    if not last_run_time:
        Variable.set(last_run_key, datetime.utcnow().isoformat())
        return None

    last_run = datetime.fromisoformat(last_run_time)
    now = datetime.utcnow()
    gap_minutes = (now - last_run).total_seconds() / 60

    if gap_minutes > gap_threshold_minutes:
        print(f"Gap detected for {source}: {gap_minutes:.1f} minutes")
        return {
            "source": source,
            "gap_minutes": gap_minutes,
            "last_run": last_run_time,
            "trigger_backfill": True,
        }

    Variable.set(last_run_key, now.isoformat())
    return None


def generate_polling_dags() -> list[DAG]:
    """Generate polling DAGs for all enabled sources."""
    from orchestration.airflow.config import get_polling_interval
    
    dags = []
    
    for source in list_enabled_sources():
        interval = get_polling_interval(source)
        if interval > 0:
            dag = create_polling_dag(source, interval)
            dags.append(dag)
    
    return dags


# Generate DAGs at module load time
globals().update({dag.dag_id: dag for dag in generate_polling_dags()})
