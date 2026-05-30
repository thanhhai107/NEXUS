"""Batch Download DAG.

Downloads data from configured sources on-demand or scheduled.
Uses config-driven approach for source configuration.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from orchestration.airflow.config import list_enabled_sources
from orchestration.airflow.pools import get_pool_name
from orchestration.airflow.config import get_source_timeout, get_source_retry_config
from orchestration.shared import is_published


DEFAULT_SOURCES = [
    source.strip()
    for source in os.getenv("NEXUS_DOWNLOAD_SOURCES", "openaq,tfl_status,waqi").split(",")
    if source.strip()
]
DEFAULT_MODE = os.getenv("NEXUS_DOWNLOAD_MODE", "small_demo")
DEFAULT_TIMEOUT_MINUTES = int(os.getenv("NEXUS_AIRFLOW_DOWNLOAD_TIMEOUT_MINUTES", "120"))

from common.config import BRONZE_DIR

DOWNLOAD_ROOT = Path(os.getenv("NEXUS_DOWNLOAD_ROOT", str(BRONZE_DIR)))
RAW_ENVELOPE_REQUIRED = os.getenv("NEXUS_RAW_ENVELOPE_REQUIRED", "true").lower() == "true"


def get_downloader_cmd(source: str, mode: str = DEFAULT_MODE) -> str:
    """Build downloader command for a source."""
    project_root = Path(__file__).resolve().parents[5]
    nexus_repo_path = os.getenv("NEXUS_REPO_PATH", str(project_root))
    
    return (
        f"cd {nexus_repo_path} && "
        f"python ingestion/downloaders/london_downloader.py "
        f"--source {source} "
        f"--mode {mode} "
        "--run-id {{ ts_nodash }}"
    )


def get_retry_params(source: str) -> dict:
    """Get retry parameters for a source from config."""
    retry_config = get_source_retry_config(source)
    return {
        "retries": retry_config.get("max_attempts", 3),
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
    }


def get_timeout_minutes(source: str) -> int:
    """Get timeout in minutes for a source."""
    return get_source_timeout(source)


def assert_published_outputs(run_id: str, expected_count: int) -> list[str]:
    """Assert that expected outputs are published."""
    manifests = sorted(DOWNLOAD_ROOT.glob(f"*/run_id={run_id}/published/published_manifest.json"))
    if len(manifests) < expected_count:
        raise FileNotFoundError(
            f"Expected at least {expected_count} published manifests for run_id={run_id}, "
            f"found {len(manifests)}."
        )

    raw_paths: list[str] = []
    for manifest_path in manifests:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if payload.get("publish_status") not in {"published", "published_with_warning"}:
            raise ValueError(f"Manifest is not published: {manifest_path}")
        raw_path = payload.get("downstream_raw_path")
        if RAW_ENVELOPE_REQUIRED:
            if not raw_path:
                raise ValueError(f"Published manifest lacks downstream_raw_path: {manifest_path}")
            if not Path(str(raw_path)).exists():
                raise FileNotFoundError(f"Stamped raw envelope does not exist: {raw_path}")
        if raw_path:
            raw_paths.append(str(raw_path))
    return raw_paths


with DAG(
    dag_id="nexus_batch_download",
    description="Download external API/open-data sources using config-driven approach.",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nexus", "batch", "download"],
) as dag:
    download_tasks = []
    
    for source in DEFAULT_SOURCES:
        retry_params = get_retry_params(source)
        
        download_tasks.append(
            BashOperator(
                task_id=f"download_{source}",
                bash_command=get_downloader_cmd(source),
                pool=get_pool_name(source),
                pool_slots=1,
                execution_timeout=timedelta(minutes=get_timeout_minutes(source)),
                **retry_params,
            )
        )

    validate_published_outputs = PythonOperator(
        task_id="validate_published_outputs",
        python_callable=assert_published_outputs,
        op_kwargs={
            "run_id": "{{ ts_nodash }}",
            "expected_count": len(DEFAULT_SOURCES),
        },
        trigger_rule="none_failed_min_one_success",
    )

    download_tasks >> validate_published_outputs
