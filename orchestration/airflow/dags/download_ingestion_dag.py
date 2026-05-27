from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


DEFAULT_SOURCES = [
    source.strip()
    for source in os.getenv("NEXUS_DOWNLOAD_SOURCES", "openaq,tfl_status,waqi").split(",")
    if source.strip()
]
DEFAULT_MODE = os.getenv("NEXUS_DOWNLOAD_MODE", "small_demo")
from common.config import BRONZE_DIR

DOWNLOAD_ROOT = Path(os.getenv("NEXUS_DOWNLOAD_ROOT", str(DATASETS_DIR)))
RAW_ENVELOPE_REQUIRED = os.getenv("NEXUS_RAW_ENVELOPE_REQUIRED", "true").lower() == "true"


def source_pool(source: str) -> str:
    env_name = f"NEXUS_AIRFLOW_POOL_{source.upper().replace('-', '_')}"
    return os.getenv(env_name) or os.getenv("NEXUS_AIRFLOW_API_POOL", "default_pool")


def assert_published_outputs(run_id: str, expected_count: int) -> list[str]:
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
    dag_id="nexus_download_ingestion_pipeline",
    description="Download external API/open-data sources with timeout, retry, pools and published-manifest gate.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nexus", "download", "ingestion"],
) as dag:
    download_tasks = []
    for source in DEFAULT_SOURCES:
        download_tasks.append(
            BashOperator(
                task_id=f"download_{source}",
                bash_command=(
                    "python /opt/airflow/ingestion/downloaders/london_downloader.py "
                    f"--source {source} "
                    f"--mode {DEFAULT_MODE} "
                    "--run-id {{ ts_nodash }}"
                ),
                pool=source_pool(source),
                pool_slots=1,
                retries=int(os.getenv("NEXUS_AIRFLOW_DOWNLOAD_RETRIES", "3")),
                retry_delay=timedelta(minutes=1),
                retry_exponential_backoff=True,
                max_retry_delay=timedelta(minutes=30),
                execution_timeout=timedelta(
                    minutes=int(os.getenv("NEXUS_AIRFLOW_DOWNLOAD_TIMEOUT_MINUTES", "120"))
                ),
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
