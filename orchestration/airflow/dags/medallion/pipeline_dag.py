"""Medallion Pipeline DAG.

Orchestrates the full Bronze → Silver → Gold Spark processing pipeline.
Triggers after batch download or streaming ingestion completes.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = str(Path(__file__).resolve().parents[5])
NEXUS_REPO = os.getenv("NEXUS_REPO_PATH", PROJECT_ROOT)
SPARK_SUBMIT = os.getenv("NEXUS_SPARK_SUBMIT", "spark-submit")
BRONZE_CATALOG = os.getenv("NEXUS_BRONZE_CATALOG", "nexus.bronze")
SILVER_CATALOG = os.getenv("NEXUS_SILVER_CATALOG", "nexus.silver")
GOLD_CATALOG = os.getenv("NEXUS_GOLD_CATALOG", "nexus.gold")

DATASETS_RAW_PATHS = {
    "openaq": f"{NEXUS_REPO}/runtime/bronze/openaq_measurements",
    "waqi": f"{NEXUS_REPO}/runtime/bronze/waqi_air_quality",
    "londonair": f"{NEXUS_REPO}/runtime/bronze/londonair_monitoring",
    "tfl_status": f"{NEXUS_REPO}/runtime/bronze/tfl_transport_status",
    "openweather": f"{NEXUS_REPO}/runtime/bronze/openweather_current",
    "openmeteo": f"{NEXUS_REPO}/runtime/bronze/openmeteo_air_quality",
    "transport": f"{NEXUS_REPO}/runtime/bronze/transport_events",
}


def bronze_task(dataset: str) -> BashOperator:
    table = f"{BRONZE_CATALOG}.{dataset}"
    raw = DATASETS_RAW_PATHS.get(dataset, "")
    return BashOperator(
        task_id=f"bronze_{dataset}",
        bash_command=(
            f"cd {NEXUS_REPO} && "
            f"{SPARK_SUBMIT} processing/bronze/raw_to_bronze.py "
            f"--raw-path {raw} "
            f"--bronze-table {table} "
            "--write-mode merge"
        ),
        pool="batch_pool",
        pool_slots=1,
        retries=2,
        retry_delay=timedelta(minutes=2),
    )


def silver_task(dataset: str) -> BashOperator:
    bronze_table = f"{BRONZE_CATALOG}.{dataset}"
    silver_table = f"{SILVER_CATALOG}.{dataset}"
    return BashOperator(
        task_id=f"silver_{dataset}",
        bash_command=(
            f"cd {NEXUS_REPO} && "
            f"{SPARK_SUBMIT} processing/silver/bronze_to_silver.py "
            f"--bronze-table {bronze_table} "
            f"--silver-table {silver_table} "
            f"--dataset {dataset} "
            "--write-mode merge"
        ),
        pool="batch_pool",
        pool_slots=1,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )


def gold_task(dataset: str) -> BashOperator:
    silver_table = f"{SILVER_CATALOG}.{dataset}"
    gold_table = f"{GOLD_CATALOG}.{dataset}"
    return BashOperator(
        task_id=f"gold_{dataset}",
        bash_command=(
            f"cd {NEXUS_REPO} && "
            f"{SPARK_SUBMIT} processing/gold/silver_to_gold.py "
            f"--silver-table {silver_table} "
            f"--gold-table {gold_table}"
        ),
        pool="batch_pool",
        pool_slots=1,
        retries=1,
        retry_delay=timedelta(minutes=2),
    )


def create_medallion_dag(dataset: str) -> DAG:
    dag_id = f"nexus_medallion_{dataset}"

    dag = DAG(
        dag_id=dag_id,
        description=f"Medallion pipeline: Bronze → Silver → Gold for {dataset}",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,
        tags=["nexus", "medallion", "spark", dataset],
    )

    with dag:
        bronze = bronze_task(dataset)
        silver = silver_task(dataset)
        gold = gold_task(dataset)
        bronze >> silver >> gold

    return dag


ENABLED_DATASETS = os.getenv(
    "NEXUS_MEDALLION_DATASETS",
    "openaq,waqi,londonair,tfl_status,transport",
).split(",")

for ds in [d.strip() for d in ENABLED_DATASETS if d.strip() and d.strip() in DATASETS_RAW_PATHS]:
    globals()[f"nexus_medallion_{ds}"] = create_medallion_dag(ds)
