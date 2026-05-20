from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

DEFAULT_DATASET = os.getenv("NEXUS_REPROCESS_DATASET", "us_accidents")
DEFAULT_BRONZE_TABLE = os.getenv("NEXUS_REPROCESS_BRONZE_TABLE", "nexus.bronze.us_accidents")
DEFAULT_SILVER_TABLE = os.getenv("NEXUS_REPROCESS_SILVER_TABLE", "nexus.silver.us_accidents")
DEFAULT_GOLD_TABLE = os.getenv("NEXUS_REPROCESS_GOLD_TABLE", "nexus.gold.us_accidents_summary")
DEFAULT_RAW_GLOB = os.getenv("NEXUS_REPROCESS_RAW_GLOB", "/opt/airflow/runtime/raw/us_accidents/*.jsonl")

with DAG(
    dag_id="nexus_reprocess_pipeline",
    description="Replay raw landing files through Bronze, Silver and Gold for recovery or backfill.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nexus", "reprocess", "recovery"],
    params={
        "dataset": Param(DEFAULT_DATASET, type="string"),
        "raw_glob": Param(DEFAULT_RAW_GLOB, type="string"),
        "bronze_table": Param(DEFAULT_BRONZE_TABLE, type="string"),
        "silver_table": Param(DEFAULT_SILVER_TABLE, type="string"),
        "gold_table": Param(DEFAULT_GOLD_TABLE, type="string"),
        "group_by": Param("state", type="string"),
        "metric_column": Param("distance_mi", type="string"),
    },
) as dag:
    raw_to_bronze = BashOperator(
        task_id="reprocess_raw_to_bronze",
        bash_command=(
            "bash /opt/airflow/infra/spark/spark-submit-wrapper.sh "
            "/opt/airflow/processing/bronze/raw_to_bronze.py "
            "--raw-path {{ params.raw_glob }} "
            "--bronze-table {{ params.bronze_table }}"
        ),
    )

    bronze_to_silver = BashOperator(
        task_id="reprocess_bronze_to_silver",
        bash_command=(
            "bash /opt/airflow/infra/spark/spark-submit-wrapper.sh "
            "/opt/airflow/processing/silver/bronze_to_silver.py "
            "--bronze-table {{ params.bronze_table }} "
            "--silver-table {{ params.silver_table }}"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="reprocess_silver_to_gold",
        bash_command=(
            "bash /opt/airflow/infra/spark/spark-submit-wrapper.sh "
            "/opt/airflow/processing/gold/silver_to_gold.py "
            "--silver-table {{ params.silver_table }} "
            "--gold-table {{ params.gold_table }} "
            "--group-by {{ params.group_by }} "
            "--metric-column {{ params.metric_column }}"
        ),
    )

    record_reprocess_lineage = BashOperator(
        task_id="record_reprocess_lineage",
        bash_command=(
            "python -m cli.nexus lineage record "
            "--job-name reprocess_{{ params.dataset }} "
            "--inputs {{ params.raw_glob }} "
            "--outputs {{ params.gold_table }} "
            "--batch-id {{ run_id }} "
            "--run-id {{ run_id }} "
            "--source-path {{ params.raw_glob }} "
            "--actor airflow"
        ),
    )

    raw_to_bronze >> bronze_to_silver >> silver_to_gold >> record_reprocess_lineage