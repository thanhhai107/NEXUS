from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator


AGENT_DECISIONS_LOG = Path("/opt/airflow/runtime/logs/agent_decisions.jsonl")


def branch_on_agent_decision(dataset_name: str) -> str:
    if not AGENT_DECISIONS_LOG.exists():
        raise FileNotFoundError(f"No agent decision log found at {AGENT_DECISIONS_LOG}")

    latest_decision: str | None = None
    with AGENT_DECISIONS_LOG.open("r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            decision = json.loads(line)
            if decision.get("dataset_name") == dataset_name:
                latest_decision = decision.get("decision")

    if latest_decision in {"PASS", "WARNING"}:
        return "raw_to_bronze"
    if latest_decision == "FAIL":
        return "stop_after_quarantine"
    raise ValueError(f"No valid agent decision found for {dataset_name}")


with DAG(
    dag_id="nexus_batch_ingestion_pipeline",
    description="Batch CSV/API ingestion through quality gates and medallion Spark jobs.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["nexus", "batch", "lakehouse"],
) as dag:
    ingest_csv = BashOperator(
        task_id="ingest_us_accidents_csv",
        bash_command=(
            "python /opt/airflow/ingestion/batch/csv_ingestion.py "
            "--dataset us_accidents "
            "--source /opt/airflow/samples/us_accidents_sample.csv"
        ),
    )

    run_quality_gate = BashOperator(
        task_id="run_quality_gate",
        bash_command=(
            "python -m cli.nexus quality check "
            "--dataset us_accidents "
            "--source /opt/airflow/samples/us_accidents_sample.csv "
            "--required-columns ID Severity Start_Time Start_Lat Start_Lng State "
            "--primary-keys ID "
            "--freshness-column Start_Time "
            "--max-age-hours 24 "
            "--batch-id {{ run_id }} "
            "--run-id {{ run_id }} "
            "--actor airflow "
            "--no-exit-on-fail"
        ),
    )

    agent_review = BashOperator(
        task_id="agent_review",
        bash_command=(
            "python -m cli.nexus agent review "
            "--dataset us_accidents "
            "--batch-id {{ run_id }}"
        ),
    )

    branch_on_decision = BranchPythonOperator(
        task_id="branch_on_agent_decision",
        python_callable=branch_on_agent_decision,
        op_kwargs={"dataset_name": "us_accidents"},
    )

    stop_after_quarantine = BashOperator(
        task_id="stop_after_quarantine",
        bash_command=(
            "echo 'NEXUS Governance Agent returned FAIL. "
            "Stopping pipeline after quarantine.'"
        ),
    )

    raw_to_bronze = BashOperator(
        task_id="raw_to_bronze",
        bash_command=(
            "spark-submit --master spark://spark:7077 /opt/airflow/processing/bronze/raw_to_bronze.py "
            "--raw-path /opt/airflow/runtime/raw/us_accidents/*.jsonl "
            "--bronze-table nexus.bronze.us_accidents"
        ),
    )

    record_bronze_lineage = BashOperator(
        task_id="record_bronze_lineage",
        bash_command=(
            "python -m cli.nexus lineage record "
            "--job-name raw_to_bronze "
            "--inputs /opt/airflow/runtime/raw/us_accidents/*.jsonl "
            "--outputs nexus.bronze.us_accidents "
            "--batch-id {{ run_id }} "
            "--run-id {{ run_id }} "
            "--source-path /opt/airflow/samples/us_accidents_sample.csv "
            "--actor airflow"
        ),
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            "spark-submit --master spark://spark:7077 /opt/airflow/processing/silver/bronze_to_silver.py "
            "--bronze-table nexus.bronze.us_accidents "
            "--silver-table nexus.silver.us_accidents"
        ),
    )

    record_silver_lineage = BashOperator(
        task_id="record_silver_lineage",
        bash_command=(
            "python -m cli.nexus lineage record "
            "--job-name bronze_to_silver "
            "--inputs nexus.bronze.us_accidents "
            "--outputs nexus.silver.us_accidents "
            "--batch-id {{ run_id }} "
            "--run-id {{ run_id }} "
            "--source-path nexus.bronze.us_accidents "
            "--actor airflow"
        ),
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=(
            "spark-submit --master spark://spark:7077 /opt/airflow/processing/gold/silver_to_gold.py "
            "--silver-table nexus.silver.us_accidents "
            "--gold-table nexus.gold.us_accidents_summary "
            "--group-by state "
            "--metric-column distance_mi"
        ),
    )

    record_gold_lineage = BashOperator(
        task_id="record_gold_lineage",
        bash_command=(
            "python -m cli.nexus lineage record "
            "--job-name silver_to_gold "
            "--inputs nexus.silver.us_accidents "
            "--outputs nexus.gold.us_accidents_summary "
            "--batch-id {{ run_id }} "
            "--run-id {{ run_id }} "
            "--source-path nexus.silver.us_accidents "
            "--actor airflow"
        ),
    )

    # Airflow owns orchestration; Python/Spark modules own pipeline behavior.
    ingest_csv >> run_quality_gate >> agent_review >> branch_on_decision
    branch_on_decision >> raw_to_bronze >> record_bronze_lineage >> bronze_to_silver
    bronze_to_silver >> record_silver_lineage >> silver_to_gold >> record_gold_lineage
    branch_on_decision >> stop_after_quarantine
