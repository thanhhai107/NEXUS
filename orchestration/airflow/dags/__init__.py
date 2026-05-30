"""Orchestration Airflow DAGs.

This module re-exports DAGs from submodules for Airflow discovery.
"""

from orchestration.airflow.dags.batch.download_dag import dag as batch_download_dag
from orchestration.airflow.dags.streaming.pipeline_dag import dag as streaming_pipeline_dag

# Polling DAGs are dynamically generated in polling/scheduled_dag.py

__all__ = [
    "batch_download_dag",
    "streaming_pipeline_dag",
]
