#!/usr/bin/env bash
# NEXUS per-node service assignment.
# Supports 1 master + N workers. Worker index > 3 falls back to compute profile.

master_services() {
  echo "zookeeper-1 kafka-1 minio-1 minio-init \
    hive-metastore-db hive-metastore \
    airflow-db \
    redis spark spark-worker-1 \
    trino-coordinator trino-worker-1 \
    airflow airflow-scheduler-1 airflow-scheduler-2 airflow-worker-1 airflow-worker-2 \
    superset api-lb api-1 \
    openmetadata-postgresql openmetadata-elasticsearch openmetadata-migrate openmetadata-server openmetadata-ingestion \
    marquez-db marquez marquez-web"
}

worker_profile_1_services() {
  echo "zookeeper-2 kafka-2 minio-2 spark-worker trino-worker airflow-worker api-2"
}

worker_profile_2_services() {
  echo "zookeeper-3 kafka-3 minio-3 spark-worker trino-worker airflow-worker api-3"
}

worker_profile_3_services() {
  echo "minio-4 spark-worker trino-worker airflow-worker"
}

worker_compute_only_services() {
  echo "spark-worker trino-worker airflow-worker"
}

worker_services() {
  case "${NEXUS_NODE_INDEX:-1}" in
    1) worker_profile_1_services ;;
    2) worker_profile_2_services ;;
    3) worker_profile_3_services ;;
    *) worker_compute_only_services ;;
  esac
}

node_services() {
  case "${NEXUS_NODE_ROLE:-}" in
    master) master_services ;;
    worker) worker_services ;;
    *) echo "" ;;
  esac
}
