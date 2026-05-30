#!/usr/bin/env bash
# spark-submit wrapper that injects OpenLineage Spark listener configuration
# whenever OPENLINEAGE_URL is set. Falls back to vanilla spark-submit otherwise.
set -euo pipefail

SPARK_BIN=${SPARK_SUBMIT_BIN:-spark-submit}
MASTER=${SPARK_MASTER:-spark://spark:7077}
if [[ -n "${SPARK_PROPERTIES_FILE:-}" ]]; then
  PROPERTIES_FILE=$SPARK_PROPERTIES_FILE
elif [[ -f /opt/nexus/infra/spark/spark-defaults.conf ]]; then
  PROPERTIES_FILE=/opt/nexus/infra/spark/spark-defaults.conf
else
  PROPERTIES_FILE=/opt/airflow/config/spark-defaults.conf
fi
ICEBERG_PACKAGE=${ICEBERG_SPARK_PACKAGE:-org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2}
HADOOP_AWS_PACKAGE=${HADOOP_AWS_PACKAGE:-org.apache.hadoop:hadoop-aws:3.3.4}
AWS_BUNDLE_PACKAGE=${AWS_BUNDLE_PACKAGE:-com.amazonaws:aws-java-sdk-bundle:1.12.262}

export HADOOP_USER_NAME=${HADOOP_USER_NAME:-spark}
export USER=${USER:-$HADOOP_USER_NAME}
export LOGNAME=${LOGNAME:-$HADOOP_USER_NAME}

PACKAGES=("$ICEBERG_PACKAGE" "$HADOOP_AWS_PACKAGE" "$AWS_BUNDLE_PACKAGE")
EXTRA_ARGS=(
  "--conf" "spark.jars.ivy=/tmp/.ivy2"
)

if [[ -f "$PROPERTIES_FILE" ]]; then
  EXTRA_ARGS+=("--properties-file" "$PROPERTIES_FILE")
fi

add_conf() {
  local key="$1"
  local value="$2"
  if [[ -n "$value" ]]; then
    EXTRA_ARGS+=("--conf" "$key=$value")
  fi
}

add_conf "spark.driver.bindAddress" "${SPARK_DRIVER_BIND_ADDRESS:-}"
add_conf "spark.driver.host" "${SPARK_DRIVER_HOST:-}"
add_conf "spark.driver.port" "${SPARK_DRIVER_PORT:-}"
add_conf "spark.blockManager.port" "${SPARK_BLOCKMANAGER_PORT:-}"
add_conf "spark.executor.cores" "${SPARK_EXECUTOR_CORES:-}"
add_conf "spark.executor.memory" "${SPARK_EXECUTOR_MEMORY:-}"
add_conf "spark.default.parallelism" "${SPARK_DEFAULT_PARALLELISM:-}"
add_conf "spark.sql.shuffle.partitions" "${SPARK_SQL_SHUFFLE_PARTITIONS:-}"
add_conf "spark.local.dir" "${SPARK_LOCAL_DIRS:-}"
add_conf "spark.sql.catalog.nexus.uri" "${HIVE_METASTORE_URI:-}"
add_conf "hive.metastore.uris" "${HIVE_METASTORE_URI:-}"
add_conf "spark.hadoop.fs.s3a.endpoint" "${MINIO_ENDPOINT:-}"
add_conf "spark.hadoop.fs.s3a.access.key" "${MINIO_ROOT_USER:-}"
add_conf "spark.hadoop.fs.s3a.secret.key" "${MINIO_ROOT_PASSWORD:-}"

if [[ -n "${OPENLINEAGE_URL:-}" ]]; then
  OL_NAMESPACE=${OPENLINEAGE_NAMESPACE:-nexus}
  OL_PACKAGE=${OPENLINEAGE_SPARK_PACKAGE:-io.openlineage:openlineage-spark_2.12:1.21.0}
  OL_ENDPOINT=${OPENLINEAGE_ENDPOINT:-/api/v1/lineage}
  PACKAGES+=("$OL_PACKAGE")
  EXTRA_ARGS+=(
    "--conf" "spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener"
    "--conf" "spark.openlineage.transport.type=http"
    "--conf" "spark.openlineage.transport.url=$OPENLINEAGE_URL"
    "--conf" "spark.openlineage.transport.endpoint=$OL_ENDPOINT"
    "--conf" "spark.openlineage.namespace=$OL_NAMESPACE"
  )
fi

IFS=,
PACKAGE_CSV="${PACKAGES[*]}"
unset IFS

exec "$SPARK_BIN" --master "$MASTER" --packages "$PACKAGE_CSV" "${EXTRA_ARGS[@]}" "$@"
