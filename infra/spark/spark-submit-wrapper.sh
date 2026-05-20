#!/usr/bin/env bash
# spark-submit wrapper that injects OpenLineage Spark listener configuration
# whenever OPENLINEAGE_URL is set. Falls back to vanilla spark-submit otherwise.
set -euo pipefail

SPARK_BIN=${SPARK_SUBMIT_BIN:-spark-submit}
MASTER=${SPARK_MASTER:-spark://spark:7077}

EXTRA_ARGS=()
if [[ -n "${OPENLINEAGE_URL:-}" ]]; then
  OL_NAMESPACE=${OPENLINEAGE_NAMESPACE:-nexus}
  OL_PACKAGE=${OPENLINEAGE_SPARK_PACKAGE:-io.openlineage:openlineage-spark_2.12:1.21.0}
  OL_ENDPOINT=${OPENLINEAGE_ENDPOINT:-/api/v1/lineage}
  EXTRA_ARGS+=(
    "--packages" "$OL_PACKAGE"
    "--conf" "spark.jars.ivy=/tmp/.ivy2"
    "--conf" "spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener"
    "--conf" "spark.openlineage.transport.type=http"
    "--conf" "spark.openlineage.transport.url=$OPENLINEAGE_URL"
    "--conf" "spark.openlineage.transport.endpoint=$OL_ENDPOINT"
    "--conf" "spark.openlineage.namespace=$OL_NAMESPACE"
  )
fi

exec "$SPARK_BIN" --master "$MASTER" "${EXTRA_ARGS[@]}" "$@"