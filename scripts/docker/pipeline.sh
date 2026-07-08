#!/usr/bin/env bash
# Script principal pipeline — execute spark-submit dans nyc-spark-client
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="${ROOT}/docker/.env"
LAYER="${LAYER:-all}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source <(grep -v '^#' "$ENV_FILE" | sed 's/\r$//')
  set +a
fi

SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-1g}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-2g}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-2}"
SPARK_NUM_EXECUTORS="${SPARK_NUM_EXECUTORS:-2}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-16}"
HDFS_URI="${HDFS_URI:-hdfs://namenode:9000}"
SPARK_EVENT_LOG_DIR="${SPARK_EVENT_LOG_DIR:-file:/spark-events}"
MONGO_URI="${MONGO_URI:-mongodb://mongodb:27017}"
MONGO_DATABASE="${MONGO_DATABASE:-nyc_taxi_warehouse}"

if ! docker ps --filter "name=nyc-spark-client" --filter "status=running" -q | grep -q .; then
  echo "Erreur: stack non demarree. Lancez: make up" >&2
  exit 1
fi

echo "[pipeline] Dependances Python..."
docker exec nyc-spark-client pip install -q -r /app/requirements.txt

echo "[pipeline] Repertoires HDFS..."
docker exec nyc-namenode bash -c "
  hdfs dfs -mkdir -p /data/bronze /data/silver /data/gold /data/lake /data/raw /spark-logs /spark-tmp
  hdfs dfs -chmod -R 777 /data /spark-logs /spark-tmp
" >/dev/null 2>&1 || true

LOG_DIR="${ROOT}/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/pipeline_run.log"

echo "[pipeline] spark-submit --layer ${LAYER} $*"
echo "[pipeline] Log: ${LOG_FILE}"

docker exec -w /app \
  -e DOCKER_MODE=true \
  -e STORAGE_BACKEND=hdfs \
  -e HADOOP_USER_NAME=spark \
  -e MONGO_URI="$MONGO_URI" \
  -e MONGO_DATABASE="$MONGO_DATABASE" \
  -e SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  -e PROJECT_ROOT=/app \
  -e PYTHONPATH=/app \
  -e HDFS_URI="$HDFS_URI" \
  -e SPARK_NUM_EXECUTORS="$SPARK_NUM_EXECUTORS" \
  -e SPARK_SHUFFLE_PARTITIONS="$SPARK_SHUFFLE_PARTITIONS" \
  -e SPARK_DRIVER_MEMORY="$SPARK_DRIVER_MEMORY" \
  -e SPARK_EXECUTOR_MEMORY="$SPARK_EXECUTOR_MEMORY" \
  -e SPARK_EXECUTOR_CORES="$SPARK_EXECUTOR_CORES" \
  -e GOLD_PARQUET_FALLBACK="${GOLD_PARQUET_FALLBACK:-true}" \
  -e LIGHT_MODE="${LIGHT_MODE:-false}" \
  -e SAMPLE_YEAR="${SAMPLE_YEAR:-}" \
  -e SAMPLE_MONTH="${SAMPLE_MONTH:-}" \
  -e SAMPLE_MODE="${SAMPLE_MODE:-false}" \
  nyc-spark-client /opt/bitnami/spark/bin/spark-submit \
    --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
    --driver-memory "$SPARK_DRIVER_MEMORY" \
    --executor-memory "$SPARK_EXECUTOR_MEMORY" \
    --executor-cores "$SPARK_EXECUTOR_CORES" \
    --conf "spark.executor.instances=$SPARK_NUM_EXECUTORS" \
    --conf "spark.sql.shuffle.partitions=$SPARK_SHUFFLE_PARTITIONS" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.hadoop.fs.defaultFS=$HDFS_URI" \
    --conf "spark.eventLog.enabled=true" \
    --conf "spark.eventLog.dir=$SPARK_EVENT_LOG_DIR" \
    --conf spark.executorEnv.HADOOP_USER_NAME=spark \
    --conf spark.driver.extraPythonPath=/app \
    --conf spark.executorEnv.PYTHONPATH=/app \
    src/pipeline/run_pipeline.py --layer "$LAYER" "$@" \
  2>&1 | tee -a "$LOG_FILE"
