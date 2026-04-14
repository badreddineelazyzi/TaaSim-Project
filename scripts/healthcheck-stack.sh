#!/usr/bin/env bash

set -euo pipefail

COMPOSE_FILE="docker-compose.yml"
MC_IMAGE="minio/mc:latest"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is not installed or not in PATH"
  exit 1
fi

echo "[1/6] Kafka topic listing"
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker compose -f "${COMPOSE_FILE}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null
echo "  OK"

echo "[2/6] MinIO accessibility via mc"
docker run --rm --network taasim-net -e MC_HOST_local="http://minioadmin:minioadmin123@minio:9000" "${MC_IMAGE}" ls local >/dev/null
echo "  OK"

echo "[3/6] Cassandra cqlsh connectivity"
docker compose -f "${COMPOSE_FILE}" exec -T cassandra cqlsh -e "DESCRIBE KEYSPACES;" >/dev/null
echo "  OK"

echo "[4/6] Flink list command"
docker compose -f "${COMPOSE_FILE}" exec -T flink-jobmanager flink list >/dev/null
echo "  OK"

echo "[5/6] Spark shell launch check"
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL="*" docker compose -f "${COMPOSE_FILE}" exec -T spark-master bash -lc "echo 'println(\"spark-shell ready\"); System.exit(0)' >/tmp/healthcheck.scala; /opt/spark/bin/spark-shell -i /tmp/healthcheck.scala >/dev/null"
echo "  OK"

echo "[6/6] Grafana health endpoint"
curl -fsS http://localhost:3000/api/health >/dev/null
echo "  OK"

echo "All stack checks passed."