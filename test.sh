#!/bin/bash

echo "Running TaaSim stack health checks..."
echo ""

# Wait for services to stabilize
echo "Waiting 10 seconds for services to stabilize..."
sleep 10

PASS=0
FAIL=0

# Kafka
echo "--- [1/6] Kafka ---"
if docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null; then
  echo "  ✅ Kafka OK"
  ((PASS++))
else
  echo "  ❌ Kafka FAILED"
  ((FAIL++))
fi

# MinIO
echo "--- [2/6] MinIO ---"
if docker run --rm --network taasim-net --entrypoint sh minio/mc:latest -c "mc alias set local http://minio:9000 minioadmin minioadmin123 >/dev/null 2>&1 && mc ls local" 2>/dev/null; then
  echo "  ✅ MinIO OK"
  ((PASS++))
else
  echo "  ❌ MinIO FAILED"
  ((FAIL++))
fi

# Cassandra
echo "--- [3/6] Cassandra ---"
if docker compose exec -T cassandra cqlsh -e 'DESCRIBE KEYSPACES' 2>/dev/null; then
  echo "  ✅ Cassandra OK"
  ((PASS++))
else
  echo "  ❌ Cassandra FAILED"
  ((FAIL++))
fi

# Flink
echo "--- [4/6] Flink ---"
if docker compose exec -T flink-jobmanager flink list 2>/dev/null; then
  echo "  ✅ Flink OK"
  ((PASS++))
else
  echo "  ❌ Flink FAILED"
  ((FAIL++))
fi

# Spark
echo "--- [5/6] Spark ---"
if curl -sf http://localhost:8080/ >/dev/null 2>&1; then
  echo "  ✅ Spark OK"
  ((PASS++))
else
  echo "  ❌ Spark FAILED"
  ((FAIL++))
fi

# Grafana
echo "--- [6/6] Grafana ---"
if curl -sf http://localhost:3000/api/health 2>/dev/null | grep -q ok; then
  echo "  ✅ Grafana OK"
  ((PASS++))
else
  echo "  ❌ Grafana FAILED"
  ((FAIL++))
fi

echo ""
echo "Results: ${PASS} passed, ${FAIL} failed out of 6 checks."

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
echo "All stack checks passed! ✅"
