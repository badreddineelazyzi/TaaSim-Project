#!/bin/bash
# Initialize Kafka topics with correct partition count and retention.
# Run after the Docker stack is healthy.
#
# Usage:  bash scripts/init-kafka-topics.sh

set -euo pipefail

BOOTSTRAP="localhost:9092"
RETENTION_MS=604800000   # 7 days

echo "Creating Kafka topics..."

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --create --if-not-exists \
  --topic raw.gps \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=${RETENTION_MS}
echo "  ✅ raw.gps (3 partitions, 7d retention)"

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --create --if-not-exists \
  --topic raw.trips \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=${RETENTION_MS}
echo "  ✅ raw.trips (3 partitions, 7d retention)"

echo ""
echo "Verifying topics:"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --describe --topic raw.gps

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" \
  --describe --topic raw.trips

echo ""
echo "Done — Kafka topics initialized."
