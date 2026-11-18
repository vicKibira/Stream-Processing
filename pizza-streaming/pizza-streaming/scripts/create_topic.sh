#!/usr/bin/env bash
set -euo pipefail

TOPIC=${1:-pizza_orders}
PARTITIONS=${2:-3}
REPLICATION=${3:-1}

echo "Creating Kafka topic: $TOPIC (partitions=$PARTITIONS, replication=$REPLICATION)..."

docker exec pizza-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION"

echo "✅ Topic '$TOPIC' ready."
