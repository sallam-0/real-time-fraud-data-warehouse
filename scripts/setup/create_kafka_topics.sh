#!/bin/bash

# Script to create Kafka topics for CDC data
set -e

echo "=========================================="
echo "Creating Kafka Topics for CDC"
echo "=========================================="

KAFKA_BROKER="localhost:29092"
PARTITIONS=3
REPLICATION_FACTOR=1

# Array of topics to create
declare -a topics=(
    "cdc.Transaction"
    "schema-changes.fraud-detection"
    "connect-configs"
    "connect-offsets"
    "connect-status"
)

# Function to create topic
create_topic() {
    local topic=$1
    local partitions=${2:-$PARTITIONS}
    local replication=${3:-$REPLICATION_FACTOR}
    
    echo "Creating topic: $topic (partitions=$partitions, replication=$replication)"
    
    kafka-topics.sh --create \
        --bootstrap-server "$KAFKA_BROKER" \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor "$replication" \
        --if-not-exists \
        --config retention.ms=604800000 \
        --config compression.type=lz4 \
        --config segment.ms=3600000
    
    if [ $? -eq 0 ]; then
        echo "✓ Topic '$topic' created successfully"
    else
        echo "✗ Failed to create topic '$topic'"
    fi
}

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
max_attempts=30
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if kafka-broker-api-versions.sh --bootstrap-server "$KAFKA_BROKER" &> /dev/null; then
        echo "✓ Kafka is ready!"
        break
    fi
    
    attempt=$((attempt + 1))
    echo "Waiting... (Attempt $attempt/$max_attempts)"
    sleep 5
done

if [ $attempt -eq $max_attempts ]; then
    echo "✗ Kafka is not ready after $max_attempts attempts"
    exit 1
fi

echo ""

# Create all topics
for topic in "${topics[@]}"; do
    create_topic "$topic"
done

echo ""
echo "=========================================="
echo "Listing all topics:"
echo "=========================================="
kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" --list

echo ""
echo "=========================================="
echo "Topic Details:"
echo "=========================================="
for topic in "${topics[@]}"; do
    echo ""
    echo "Topic: $topic"
    kafka-topics.sh --bootstrap-server "$KAFKA_BROKER" --describe --topic "$topic" 2>/dev/null || echo "Topic not found"
done

echo ""
echo "✓ Kafka topics setup complete!"
