#!/bin/bash

# Kafka Source Demo (Python)
# Basic Kafka source reading transactions

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PYTHON_SCRIPT="$PROJECT_ROOT/fraud-detection/src/main/python/scripts/kafka_source_demo.py"
PYTHON_DIR="$PROJECT_ROOT/fraud-detection/src/main/python"
PARALLELISM="${1:-2}"

if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "Error: Python script not found at $PYTHON_SCRIPT"
    exit 1
fi

if ! docker ps | grep -q "flink-jobmanager"; then
    echo "Error: Flink not running"
    exit 1
fi

echo "Deploying Kafka Source Demo (Python, parallelism: $PARALLELISM)"

# Copy Python files to container
docker exec flink-jobmanager mkdir -p /opt/flink/pyflink/
docker cp "$PYTHON_DIR" flink-jobmanager:/opt/flink/pyflink/

# Submit Python job with --pyFiles
docker exec flink-jobmanager bash -c "
export PYTHONPATH=/opt/flink/pyflink/python:\$PYTHONPATH
flink run -d \
  --pyFiles /opt/flink/pyflink/python \
  -py /opt/flink/pyflink/python/scripts/kafka_source_demo.py
"

echo ""
echo "Job submitted!"
echo "Check Flink UI: http://localhost:8081"
echo "Input topic: transactions"

