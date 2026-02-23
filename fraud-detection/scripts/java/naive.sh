#!/bin/bash

# Naive Fraud Detection Demo
# Simple threshold-based fraud detection (Amount > 100,000 EUR)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
JAR="$PROJECT_ROOT/fraud-detection/target/fraud-detection-1.0-SNAPSHOT.jar"
CLASS="com.wslimi.demo.fraud.NaiveFraudDetectionDemo"
PARALLELISM="${1:-2}"

# Build if JAR doesn't exist
if [ ! -f "$JAR" ]; then
    echo "JAR not found, building..."
    echo "Building in: $PROJECT_ROOT"
    cd "$PROJECT_ROOT"
    mvn clean package -DskipTests
    cd - > /dev/null
fi

# Check Docker
if ! docker ps | grep -q "flink-jobmanager"; then
    echo "Error: Flink not running"
    echo "Start with: cd infrastructure && docker compose up -d"
    exit 1
fi

echo "Deploying Naive Fraud Detection (parallelism: $PARALLELISM)"
echo "Rule: Amount > 100,000 EUR = FRAUD"

# Create directory in container if it doesn't exist
docker exec flink-jobmanager mkdir -p /opt/flink/usrlib/

# Copy JAR
docker cp "$JAR" flink-jobmanager:/opt/flink/usrlib/

# Submit job
docker exec flink-jobmanager flink run \
    -d -p "$PARALLELISM" -c "$CLASS" \
    /opt/flink/usrlib/fraud-detection-1.0-SNAPSHOT.jar

echo ""
echo "Job submitted!"
echo "Check Flink UI: http://localhost:8081"
echo "Input topic: transactions"
echo "Output topic: fraud-alerts-naive"

