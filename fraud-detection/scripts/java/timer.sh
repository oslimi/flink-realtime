#!/bin/bash

# Timer-based Fraud Reporting Demo
# Aggregates fraud alerts using timer-based reporting (60-second windows)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
JAR="$PROJECT_ROOT/target/fraud-detection-1.0-SNAPSHOT.jar"
CLASS="com.wslimi.demo.fraud.TimerReportingDemoApp"
PARALLELISM="${1:-2}"

# Build if JAR doesn't exist
if [ ! -f "$JAR" ]; then
    echo "JAR not found, building..."
    echo "Building in: $PROJECT_ROOT"
    cd "$PROJECT_ROOT"
    mvn clean package -DskipTests
    cd - > /dev/null
fi

if ! docker ps | grep -q "flink-jobmanager"; then
    echo "Error: Flink not running"
    exit 1
fi

echo "Deploying Timer-based Reporting (parallelism: $PARALLELISM)"
echo "Aggregation window: 60 seconds"

docker exec flink-jobmanager mkdir -p /opt/flink/usrlib/

docker cp "$JAR" flink-jobmanager:/opt/flink/usrlib/

docker exec flink-jobmanager flink run \
    -d -p "$PARALLELISM" -c "$CLASS" \
    /opt/flink/usrlib/fraud-detection-1.0-SNAPSHOT.jar

echo ""
echo "Job submitted!"
echo "Check Flink UI: http://localhost:8081"
echo "Input topic: fraud-alerts"
echo "Output topic: fraud-reports"





