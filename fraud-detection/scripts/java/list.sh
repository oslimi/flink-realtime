#!/bin/bash

# List running Flink jobs

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! docker ps | grep -q "flink-jobmanager"; then
    echo "Error: Flink not running"
    exit 1
fi

echo "Running Flink jobs:"
docker exec flink-jobmanager flink list

