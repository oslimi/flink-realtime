#!/bin/bash

# Stop specific Flink job

JOB_ID=$1

if [ -z "$JOB_ID" ]; then
    echo "Usage: $0 <job-id>"
    exit 1
fi

if ! docker ps | grep -q "flink-jobmanager"; then
    echo "Error: Flink not running"
    exit 1
fi

echo "Stopping job: $JOB_ID"
docker exec flink-jobmanager flink cancel "$JOB_ID"
echo "Done"

