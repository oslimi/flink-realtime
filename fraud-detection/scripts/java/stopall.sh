#!/bin/bash

# Stop all running Flink jobs

if ! docker ps | grep -q "flink-jobmanager"; then
    echo "Error: Flink not running"
    exit 1
fi

echo "Stopping all jobs..."

job_ids=$(docker exec flink-jobmanager flink list 2>/dev/null | grep RUNNING | grep -oP 'ID: \K[a-f0-9]+' || true)

if [ -z "$job_ids" ]; then
    echo "No running jobs"
    exit 0
fi

echo "$job_ids" | while read job_id; do
    echo "Canceling: $job_id"
    docker exec flink-jobmanager flink cancel "$job_id" 2>/dev/null || true
done

echo "Done"

