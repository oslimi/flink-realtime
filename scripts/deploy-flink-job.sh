#!/bin/bash

# ===========================================
# Flink Job Deployment Script
# ===========================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
FLINK_CONTAINER="flink-jobmanager"
FLINK_JOBS_DIR="/opt/flink/usrlib"

# Usage function
usage() {
    echo -e "${YELLOW}Usage:${NC} $0 <jar-file> [entry-class] [options]"
    echo ""
    echo "Arguments:"
    echo "  jar-file      Path to the Flink JAR file (required)"
    echo "  entry-class   Main class name (optional, auto-detected if not provided)"
    echo ""
    echo "Options:"
    echo "  -p, --parallelism   Set job parallelism (default: 1)"
    echo "  -d, --detached      Run job in detached mode"
    echo "  -h, --help          Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 ./target/flink-jobs-1.0.jar"
    echo "  $0 ./target/flink-jobs-1.0.jar com.demo.jobs.SampleStreamingJob"
    echo "  $0 ./target/flink-jobs-1.0.jar com.demo.jobs.SampleStreamingJob -p 2 -d"
    exit 1
}

# Check if JAR file is provided
if [ -z "$1" ] || [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    usage
fi

JAR_FILE="$1"
ENTRY_CLASS="$2"
PARALLELISM=1
DETACHED=""

# Parse optional arguments
shift 2 2>/dev/null || shift 1
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--parallelism)
            PARALLELISM="$2"
            shift 2
            ;;
        -d|--detached)
            DETACHED="-d"
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
done

# Check if JAR file exists
if [ ! -f "$JAR_FILE" ]; then
    echo -e "${RED}Error: JAR file not found: $JAR_FILE${NC}"
    exit 1
fi

# Get absolute path and filename
JAR_PATH=$(realpath "$JAR_FILE")
JAR_NAME=$(basename "$JAR_FILE")

echo -e "${YELLOW}===========================================\n"
echo -e "Flink Job Deployment"
echo -e "===========================================${NC}"
echo -e "JAR File: ${GREEN}$JAR_NAME${NC}"
echo -e "Entry Class: ${GREEN}${ENTRY_CLASS:-auto-detect}${NC}"
echo -e "Parallelism: ${GREEN}$PARALLELISM${NC}"
echo ""

# Check if Flink JobManager is running
echo -e "${YELLOW}Checking Flink JobManager...${NC}"
if ! sudo docker ps --format '{{.Names}}' | grep -q "^${FLINK_CONTAINER}$"; then
    echo -e "${RED}Error: Flink JobManager container is not running${NC}"
    echo "Start it with: cd infrastructure && sudo docker compose up -d"
    exit 1
fi
echo -e "${GREEN}✓ Flink JobManager is running${NC}"

# Copy JAR to container
echo -e "${YELLOW}Copying JAR to Flink container...${NC}"
sudo docker exec "$FLINK_CONTAINER" mkdir -p "$FLINK_JOBS_DIR"
sudo docker cp "$JAR_PATH" "$FLINK_CONTAINER:$FLINK_JOBS_DIR/$JAR_NAME"
echo -e "${GREEN}✓ JAR copied successfully${NC}"

# Build flink run command
FLINK_CMD="flink run $DETACHED -p $PARALLELISM"
if [ -n "$ENTRY_CLASS" ]; then
    FLINK_CMD="$FLINK_CMD -c $ENTRY_CLASS"
fi
FLINK_CMD="$FLINK_CMD $FLINK_JOBS_DIR/$JAR_NAME"

# Submit job
echo -e "${YELLOW}Submitting job to Flink...${NC}"
echo -e "Command: $FLINK_CMD"
echo ""

sudo docker exec "$FLINK_CONTAINER" $FLINK_CMD

echo ""
echo -e "${GREEN}===========================================${NC}"
echo -e "${GREEN}Job submitted successfully!${NC}"
echo -e "${GREEN}===========================================${NC}"
echo ""
echo -e "View job at: ${YELLOW}http://localhost:8081${NC}"

