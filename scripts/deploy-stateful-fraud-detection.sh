#!/bin/bash

# ===========================================
# Stateful Fraud Detection Demo Deployment Script
# ===========================================
#
# This script supports BOTH Java and Python deployments for comparison:
#
# JAVA MODE (default):
# 1. Builds the fraud-detection module with Maven
# 2. Packages the StatefulFraudDetectionDemoApp
# 3. Deploys it to the Flink cluster
#
# PYTHON MODE:
# 1. Sets up Python virtual environment (if needed)
# 2. Runs the stateful_fraud_detection_demo.py locally with PyFlink
#
# Prerequisites:
# - Flink cluster running (docker compose up)
# - Kafka broker running with 'transactions' topic
# - TransactionDataGeneratorApp running to produce data
# - For Python: Python 3.11 with apache-flink package

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FRAUD_DETECTION_MODULE="$PROJECT_ROOT/fraud-detection"
JAR_FILE="$FRAUD_DETECTION_MODULE/target/fraud-detection-1.0-SNAPSHOT.jar"
ENTRY_CLASS="com.wslimi.demo.fraud.StatefulFraudDetectionDemoApp"
DEFAULT_PARALLELISM=1
FLINK_CONTAINER="flink-jobmanager"

# Python Configuration
PYTHON_DIR="$FRAUD_DETECTION_MODULE/src/main/python"
PYTHON_SCRIPT="$PYTHON_DIR/scripts/stateful_fraud_detection_demo.py"
PYTHON_VENV="$PROJECT_ROOT/.venv"
KAFKA_CONNECTOR_JAR="$PYTHON_DIR/lib/flink-sql-connector-kafka-3.3.0-1.20.jar"

# Parse command line arguments
PARALLELISM=$DEFAULT_PARALLELISM
DETACHED="-d"  # Default to detached mode
SKIP_BUILD=false
MODE="java"  # Default mode: java or python

usage() {
    echo -e "${YELLOW}Usage:${NC} $0 [options]"
    echo ""
    echo -e "${CYAN}Mode Selection:${NC}"
    echo "  --java                  Run Java implementation (default)"
    echo "  --python                Run Python implementation"
    echo "  --both                  Run both sequentially for comparison"
    echo ""
    echo -e "${CYAN}Common Options:${NC}"
    echo "  -p, --parallelism NUM   Set job parallelism (default: $DEFAULT_PARALLELISM)"
    echo "  -f, --foreground        Run job in foreground (blocking) mode"
    echo "  -s, --skip-build        Skip Maven build (Java) / Skip venv setup (Python)"
    echo "  -h, --help              Show this help message"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo "  $0                      # Deploy Java in detached mode (default)"
    echo "  $0 --java -p 4          # Deploy Java with parallelism of 4"
    echo "  $0 --python             # Deploy Python to cluster"
    echo "  $0 --both               # Deploy both for comparison"
    echo "  $0 --java -f            # Deploy Java in foreground (blocking)"
    echo ""
    echo -e "${CYAN}Comparison Notes:${NC}"
    echo "  Java:   Runs on Flink cluster, uses Java serialization"
    echo "  Python: Runs on Flink cluster, uses Py4J bridge"
    echo ""
    echo -e "${CYAN}Note:${NC} Jobs run in detached mode by default (returns immediately)."
    echo "      Use -f/--foreground to wait for job completion."
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --java)
            MODE="java"
            shift
            ;;
        --python)
            MODE="python"
            shift
            ;;
        --both)
            MODE="both"
            shift
            ;;
        -p|--parallelism)
            PARALLELISM="$2"
            shift 2
            ;;
        -f|--foreground)
            DETACHED=""
            shift
            ;;
        -d|--detached)
            DETACHED="-d"
            shift
            ;;
        -s|--skip-build)
            SKIP_BUILD=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
done

echo -e "${BLUE}==========================================="
echo -e "Stateful Fraud Detection Demo Deployment"
echo -e "===========================================${NC}"
echo -e "Mode: ${GREEN}$MODE${NC}"
echo -e "Parallelism: ${GREEN}$PARALLELISM${NC}"
if [ -n "$DETACHED" ]; then
    echo -e "Execution: ${GREEN}detached (non-blocking)${NC}"
else
    echo -e "Execution: ${GREEN}foreground (blocking)${NC}"
fi
if [ "$MODE" = "java" ]; then
    echo -e "Entry Class: ${GREEN}$ENTRY_CLASS${NC}"
fi
echo ""

# ===========================================
# JAVA DEPLOYMENT FUNCTION
# ===========================================
deploy_java() {
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo -e "  JAVA DEPLOYMENT"
    echo -e "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""

    # Step 1: Build the project
    if [ "$SKIP_BUILD" = false ]; then
        echo -e "${YELLOW}Step 1/4: Building fraud-detection module...${NC}"
        cd "$FRAUD_DETECTION_MODULE"

        if ! mvn clean package -DskipTests; then
            echo -e "${RED}Error: Maven build failed${NC}"
            return 1
        fi

        echo -e "${GREEN}✓ Build successful${NC}"
        echo ""
    else
        echo -e "${YELLOW}Step 1/4: Skipping build (using existing JAR)${NC}"
        echo ""
    fi

    # Step 2: Verify JAR exists
    echo -e "${YELLOW}Step 2/4: Verifying JAR file...${NC}"
    if [ ! -f "$JAR_FILE" ]; then
        echo -e "${RED}Error: JAR file not found: $JAR_FILE${NC}"
        echo "Try running without --skip-build flag"
        return 1
    fi

    JAR_SIZE=$(du -h "$JAR_FILE" | cut -f1)
    echo -e "JAR file: ${GREEN}$(basename "$JAR_FILE")${NC}"
    echo -e "Size: ${GREEN}$JAR_SIZE${NC}"
    echo -e "${GREEN}✓ JAR file verified${NC}"
    echo ""

    # Step 3: Check Flink cluster
    echo -e "${YELLOW}Step 3/4: Checking Flink cluster...${NC}"
    if ! sudo docker ps --format '{{.Names}}' | grep -q "^${FLINK_CONTAINER}$"; then
        echo -e "${RED}Error: Flink JobManager container is not running${NC}"
        echo ""
        echo "Start the infrastructure with:"
        echo "  cd $PROJECT_ROOT/infrastructure"
        echo "  sudo docker compose up -d"
        return 1
    fi

    # Check if TaskManager is also running
    if ! sudo docker ps --format '{{.Names}}' | grep -q "flink-taskmanager"; then
        echo -e "${YELLOW}Warning: Flink TaskManager may not be running${NC}"
    fi

    echo -e "${GREEN}✓ Flink cluster is running${NC}"
    echo ""

    # Step 4: Deploy to Flink
    echo -e "${YELLOW}Step 4/4: Deploying to Flink cluster...${NC}"
    echo ""

    # Copy JAR to container
    echo -e "  ${BLUE}→${NC} Copying JAR to Flink JobManager..."
    FLINK_JOBS_DIR="/opt/flink/usrlib"
    sudo docker exec "$FLINK_CONTAINER" mkdir -p "$FLINK_JOBS_DIR"
    sudo docker cp "$JAR_FILE" "$FLINK_CONTAINER:$FLINK_JOBS_DIR/$(basename "$JAR_FILE")"
    echo -e "  ${GREEN}✓${NC} JAR copied"
    echo ""

    # Build flink run command
    FLINK_CMD="flink run $DETACHED -p $PARALLELISM -c $ENTRY_CLASS $FLINK_JOBS_DIR/$(basename "$JAR_FILE")"

    echo -e "  ${BLUE}→${NC} Submitting job to Flink..."
    echo -e "  Command: ${BLUE}$FLINK_CMD${NC}"
    echo ""

    # Submit the job
    if sudo docker exec "$FLINK_CONTAINER" $FLINK_CMD; then
        echo ""
        echo -e "${GREEN}==========================================="
        echo -e "✓ Java Job Deployed Successfully!"
        echo -e "===========================================${NC}"
        echo ""
        echo -e "${BLUE}Job Details:${NC}"
        echo -e "  Language:    ${GREEN}Java${NC}"
        echo -e "  Entry Class: ${GREEN}$ENTRY_CLASS${NC}"
        echo -e "  Parallelism: ${GREEN}$PARALLELISM${NC}"
        echo ""
        echo -e "${BLUE}Kafka Topics:${NC}"
        echo -e "  Input:  ${GREEN}transactions${NC}"
        echo -e "  Output: ${GREEN}fraud-alerts-stateful${NC}"
        echo ""
        echo -e "${BLUE}Fraud Detection Rule:${NC}"
        echo -e "  A fraud alert is raised when a small transaction (< 100)"
        echo -e "  is followed by a large transaction (> 50,000) from the same account."
        echo ""
        echo -e "${BLUE}Monitoring:${NC}"
        echo -e "  Flink Web UI: ${YELLOW}http://localhost:8081${NC}"
        echo -e "  Conduktor UI: ${YELLOW}http://localhost:8080${NC}"
        echo ""
        return 0
    else
        echo ""
        echo -e "${RED}==========================================="
        echo -e "✗ Java Deployment Failed!"
        echo -e "===========================================${NC}"
        echo ""
        echo -e "Check the Flink logs for details:"
        echo -e "  sudo docker logs flink-jobmanager"
        return 1
    fi
}

# ===========================================
# PYTHON DEPLOYMENT FUNCTION
# ===========================================
deploy_python() {
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo -e "  PYTHON DEPLOYMENT (PyFlink to Cluster)"
    echo -e "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""

    # Step 1: Check Python files exist
    echo -e "${YELLOW}Step 1/5: Verifying Python files...${NC}"
    if [ ! -f "$PYTHON_SCRIPT" ]; then
        echo -e "${RED}Error: Python script not found: $PYTHON_SCRIPT${NC}"
        return 1
    fi
    echo -e "Script: ${GREEN}$(basename "$PYTHON_SCRIPT")${NC}"
    echo -e "${GREEN}✓ Python script verified${NC}"
    echo ""

    # Step 2: Check Kafka connector JAR
    echo -e "${YELLOW}Step 2/5: Checking Kafka connector...${NC}"
    if [ ! -f "$KAFKA_CONNECTOR_JAR" ]; then
        echo -e "  ${YELLOW}Warning: Kafka connector JAR not found at $KAFKA_CONNECTOR_JAR${NC}"
        echo -e "  ${BLUE}→${NC} Attempting to download..."
        mkdir -p "$(dirname "$KAFKA_CONNECTOR_JAR")"
        wget -q -O "$KAFKA_CONNECTOR_JAR" \
            "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar" || {
            echo -e "${RED}Error: Failed to download Kafka connector${NC}"
            return 1
        }
    fi
    echo -e "  Kafka connector: ${GREEN}$(basename "$KAFKA_CONNECTOR_JAR")${NC}"
    echo -e "${GREEN}✓ Kafka connector ready${NC}"
    echo ""

    # Step 3: Check Flink cluster
    echo -e "${YELLOW}Step 3/5: Checking Flink cluster...${NC}"
    if ! sudo docker ps --format '{{.Names}}' | grep -q "^${FLINK_CONTAINER}$"; then
        echo -e "${RED}Error: Flink JobManager container is not running${NC}"
        echo ""
        echo "Start the infrastructure with:"
        echo "  cd $PROJECT_ROOT/infrastructure"
        echo "  sudo docker compose up -d"
        return 1
    fi
    echo -e "${GREEN}✓ Flink cluster is running${NC}"
    echo ""

    # Step 4: Package Python files and copy to Flink container
    echo -e "${YELLOW}Step 4/5: Packaging and copying Python files to Flink...${NC}"

    FLINK_PYTHON_DIR="/opt/flink/usrlib/python"

    # Create directory in container
    echo -e "  ${BLUE}→${NC} Preparing Flink container..."
    sudo docker exec "$FLINK_CONTAINER" mkdir -p "$FLINK_PYTHON_DIR"
    sudo docker exec "$FLINK_CONTAINER" rm -rf "$FLINK_PYTHON_DIR"/*

    # Copy Python directories directly (no zip needed)
    echo -e "  ${BLUE}→${NC} Copying Python files to Flink JobManager..."
    cd "$PYTHON_DIR"

    # Copy each directory separately
    for dir in config model processor serde scripts; do
        if [ -d "$dir" ]; then
            sudo docker cp "$dir" "$FLINK_CONTAINER:$FLINK_PYTHON_DIR/"
        fi
    done

    # Copy the Kafka connector JAR
    echo -e "  ${BLUE}→${NC} Copying Kafka connector JAR..."
    sudo docker exec "$FLINK_CONTAINER" mkdir -p "$FLINK_PYTHON_DIR/lib"
    sudo docker cp "$KAFKA_CONNECTOR_JAR" "$FLINK_CONTAINER:$FLINK_PYTHON_DIR/lib/"

    echo -e "  ${GREEN}✓${NC} Python files copied"
    echo ""

    # Step 5: Submit PyFlink job to cluster
    echo -e "${YELLOW}Step 5/5: Submitting PyFlink job to Flink cluster...${NC}"
    echo ""

    PYTHON_ENTRY="$FLINK_PYTHON_DIR/scripts/stateful_fraud_detection_demo.py"
    PYTHON_FILES="$FLINK_PYTHON_DIR"
    JAR_PATH="$FLINK_PYTHON_DIR/lib/$(basename "$KAFKA_CONNECTOR_JAR")"

    # Build flink run command for Python
    FLINK_CMD="flink run $DETACHED -py $PYTHON_ENTRY -pyfs $PYTHON_FILES -j $JAR_PATH -p $PARALLELISM"

    echo -e "  ${BLUE}→${NC} Submitting job to Flink..."
    echo -e "  Command: ${BLUE}$FLINK_CMD${NC}"
    echo ""

    # Submit the job
    if sudo docker exec "$FLINK_CONTAINER" $FLINK_CMD; then
        echo ""
        echo -e "${GREEN}==========================================="
        echo -e "✓ Python Job Deployed Successfully!"
        echo -e "===========================================${NC}"
        echo ""
        echo -e "${BLUE}Job Details:${NC}"
        echo -e "  Language:    ${GREEN}Python (PyFlink)${NC}"
        echo -e "  Script:      ${GREEN}stateful_fraud_detection_demo.py${NC}"
        echo -e "  Parallelism: ${GREEN}$PARALLELISM${NC}"
        echo ""
        echo -e "${BLUE}Kafka Topics:${NC}"
        echo -e "  Input:  ${GREEN}transactions${NC}"
        echo -e "  Output: ${GREEN}fraud-alerts-stateful${NC}"
        echo ""
        echo -e "${BLUE}Monitoring:${NC}"
        echo -e "  Flink Web UI: ${YELLOW}http://localhost:8081${NC}"
        echo -e "  Conduktor UI: ${YELLOW}http://localhost:8080${NC}"
        echo ""
        return 0
    else
        echo ""
        echo -e "${RED}==========================================="
        echo -e "✗ Python Deployment Failed!"
        echo -e "===========================================${NC}"
        echo ""
        echo -e "${YELLOW}Common issues:${NC}"
        echo -e "  1. PyFlink not installed in Flink container"
        echo -e "  2. Python dependencies missing"
        echo -e "  3. Kafka broker not reachable"
        echo ""
        echo -e "${BLUE}Check logs:${NC}"
        echo -e "  sudo docker logs flink-jobmanager"
        return 1
    fi
}

# ===========================================
# COMPARISON MODE FUNCTION
# ===========================================
run_comparison() {
    echo -e "${CYAN}==========================================="
    echo -e "  JAVA vs PYTHON COMPARISON MODE"
    echo -e "===========================================${NC}"
    echo ""
    echo -e "${YELLOW}This will run both implementations sequentially.${NC}"
    echo -e "Press Ctrl+C at any time to stop."
    echo ""

    # Run Java first
    echo -e "${BLUE}▶ Starting JAVA deployment...${NC}"
    echo ""
    deploy_java
    JAVA_RESULT=$?

    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""

    # Then run Python
    echo -e "${BLUE}▶ Starting PYTHON deployment...${NC}"
    echo ""
    deploy_python
    PYTHON_RESULT=$?

    # Summary
    echo ""
    echo -e "${CYAN}==========================================="
    echo -e "  COMPARISON SUMMARY"
    echo -e "===========================================${NC}"
    echo ""
    echo -e "  Java:   $([ $JAVA_RESULT -eq 0 ] && echo -e "${GREEN}✓ Success${NC}" || echo -e "${RED}✗ Failed${NC}")"
    echo -e "  Python: $([ $PYTHON_RESULT -eq 0 ] && echo -e "${GREEN}✓ Success${NC}" || echo -e "${RED}✗ Failed${NC}")"
    echo ""
    echo -e "${BLUE}Both jobs are deployed to the Flink cluster.${NC}"
    echo ""
    echo -e "${BLUE}Monitoring:${NC}"
    echo -e "  Flink Web UI: ${YELLOW}http://localhost:8081${NC}"
    echo -e "  Conduktor UI: ${YELLOW}http://localhost:8080${NC}"
    echo ""
    echo -e "${BLUE}Key Differences:${NC}"
    echo -e "  ┌─────────────┬──────────────────────────┬──────────────────────────┐"
    echo -e "  │ Aspect      │ Java                     │ Python                   │"
    echo -e "  ├─────────────┼──────────────────────────┼──────────────────────────┤"
    echo -e "  │ Execution   │ Flink Cluster            │ Flink Cluster            │"
    echo -e "  │ Packaging   │ JAR (shaded)             │ ZIP (python files)       │"
    echo -e "  │ Serializer  │ Compile-time type safe   │ Runtime type checking    │"
    echo -e "  │ State       │ Native Flink state       │ State via Py4J bridge    │"
    echo -e "  │ Output Topic│ fraud-alerts-stateful    │ fraud-alerts-stateful    │"
    echo -e "  └─────────────┴──────────────────────────┴──────────────────────────┘"
    echo ""
}

# ===========================================
# MAIN EXECUTION
# ===========================================
case "$MODE" in
    java)
        deploy_java
        ;;
    python)
        deploy_python
        ;;
    both)
        run_comparison
        ;;
    *)
        echo -e "${RED}Unknown mode: $MODE${NC}"
        usage
        ;;
esac

