#!/bin/bash

# ===========================================
# Transaction Data Generator Deployment Script
# ===========================================
#
# This script:
# 1. Builds the fraud-detection module with Maven
# 2. Packages the TransactionDataGeneratorApp
# 3. Deploys it to the Flink cluster with parallelism of 3

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FRAUD_DETECTION_MODULE="$PROJECT_ROOT/fraud-detection"
JAR_FILE="$FRAUD_DETECTION_MODULE/target/fraud-detection-1.0-SNAPSHOT.jar"
ENTRY_CLASS="com.wslimi.demo.fraud.TransactionDataGeneratorApp"
PARALLELISM=1
FLINK_CONTAINER="flink-jobmanager"

# Parse command line arguments
PROFILE="default"
DETACHED=""
SKIP_BUILD=false

usage() {
    echo -e "${YELLOW}Usage:${NC} $0 [options]"
    echo ""
    echo "Options:"
    echo "  -p, --profile PROFILE    Configuration profile (default, fraud, high, low, demo)"
    echo "                           Default: default"
    echo "  -d, --detached           Run job in detached mode"
    echo "  -s, --skip-build         Skip Maven build (use existing JAR)"
    echo "  -h, --help               Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                       # Deploy with default profile"
    echo "  $0 -p fraud              # Deploy with fraud test profile"
    echo "  $0 -p demo -d            # Deploy with demo profile in detached mode"
    echo "  $0 -s                    # Skip build and deploy existing JAR"
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--profile)
            PROFILE="$2"
            shift 2
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

# Validate profile
case $PROFILE in
    default|fraud|high|low|demo)
        ;;
    *)
        echo -e "${RED}Error: Invalid profile '$PROFILE'${NC}"
        echo "Valid profiles: default, fraud, high, low, demo"
        exit 1
        ;;
esac

echo -e "${BLUE}===========================================\n"
echo -e "Transaction Data Generator Deployment"
echo -e "===========================================${NC}"
echo -e "Profile: ${GREEN}$PROFILE${NC}"
echo -e "Parallelism: ${GREEN}$PARALLELISM${NC}"
echo -e "Detached: ${GREEN}${DETACHED:-no}${NC}"
echo ""

# Step 1: Build the project
if [ "$SKIP_BUILD" = false ]; then
    echo -e "${YELLOW}Step 1/4: Building fraud-detection module...${NC}"
    cd "$FRAUD_DETECTION_MODULE"

    if ! mvn clean package -DskipTests; then
        echo -e "${RED}Error: Maven build failed${NC}"
        exit 1
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
    exit 1
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
    exit 1
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
FLINK_CMD="flink run $DETACHED -p $PARALLELISM -c $ENTRY_CLASS $FLINK_JOBS_DIR/$(basename "$JAR_FILE") $PROFILE"

echo -e "  ${BLUE}→${NC} Submitting job to Flink..."
echo -e "  Command: ${BLUE}$FLINK_CMD${NC}"
echo ""

# Submit the job
if sudo docker exec "$FLINK_CONTAINER" $FLINK_CMD; then
    echo ""
    echo -e "${GREEN}===========================================\n"
    echo -e "✓ Job Deployed Successfully!"
    echo -e "===========================================${NC}"
    echo ""
    echo -e "${BLUE}Job Details:${NC}"
    echo -e "  Entry Class: ${GREEN}$ENTRY_CLASS${NC}"
    echo -e "  Profile: ${GREEN}$PROFILE${NC}"
    echo -e "  Parallelism: ${GREEN}$PARALLELISM${NC}"
    echo ""
    echo -e "${BLUE}Monitoring:${NC}"
    echo -e "  Flink Web UI: ${YELLOW}http://localhost:8081${NC}"
    echo -e "  Conduktor UI: ${YELLOW}http://localhost:8080${NC} (kafka topic: transactions)"
    echo ""
    echo -e "${BLUE}Profile Configuration:${NC}"
    case $PROFILE in
        default)
            echo -e "  Messages/sec: 10"
            echo -e "  Accounts: 100"
            echo -e "  Fraud patterns: Disabled"
            ;;
        fraud)
            echo -e "  Messages/sec: 5"
            echo -e "  Accounts: 50"
            echo -e "  Fraud patterns: Enabled (10% probability)"
            ;;
        high)
            echo -e "  Messages/sec: 100"
            echo -e "  Accounts: 1000"
            echo -e "  Fraud patterns: Enabled (2% probability)"
            ;;
        low)
            echo -e "  Messages/sec: 1"
            echo -e "  Total messages: 100"
            echo -e "  Accounts: 10"
            ;;
        demo)
            echo -e "  Messages/sec: 5"
            echo -e "  Accounts: 20"
            echo -e "  Fraud patterns: Enabled (15% probability)"
            ;;
    esac
    echo ""
    echo -e "${BLUE}To stop the job:${NC}"
    echo -e "  1. Go to Flink Web UI: http://localhost:8081"
    echo -e "  2. Click on 'Running Jobs'"
    echo -e "  3. Select 'Transaction Data Generator'"
    echo -e "  4. Click 'Cancel'"
    echo ""
else
    echo ""
    echo -e "${RED}===========================================\n"
    echo -e "✗ Deployment Failed!"
    echo -e "===========================================${NC}"
    echo ""
    echo -e "Check the Flink logs for details:"
    echo -e "  sudo docker logs flink-jobmanager"
    exit 1
fi

