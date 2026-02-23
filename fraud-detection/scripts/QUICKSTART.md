# Quick Start Guide

## Setup

Build the project:
```bash
mvn clean package
```

Start infrastructure:
```bash
cd infrastructure
docker compose up -d
```

## Run Demos

Go to scripts folder:
```bash
cd fraud-detection/scripts
```

### Example 1: Simple Demo

Terminal 1:
```bash
./generator.sh 3
```

Terminal 2:
```bash
./naive.sh 2
```

Monitor at http://localhost:8081

### Example 2: Full Pipeline

Terminal 1:
```bash
./generator.sh 3
```

Terminal 2:
```bash
./stateful.sh 2
```

Terminal 3:
```bash
./windowed.sh 2
```

Terminal 4:
```bash
watch -n 2 ./list.sh
```

### Example 3: High Throughput

```bash
./generator.sh 8
./stateful.sh 4
./windowed.sh 4
```

## Stop Jobs

```bash
# List all running jobs
./list.sh

# Stop specific job
./stop.sh <job-id>

# Stop everything
./stopall.sh
```

## Check Output

Kafka topics:
```bash
# Transactions
docker exec broker kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic transactions --max-messages 5

# Fraud alerts
docker exec broker kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic fraud-alerts-stateful --max-messages 5

# Reports
docker exec broker kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic fraud-reports-windowed --max-messages 5
```

PostgreSQL:
```bash
docker exec -it postgresql psql -U app_user -d streaming_demo \
  -c "SELECT * FROM fraud_reports LIMIT 5;"
```

## Learning Path

1. **Start with naive.sh**
   - Understand basic fraud detection
   - Threshold-based rules

2. **Try stateful.sh**
   - See pattern detection in action
   - Learn state management

3. **Compare timer.sh vs windowed.sh**
   - Timer: imperative (manual scheduling)
   - Windowed: declarative (automatic grouping)

4. **Monitor in Flink UI**
   - View job graph
   - Check parallelism and tasks
   - Monitor throughput

