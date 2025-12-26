# Demo Execution Guide

## Prerequisites

```bash
cd infrastructure && sudo docker compose up -d
sudo docker ps
```

**URLs**: Flink UI http://localhost:8081 | Conduktor http://localhost:8080 (admin@conduktor.io / adminP4ss!)

## Demo 1: Data Generator

```bash
# IDE: Run TransactionDataGeneratorApp with args: fraud
# Cluster:
./fraud-detection/app/java/deploy-data-generator.sh -p fraud -d
```

Profiles: `default`, `fraud`, `high`, `low`, `demo`

## Demo 2: Naive Fraud Detection

```bash
# IDE: Run NaiveFraudDetectionDemo
```

## Demo 3: Stateful Fraud Detection

```bash
# IDE: Run StatefulFraudDetectionDemoApp
# Cluster:
./fraud-detection/app/java/deploy-stateful-fraud-detection.sh --java -d
```

## Demo 4: Timer Reporting

```bash
# IDE: Run TimerReportingDemoApp
```

## Build

```bash
cd fraud-detection && mvn clean package -DskipTests
```

## Commands

```bash
sudo docker logs -f flink-jobmanager          # Logs
sudo docker exec flink-jobmanager flink list  # List jobs
sudo docker exec flink-jobmanager flink cancel <job-id>  # Cancel
cd infrastructure && sudo docker compose down  # Stop all
```
