# Streaming Demo Architecture

A local development environment for streaming demos with Kafka, Flink, and PostgreSQL.

## Architecture Overview

This project provides a complete local streaming infrastructure using Docker Compose, designed to support real-time data pipeline development with Apache Flink.

### Components

- **Kafka Broker**: Message streaming platform (KRaft mode - no Zookeeper required)
- **Conduktor UI**: Kafka management and monitoring web interface
- **Apache Flink**: Distributed stream processing framework
- **PostgreSQL**: Database for metadata and application data persistence

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Docker Network (streaming-network)                    │
│                                                                          │
│  ┌─────────────┐     ┌─────────────────────┐     ┌─────────────────┐    │
│  │   Kafka     │     │   Conduktor Console │     │   PostgreSQL    │    │
│  │   Broker    │◄────│   (Kafka UI)        │────►│   (Metadata)    │    │
│  │   :9092     │     │   :8080             │     │   :5432         │    │
│  └──────┬──────┘     └─────────────────────┘     └─────────────────┘    │
│         │                      │                                         │
│         │            ┌─────────┴───────────┐                            │
│         │            │  Conduktor          │                            │
│         │            │  Monitoring         │                            │
│         │            │  (Cortex)           │                            │
│         │            └─────────────────────┘                            │
│         │                                                                │
│         ▼                                                                │
│  ┌─────────────────────────────────────────┐                            │
│  │           Apache Flink Cluster          │                            │
│  │  ┌─────────────┐    ┌─────────────────┐ │                            │
│  │  │ JobManager  │◄──►│  TaskManager    │ │                            │
│  │  │   :8081     │    │  (4 slots)      │ │                            │
│  │  └─────────────┘    └─────────────────┘ │                            │
│  └─────────────────────────────────────────┘                            │
└─────────────────────────────────────────────────────────────────────────┘
```

### Docker Services

| Service | Image | Port | Description |
|---------|-------|------|-------------|
| **broker** | `apache/kafka:latest` | 9092 | Kafka broker in KRaft mode |
| **postgresql** | `postgres:14` | 5432 | PostgreSQL database |
| **conduktor-console** | `conduktor/conduktor-console:1.41.0` | 8080 | Kafka management UI |
| **conduktor-monitoring** | `conduktor/conduktor-console-cortex:1.41.0` | - | Metrics collection |
| **flink-jobmanager** | `flink:1.20` | 8081 | Flink cluster coordinator |
| **flink-taskmanager** | `flink:1.20` | - | Flink worker (4 task slots) |

## Project Structure

```
streaming/
├── pom.xml                     # Parent Maven POM
├── README.md                   # This file
├── infrastructure/             # Docker infrastructure
│   ├── docker-compose.yaml     # Docker Compose orchestration
│   ├── .env                    # Environment variables
│   ├── kafka/                  # Kafka configurations
│   ├── flink/                  # Flink configurations
│   ├── postgres/               # PostgreSQL init scripts
│   └── conduktor/              # Conduktor UI configs
├── basic-fraud-detection/      # Fraud detection Flink job
│   ├── pom.xml
│   └── src/
├── scripts/                    # Utility scripts
│   ├── start.sh                # Start infrastructure
│   ├── stop.sh                 # Stop infrastructure
│   └── deploy-flink-job.sh     # Deploy Flink job
├── data/                       # Sample data
│   ├── input/
│   └── output/
└── docs/                       # Documentation
```

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Java 17+
- Maven 3.8+

### 1. Start the Infrastructure

```bash
cd infrastructure
docker compose up -d
```

### 2. Verify Services

```bash
docker compose ps
```

### 3. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Conduktor UI** | http://localhost:8080 | Default login |
| **Flink Dashboard** | http://localhost:8081 | No auth |
| **PostgreSQL** | localhost:5432 | `conduktor` / `conduktor_password` |
| **Kafka** | localhost:9092 | No auth |

### 4. Stop Infrastructure

```bash
docker compose down        # Stop containers
docker compose down -v     # Stop and remove volumes
```

## Using the Infrastructure for Flink Data Pipelines

### Creating Kafka Topics

**Using Conduktor UI:**
1. Open http://localhost:8080
2. Navigate to Topics → Create Topic
3. Configure topic name, partitions, replication factor

**Using CLI:**
```bash
docker exec -it broker /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic transactions \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Producing Test Messages

```bash
docker exec -it broker /opt/kafka/bin/kafka-console-producer.sh \
  --topic transactions \
  --bootstrap-server localhost:9092
```

Example JSON message:
```json
{"transactionId":"txn-001","srcAccountId":"acc-123","destAccountId":"acc-456","amount":150.00,"currency":"USD","eventTime":1702900000000}
```

### Building Flink Jobs

```bash
# Build the fraud detection job
mvn clean package -pl basic-fraud-detection -am

# The JAR will be at:
# basic-fraud-detection/target/basic-fraud-detection-1.0-SNAPSHOT.jar
```

### Deploying Flink Jobs

**Using the deploy script:**
```bash
./scripts/deploy-flink-job.sh \
  basic-fraud-detection/target/basic-fraud-detection-1.0-SNAPSHOT.jar \
  com.wslimi.streaming.basic.fraud.processor.FlinkFraudDetection
```

**Using Flink REST API:**
```bash
# Upload JAR
curl -X POST -H "Expect:" \
  -F "jarfile=@basic-fraud-detection/target/basic-fraud-detection-1.0-SNAPSHOT.jar" \
  http://localhost:8081/jars/upload

# Run the job (use the JAR ID from upload response)
curl -X POST http://localhost:8081/jars/<jar-id>/run
```

**Using Flink CLI inside container:**
```bash
docker exec -it flink-jobmanager flink run \
  /opt/flink/data/basic-fraud-detection-1.0-SNAPSHOT.jar
```

### Monitoring Jobs

1. Open Flink Dashboard: http://localhost:8081
2. View running jobs under "Running Jobs"
3. Check task managers, metrics, and logs

### Network Configuration

When developing Flink jobs:

```java
// For jobs running INSIDE Docker (deployed to cluster)
public static final String KAFKA_BOOTSTRAP = "broker:29092";

// For jobs running OUTSIDE Docker (local IDE)
public static final String KAFKA_BOOTSTRAP = "localhost:9092";
```

## Troubleshooting

### View Container Logs

```bash
docker compose logs -f broker
docker compose logs -f flink-jobmanager
docker compose logs -f conduktor-console
```

### Restart Services

```bash
docker compose restart broker
docker compose restart flink-jobmanager flink-taskmanager
```

### Common Issues

| Issue | Solution |
|-------|----------|
| Kafka broker not starting | Wait 30s for startup, check logs |
| Flink cannot connect to Kafka | Use `broker:29092` inside Docker, `localhost:9092` outside |
| Conduktor UI blank | Ensure PostgreSQL is healthy first |
| Job fails with serialization error | Check all POJOs implement `Serializable` |

### Reset Environment

```bash
docker compose down -v
docker compose up -d
```

## License

MIT

