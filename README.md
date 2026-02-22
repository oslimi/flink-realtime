<p align="center">
  <img src="https://flink.apache.org/img/flink-header-logo.svg" alt="Apache Flink" width="300"/>
</p>

<h1 align="center">Real-Time Fraud Detection with Apache Flink</h1>

<p align="center">
  <strong>A complete streaming pipeline for detecting fraudulent transactions in real-time</strong>
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#demos">Demos</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#documentation">Documentation</a>
</p>

---

## Overview

This project demonstrates real-time fraud detection using **Apache Flink**, **Apache Kafka**, and **PostgreSQL**. It implements a stateful streaming pipeline that detects suspicious transaction patterns, such as small "card testing" transactions followed by large withdrawals.

The demo is designed as a step-by-step learning resource, progressively introducing Flink concepts from basic transformations to advanced stateful processing with timers.

## Features

- 🔄 **Real-time Processing** — Sub-second fraud detection on streaming transactions
- 🧠 **Stateful Detection** — Pattern recognition using Flink's managed state
- ⏱️ **Timer-based Reporting** — Periodic aggregation with processing time timers
- 📊 **Dual Implementation** — Both Java and PyFlink versions for comparison
- 🐳 **Containerized Infrastructure** — Complete local environment with Docker Compose

## Quick Start

### Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Docker | 20.10+ | With Docker Compose |
| Java | 17 | JDK |
| Maven | 3.8+ | For building |
| Python | 3.11 | For PyFlink demos |
| Memory | 16GB RAM | Recommended |

### 1. Clone and Start Infrastructure

```sh
git clone https://github.com/oslimi/flink-realtime.git
```

```sh
cd infrastructure && sudo docker compose up -d
```

### 2. Verify Services

```sh
sudo docker ps
```

| Service | URL | Credentials |
|---------|-----|-------------|
| **Flink Dashboard** | http://localhost:8081 | — |
| **Conduktor (Kafka UI)** | http://localhost:8080 | `admin@conduktor.io` / `adminP4ss!` |
| **PostgreSQL** | localhost:5432 | `conduktor` / `change_me` |

### 3. Build the Project

```sh
cd fraud-detection && mvn clean package -DskipTests
```

### 4. Install Python Dependencies (Optional - for PyFlink)

```sh
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 5. Run the Demo

**Terminal 1 — Start the data generator:**

```sh
bash fraud-detection/app/java/deploy-data-generator.sh -p fraud -d
```

**Terminal 2 — Start fraud detection:**

```sh
bash fraud-detection/app/java/deploy-stateful-fraud-detection.sh -d
```

**Monitor results:**
- Open Flink UI at http://localhost:8081 to see running jobs
- Open Conduktor at http://localhost:8080 to view Kafka topics

## Architecture

```
┌─────────────────────┐      ┌────────────────────┐      ┌─────────────────────┐
│   Data Generator    │      │       Kafka        │      │   Fraud Detector    │
│    (Flink Job)      │─────▶│   transactions     │─────▶│    (Flink Job)      │
└─────────────────────┘      └────────────────────┘      └──────────┬──────────┘
                                                                    │
                             ┌────────────────────┐                 │
                             │       Kafka        │◀────────────────┤
                             │   fraud-alerts     │                 │
                             └────────────────────┘                 │
                                                                    │
                             ┌────────────────────┐                 │
                             │     PostgreSQL     │◀────────────────┘
                             │   fraud_reports    │
                             └────────────────────┘
```

### Components

| Component | Description |
|-----------|-------------|
| **Data Generator** | Produces synthetic transactions to Kafka with configurable fraud patterns |
| **Fraud Detector** | Stateful Flink job detecting suspicious patterns using ValueState |
| **Kafka** | Message broker for transactions and alerts |
| **PostgreSQL** | Persists aggregated fraud reports |
| **Conduktor** | Web UI for Kafka monitoring |

## Demos

The project is structured as progressive demos, each introducing new Flink concepts:

### Demo 1: Kafka Source

**File:** `KafkaSourceDemoApp.java`

Learn how to read from Kafka with JSON deserialization.

```sh
bash fraud-detection/app/java/deploy-data-generator.sh -p default -d
```

**Concepts:** KafkaSource, Deserialization, DataStream basics

---

### Demo 2: Naive Fraud Detection

**File:** `NaiveFraudDetectionDemo.java`

Simple threshold-based detection: flag transactions over $10,000.

> Run `NaiveFraudDetectionDemo` from IDE

**Concepts:** Map transformation, Filtering, KafkaSink

---

### Demo 3: Stateful Fraud Detection

**File:** `StatefulFraudDetectionDemoApp.java`

Detect "card testing" attacks using stateful pattern matching.

```sh
bash fraud-detection/app/java/deploy-stateful-fraud-detection.sh -d
```

**Detection Rule:**
```
IF previous_transaction.amount < $100
   AND current_transaction.amount > $50,000
   AND same_account
THEN → FRAUD ALERT
```

**Concepts:** KeyedProcessFunction, ValueState, Keyed streams

---

### Demo 4: Timer-based Reporting

**File:** `TimerReportingDemoApp.java`

Aggregate fraud alerts periodically using processing time timers.

> Run `TimerReportingDemoApp` from IDE

**Concepts:** onTimer callback, Processing time, State accumulation

---

### Demo 5: JDBC Sink

**File:** `JdbcSinkDemoApp.java`

Persist fraud reports to PostgreSQL using JDBC connector.

**Concepts:** JdbcSink, External system integration

## Data Generator Profiles

The data generator supports different transaction patterns:

| Profile | Description | Fraud Rate |
|---------|-------------|------------|
| `default` | Mixed realistic traffic | ~5% |
| `fraud` | High fraud pattern rate | ~30% |
| `high` | Large transactions only | Variable |
| `low` | Small transactions only | Minimal |
| `demo` | Predictable patterns for demos | ~50% |

```sh
bash fraud-detection/app/java/deploy-data-generator.sh -p fraud -d
```

## Project Structure

```
streaming/
├── fraud-detection/
│   ├── app/
│   │   ├── java/                    # Deployment scripts (Java)
│   │   └── python/                  # Deployment scripts (Python)
│   └── src/main/
│       ├── java/com/wslimi/demo/fraud/
│       │   ├── config/              # Flink & Kafka configuration
│       │   ├── datagen/             # Transaction generators
│       │   ├── model/               # POJOs (Transaction, Alert, Report)
│       │   ├── processor/           # KeyedProcessFunction implementations
│       │   ├── serde/               # Serialization schemas
│       │   └── source/              # Kafka source configuration
│       └── python/                  # PyFlink equivalent implementation
│           ├── model/
│           ├── processor/
│           ├── scripts/
│           └── requirements.txt     # Python dependencies
├── infrastructure/
│   └── docker-compose.yaml          # Complete local environment
├── docs/
│   ├── ARCHITECTURE.md
│   ├── EXECUTION.md
│   └── JAVA_VS_PYFLINK.md
├── articles/                        # Technical articles
├── requirements.txt                 # Root Python dependencies
└── scripts/
    ├── start.sh
    └── stop.sh
```

## Java vs PyFlink

This project includes both Java and PyFlink implementations for comparison.

| Aspect | Java | PyFlink |
|--------|------|---------|
| **Performance** | Optimal (native JVM) | 3-5x slower (Py4J overhead) |
| **Packaging** | Single JAR | ZIP + Python deps |
| **State backends** | All supported | Limited options |
| **Cluster setup** | Standard | Requires Python on cluster |
| **Use case** | Production workloads | Prototyping, ML integration |

See [Java vs PyFlink](docs/JAVA_VS_PYFLINK.md) for detailed comparison and benchmarks.

## Commands Reference

### Infrastructure

```sh
cd infrastructure && sudo docker compose up -d
```

```sh
cd infrastructure && sudo docker compose down
```

```sh
sudo docker logs -f flink-jobmanager
```

### Flink Operations

```sh
sudo docker exec flink-jobmanager flink list
```

```sh
sudo docker logs -f flink-taskmanager
```

### Build

```sh
mvn clean package
```

```sh
mvn clean package -DskipTests
```

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/ARCHITECTURE.md) | System design and component overview |
| [Execution Guide](docs/EXECUTION.md) | Step-by-step demo instructions |
| [Java vs PyFlink](docs/JAVA_VS_PYFLINK.md) | Performance comparison and trade-offs |

## Articles

Technical deep-dives based on this project:

1. [Building a Real-Time Fraud Detection System](articles/1_building_fraud_detection.md)
2. [Java vs Python for Apache Flink](articles/2_java_vs_python_flink.md)
3. [The Hidden Cost of "Easy Code"](articles/3_pyflink_performance_cost.md)

## Troubleshooting

<details>
<summary><strong>Docker permission denied</strong></summary>

```sh
sudo usermod -aG docker $USER
```
</details>

<details>
<summary><strong>Parallelism higher than max parallelism</strong></summary>

Set parallelism in your Flink job:
```java
env.setParallelism(1);
env.setMaxParallelism(4);
```
</details>

<details>
<summary><strong>Kafka topic not found</strong></summary>

Topics are auto-created on first message. Ensure the data generator is running:
```sh
bash fraud-detection/app/java/deploy-data-generator.sh -p default -d
```
</details>

<details>
<summary><strong>Class version error (Java 17 vs 11)</strong></summary>

The Flink cluster uses Java 17. Ensure you're compiling with Java 17:
```sh
export JAVA_HOME=/path/to/java17 && mvn clean package
```
</details>

<details>
<summary><strong>java.lang.reflect.InaccessibleObjectException on Java 17+</strong></summary>

Flink's use of Kryo requires certain JVM flags to access internal classes on Java 17. If you encounter an `InaccessibleObjectException` related to `java.util.Arrays$ArrayList`, add the following to your JVM arguments:
```sh
--add-opens java.base/java.util=ALL-UNNAMED \
--add-opens java.base/java.lang=ALL-UNNAMED \
--add-opens java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens java.base/java.nio=ALL-UNNAMED \
--add-opens java.base/java.net=ALL-UNNAMED \
--add-opens java.base/java.sql=ALL-UNNAMED
```
</details>

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the Apache License 2.0 — see the [LICENSE](LICENSE) file for details.

---

<p align="center">
  Built with ❤️ for the Apache Flink community
</p>

