# PyFlink Setup and Execution Guide

## Prerequisites

- Python 3.8+ (3.8, 3.9, or 3.10 recommended)
- pip or conda
- Java 11+ (required by PyFlink)
- Docker and Docker Compose (for infrastructure)

## Installation Steps

### 1. Navigate to Python Directory

```bash
cd fraud-detection/src/main/python
```

### 2. Create Virtual Environment (Recommended)

```bash
# Using venv
python3 -m venv venv
source venv/bin/activate  # On Linux/Mac
# venv\Scripts\activate   # On Windows

# Or using conda
conda create -n pyflink python=3.9
conda activate pyflink
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Verify Java Installation

PyFlink requires Java to be installed:

```bash
java -version  # Should show Java 11+
```

If Java is not installed:
```bash
# Ubuntu/Debian
sudo apt install openjdk-11-jdk

# macOS
brew install openjdk@11
```

### 5. Start Infrastructure

From the project root:

```bash
cd ../../../infrastructure
sudo docker compose up -d
```

Verify services are running:
```bash
sudo docker compose ps
```

## Running Demos

### Demo 1: Kafka Source

```bash
cd fraud-detection/src/main/python
python scripts/kafka_source_demo.py
```

### Demo 2: Naive Fraud Detection

```bash
python scripts/naive_fraud_detection_demo.py
```

### Demo 3: Stateful Fraud Detection

```bash
python scripts/stateful_fraud_detection_demo.py
```

### Demo 4: Timer Reporting

```bash
python scripts/timer_reporting_demo.py
```

### Demo 5: JDBC Sink

```bash
python scripts/jdbc_sink_demo.py
```

## Sending Test Messages

Use the Kafka console producer:

```bash
# Start producer
sudo docker exec -it broker kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic transactions

# Paste these messages (one per line):
{"transactionId":"tx-001","srcAccountId":"acc-123","destAccountId":"acc-456","amount":50.0,"currency":"EUR","eventTime":1702900000000}
{"transactionId":"tx-002","srcAccountId":"acc-123","destAccountId":"acc-789","amount":75000.0,"currency":"EUR","eventTime":1702900001000}
```

Or use Conduktor UI at http://localhost:8080

## Accessing Services

- **Flink Web UI**: http://localhost:8082
- **Conduktor UI**: http://localhost:8080 (login: admin@admin.io / admin)
- **PostgreSQL**: localhost:5432 (user: app_user, password: app_password)

## Troubleshooting

### Issue: "No module named 'pyflink'"

**Solution**: Make sure you installed requirements in your virtual environment:
```bash
source venv/bin/activate
pip install -r requirements.txt
```

### Issue: "JAVA_HOME is not set"

**Solution**: Set JAVA_HOME environment variable:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64  # Adjust path
export PATH=$JAVA_HOME/bin:$PATH
```

### Issue: "Connection refused" to Kafka

**Solution**: Ensure Docker services are running:
```bash
cd ../../../infrastructure
sudo docker compose ps
sudo docker compose up -d broker
```

### Issue: JDBC demo fails

**Solution**: PyFlink JDBC requires additional JARs. Download:
1. flink-connector-jdbc JAR
2. PostgreSQL JDBC driver JAR

Place them in `$FLINK_HOME/lib/` or specify with:
```bash
python scripts/jdbc_sink_demo.py \
  --jarfile /path/to/flink-connector-jdbc.jar \
  --jarfile /path/to/postgresql.jar
```

## Comparing with Java Implementation

Run both Java and Python demos simultaneously:

**Terminal 1 (Java)**:
```bash
cd fraud-detection
mvn clean package
java -cp target/fraud-detection-1.0-SNAPSHOT.jar \
  com.wslimi.demo.fraud.NaiveFraudDetectionDemo
```

**Terminal 2 (Python)**:
```bash
cd fraud-detection/src/main/python
python scripts/naive_fraud_detection_demo.py
```

Both will process from the same Kafka topic using different consumer groups!

## Performance Notes

- PyFlink has slightly higher memory overhead than Java
- For production, Java implementation is recommended
- Python is excellent for prototyping and ML integration
- Both implementations produce identical results

## Next Steps

1. Modify fraud detection thresholds in processor files
2. Add new processors for different patterns
3. Integrate ML models using ONNX or TensorFlow
4. Explore Table API for SQL-like queries
5. Add custom metrics and monitoring

