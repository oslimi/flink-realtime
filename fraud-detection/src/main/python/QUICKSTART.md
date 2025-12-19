# PyFlink Quick Reference

## Prerequisites

- **Python 3.11** (recommended) or Python 3.10
- **Java 11** - Required by Apache Flink
- **Docker** - For Kafka, Flink, and PostgreSQL

> ⚠️ **Python 3.12 Note**: Python 3.12 removed the `distutils` module which causes compatibility issues with `apache-flink` dependencies. Use Python 3.11 or 3.10 instead.

## Installation

> 🔑 **Important**: Always use a virtual environment! Never install packages system-wide with `pip`. You should see `(venv)` in your terminal prompt after activating.

### Option 1: Using Python 3.11 (Recommended)

```bash
cd fraud-detection/src/main/python

# Create virtual environment with Python 3.11
python3.11 -m venv venv
source venv/bin/activate

# Upgrade pip and install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Setup Flink Connector JARs (required for Kafka and JDBC)
mkdir -p lib
wget -P lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
wget -P lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar
wget -P lib https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
```

### Option 2: If you only have Python 3.12

```bash
cd fraud-detection/src/main/python

# Install Python 3.11
sudo apt install python3.11

# Create virtual environment with Python 3.11
python3.11 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Setup Flink Connector JARs (required for Kafka and JDBC)
mkdir -p lib
wget -P lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
wget -P lib https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar
wget -P lib https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
```

### Verify Installation

After installation, verify everything is set up correctly:

```bash
# 1. Check you're in the virtual environment (should see "(venv)" prefix)
# Your prompt should look like: (venv) user@host:~$

# 2. Verify Python version (use python3 if python command not found)
python3 --version  # Should show Python 3.11.x or 3.10.x

# 3. Verify PyFlink is installed
python3 -c "import pyflink; print(pyflink.__version__)"  # Should print version number

# 4. Check Java is available
java -version  # Should show Java 11
```

## Run Demos

```bash
# Demo 1: Kafka Source
python3 scripts/kafka_source_demo.py

# Demo 2: Naive Fraud (Stateless)
python3 scripts/naive_fraud_detection_demo.py

# Demo 3: Stateful Fraud (ValueState)
python3 scripts/stateful_fraud_detection_demo.py

# Demo 4: Timer Reports (ListState + Timers)
python3 scripts/timer_reporting_demo.py

# Demo 5: JDBC Sink (PostgreSQL)
python3 scripts/jdbc_sink_demo.py
```

## Send Test Data

```bash
# Start Kafka producer
sudo docker exec -it broker kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic transactions

# Send test messages:
{"transactionId":"tx-001","srcAccountId":"acc-123","destAccountId":"acc-456","amount":50.0,"currency":"EUR","eventTime":1702900000000}
{"transactionId":"tx-002","srcAccountId":"acc-123","destAccountId":"acc-789","amount":75000.0,"currency":"EUR","eventTime":1702900001000}
```

## Access Services

- **Flink UI**: http://localhost:8082
- **Conduktor**: http://localhost:8080 (admin@admin.io / admin)
- **PostgreSQL**: localhost:5432 (app_user / app_password)

## File Structure

```
src/main/python/
├── model/               # Data models
├── processor/           # Fraud detectors
├── serde/              # Serialization
├── scripts/            # Demos
├── config/             # Configuration
└── requirements.txt    # Dependencies
```

## Key Concepts Covered

1. **Kafka Source** - Reading from Kafka topics
2. **Stateless Processing** - KeyedProcessFunction
3. **State Management** - ValueState for pattern detection
4. **Timers** - Processing time timers for aggregation
5. **JDBC Sink** - Writing to PostgreSQL database

## Troubleshooting

**python: command not found**: On most Linux systems, use `python3` instead of `python`. All commands in this guide use `python3`.

**error: externally-managed-environment**: You're trying to install packages without activating the virtual environment. Make sure you run `source venv/bin/activate` first. You should see `(venv)` prefix in your terminal prompt.

**BindException: Could not start rest endpoint on any port in port range 8082**: Port 8082 is already in use. The demos now disable the Web UI by default. If you need the Web UI, kill any existing Flink process with `pkill -f flink` or change the port in `config/flink_config.py`.

**NullPointerException or ClassCastException with Kafka sink**: PyFlink has known issues with Python-to-Java serialization for Kafka sinks. The demos now print alerts to console instead of writing to Kafka. For production use, consider using the kafka-python library directly or Flink's Table API.

**Could not found the Java class 'org.apache.flink.connector.kafka.source.KafkaSource'**: The Kafka connector JAR is missing. Make sure you've downloaded the JARs to the `lib/` directory (see Installation section).

**No module 'pyflink'**: Activate the virtual environment (`source venv/bin/activate`) then run `pip install -r requirements.txt`

**JAVA_HOME not set**: `export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`

**Connection refused**: Start the infrastructure with `sudo docker compose up -d` in the infrastructure/ directory

**ModuleNotFoundError: No module named 'distutils'**: You're using Python 3.12. Remove the venv (`rm -rf venv`) and recreate it with Python 3.11 (`python3.11 -m venv venv`)

**numpy compatibility error**: This is caused by Python 3.12. Switch to Python 3.11 or 3.10

**python3.11: command not found**: Install Python 3.11 with `sudo apt install python3.11`  

## Documentation

- **SETUP.md** - Detailed installation guide
- **README.md** - Overview and structure
- **PYFLINK_COMPLETE.md** - Implementation summary
- **JAVA_VS_PYTHON.md** - Side-by-side comparison

