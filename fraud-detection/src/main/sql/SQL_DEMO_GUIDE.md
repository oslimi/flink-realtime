# Flink SQL Real-Time Fraud Detection Demo

## Prerequisites

1. Docker infrastructure running:
```bash
cd infrastructure && sudo docker compose up -d
```

2. Access Flink SQL Client:
```bash
sudo docker exec -it flink-jobmanager ./bin/sql-client.sh
```

---

## Demo Steps

### Step 1: Configure Streaming Mode

Set Flink to streaming mode and enable changelog result display.

```sql
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'changelog';
```

---

### Step 2: Create Faker Data Generator (No External App Needed)

Use Flink Faker connector to generate synthetic transactions directly in SQL.

```sql
CREATE TABLE transactions_faker (
    transaction_id STRING,
    src_account_id STRING,
    dst_account_id STRING,
    amount DOUBLE,
    transaction_type STRING,
    event_time AS CURRENT_TIMESTAMP,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'faker',
    'rows-per-second' = '10',
    'fields.transaction_id.expression' = '#{Internet.uuid}',
    'fields.src_account_id.expression' = '#{regexify ''ACC-[0-9]{6}''}',
    'fields.dst_account_id.expression' = '#{regexify ''ACC-[0-9]{6}''}',
    'fields.amount.expression' = '#{Number.randomDouble ''2'',''1'',''50000''}',
    'fields.transaction_type.expression' = '#{Options.option ''TRANSFER'',''PAYMENT'',''WITHDRAWAL'',''DEPOSIT''}'
);
```

**Key concepts:**
- `faker` connector: Generates random data using [Java Faker](https://github.com/DiUS/java-faker) expressions
- `rows-per-second`: Controls throughput (10 transactions/sec)
- Expressions: `#{Internet.uuid}`, `#{regexify}`, `#{Number.randomDouble}`, `#{Options.option}`

Verify data generation:
```sql
SELECT * FROM transactions_faker;
```

---

### Step 3: Create Kafka Source Table (Alternative)

If you want to use real Kafka data instead of Faker, first start the data generator:
```bash
cd fraud-detection/app/java && bash deploy-data-generator.sh
```

Then create the Kafka source table:
```sql
CREATE TABLE IF NOT EXISTS transactions_kafka (
    transaction_id STRING,
    src_account_id STRING,
    dst_account_id STRING,
    amount DOUBLE,
    transaction_type STRING,
    timestamp_ms BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(timestamp_ms, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'broker:29092',
    'properties.group.id' = 'flink-sql-demo',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);
```

---

### Step 4: Create Unified View

Create a view to easily switch between Faker and Kafka source:

```sql
-- Use Faker (no external dependencies)
CREATE VIEW transactions AS SELECT * FROM transactions_faker;

-- OR use Kafka (requires data generator)
-- CREATE VIEW transactions AS SELECT * FROM transactions_kafka;
```

---

### Step 5: Create Fraud Alerts Sink

Create output table for detected fraud alerts.

```sql
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id STRING,
    transaction_id STRING,
    account_id STRING,
    amount DOUBLE,
    reason STRING,
    alert_time TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'fraud-alerts-sql',
    'properties.bootstrap.servers' = 'broker:29092',
    'format' = 'json'
);
```

---

### Step 6: Naive Fraud Detection

Detect high-value transactions (> $10,000) and write alerts to Kafka.

```sql
INSERT INTO fraud_alerts
SELECT 
    CONCAT('ALERT-', transaction_id) AS alert_id,
    transaction_id,
    src_account_id AS account_id,
    amount,
    'HIGH_AMOUNT: Transaction exceeds $10,000' AS reason,
    event_time AS alert_time
FROM transactions
WHERE amount > 10000;
```

This runs as a continuous job. Check Flink UI at http://localhost:8081.

---

### Step 7: Real-Time Aggregations (Tumbling Window)

Aggregate transactions per account in 1-minute windows.

```sql
SELECT 
    src_account_id AS account_id,
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    COUNT(*) AS tx_count,
    CAST(SUM(amount) AS DECIMAL(10,2)) AS total_amount,
    CAST(AVG(amount) AS DECIMAL(10,2)) AS avg_amount
FROM transactions
GROUP BY 
    src_account_id,
    TUMBLE(event_time, INTERVAL '1' MINUTE);
```

**Key concept:** `TUMBLE` creates non-overlapping 1-minute windows.

---

### Step 8: Top Accounts by Volume (Sliding Window)

Find top 10 accounts by transaction volume using sliding windows.

```sql
SELECT 
    src_account_id,
    COUNT(*) AS tx_count,
    CAST(SUM(amount) AS DECIMAL(12,2)) AS total_volume
FROM transactions
GROUP BY 
    src_account_id,
    HOP(event_time, INTERVAL '10' SECOND, INTERVAL '1' MINUTE)
ORDER BY total_volume DESC
LIMIT 10;
```

**Key concept:** `HOP` creates overlapping windows (slides every 10s, spans 1min).

---

## Verify Results

Check fraud alerts in Kafka:
```bash
sudo docker exec -it broker kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic fraud-alerts-sql \
    --from-beginning
```

---

## Clean Up

Exit SQL Client:
```sql
EXIT;
```

Stop running jobs from Flink UI or:
```bash
sudo docker exec flink-jobmanager flink list
sudo docker exec flink-jobmanager flink cancel <job-id>
```

