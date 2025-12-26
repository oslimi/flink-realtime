-- ============================================================
-- FLINK SQL DEMO: Real-Time Fraud Detection
-- ============================================================
-- This demo shows how to use Flink SQL for streaming analytics
-- Run this in the Flink SQL Client
-- ============================================================

-- Step 1: Set execution mode to streaming
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'changelog';

-- ============================================================
-- STEP 2: CREATE SOURCE TABLE (Kafka)
-- ============================================================
CREATE TABLE IF NOT EXISTS transactions (
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

-- ============================================================
-- STEP 3: VERIFY DATA IS FLOWING (run this to see live data)
-- ============================================================
-- SELECT * FROM transactions;

-- ============================================================
-- STEP 4: CREATE FRAUD ALERTS SINK TABLE
-- ============================================================
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

-- ============================================================
-- STEP 5: NAIVE FRAUD DETECTION (amount > 10000)
-- ============================================================
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

-- ============================================================
-- STEP 6: REAL-TIME AGGREGATIONS (1-minute windows)
-- ============================================================
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

-- ============================================================
-- STEP 7: TOP ACCOUNTS BY VOLUME (sliding window)
-- ============================================================
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

