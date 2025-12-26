-- Flink SQL Demo: Create Fraud Alerts Sink Table
-- This table writes detected fraud alerts to Kafka topic 'fraud-alerts'

CREATE TABLE fraud_alerts (
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

