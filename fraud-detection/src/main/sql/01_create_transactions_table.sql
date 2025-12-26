-- Flink SQL Demo: Create Transactions Source Table
-- This table reads from Kafka topic 'transactions'

CREATE TABLE transactions (
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

