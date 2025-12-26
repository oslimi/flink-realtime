-- Flink Faker Data Generator for Transactions
-- No external Java application needed

SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'changelog';

-- Create Faker-based transaction generator
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

-- Verify data generation
-- SELECT * FROM transactions_faker;

-- Create view for unified access
CREATE VIEW transactions AS SELECT * FROM transactions_faker;

-- Create fraud alerts sink
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

-- Naive fraud detection: high-value transactions
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

