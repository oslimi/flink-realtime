-- Flink SQL Demo: Print Table for Live Monitoring
-- Use this to see transactions in real-time in the SQL client

CREATE TABLE transactions_print (
    transaction_id STRING,
    src_account_id STRING,
    dst_account_id STRING,
    amount DOUBLE,
    transaction_type STRING,
    event_time TIMESTAMP(3)
) WITH (
    'connector' = 'print'
);

-- Insert streaming data to print connector
INSERT INTO transactions_print
SELECT
    transaction_id,
    src_account_id,
    dst_account_id,
    amount,
    transaction_type,
    TO_TIMESTAMP_LTZ(timestamp_ms, 3) AS event_time
FROM transactions;

