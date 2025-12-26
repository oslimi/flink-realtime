-- Flink SQL Demo: Naive Fraud Detection
-- Detect transactions with amount > 10000

INSERT INTO fraud_alerts
SELECT
    CONCAT('ALERT-', transaction_id) AS alert_id,
    transaction_id,
    src_account_id AS account_id,
    amount,
    'HIGH_AMOUNT: Transaction exceeds $10,000 threshold' AS reason,
    event_time AS alert_time
FROM transactions
WHERE amount > 10000;

