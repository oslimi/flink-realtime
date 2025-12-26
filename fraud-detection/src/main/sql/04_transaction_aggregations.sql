-- Flink SQL Demo: Real-time Aggregations
-- Create a view for transaction statistics per account (tumbling window)

CREATE VIEW transaction_stats AS
SELECT
    src_account_id AS account_id,
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
    COUNT(*) AS tx_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    MAX(amount) AS max_amount,
    MIN(amount) AS min_amount
FROM transactions
GROUP BY
    src_account_id,
    TUMBLE(event_time, INTERVAL '1' MINUTE);

