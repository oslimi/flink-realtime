#!/usr/bin/env python3
"""
=============================================================================
DEMO 2: NAIVE FRAUD DETECTION (STATELESS)
=============================================================================
Building on KafkaSourceDemo, this demo adds:
    # 5. Print alerts for demo visibility
- Custom serialization schema
Fraud Rule: Flag any transaction with amount > 10,000
This is a NAIVE approach - no state, no pattern detection.
"""
import sys
import os
import logging
# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer
)
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from model.transaction import Transaction
from processor.naive_fraud_detection_processor import NaiveFraudDetectionProcessor
from config.flink_config import (
    KAFKA_BOOTSTRAP,
    TRANSACTIONS_TOPIC,
    create_stream_env,
    setup_logging
)
logger = logging.getLogger(__name__)
def main():
    """Main execution function."""
    setup_logging()
    logger.info("=== DEMO: Naive Fraud Detection (Stateless) ===")

    # 1. Create environment (disable Web UI to avoid port conflicts)
    env = create_stream_env(enable_web_ui=False, parallelism=2)
    logger.info("Flink environment created (Web UI disabled for simplicity)")

    # 2. Kafka Source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topics(TRANSACTIONS_TOPIC) \
        .set_group_id("naive-fraud-detection-demo") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    # Note: Kafka sink is disabled due to Python-Java serialization issues
    # For production, consider using kafka-python library directly or Table API
    
    # 3. Read and parse transactions
    transactions = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    ).map(lambda json_str: Transaction.from_json(json_str)).name("JSON to Transaction")

    # 4. Apply naive fraud detection
    #    - key_by() partitions data by src_account_id (required for KeyedProcessFunction)
    #    - process() applies our stateless fraud detection logic
    logger.info("Fraud rule: amount > 10,000 triggers alert")
    fraud_alerts = transactions \
        .key_by(lambda t: t.src_account_id) \
        .process(NaiveFraudDetectionProcessor()) \
        .name("Naive Fraud Detection")

    # 5. Print alerts for demo visibility
    fraud_alerts.print()

    # Execute
    logger.info("Starting Naive Fraud Detection Demo")
    logger.info("Test with amount > 10000:")
    logger.info('{"transactionId":"tx-001","srcAccountId":"acc-123","destAccountId":"acc-456","amount":15000.0,"currency":"EUR","eventTime":1702900000000}')
    env.execute("Naive Fraud Detection Demo")
if __name__ == "__main__":
    main()
