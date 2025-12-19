#!/usr/bin/env python3
"""
=============================================================================
DEMO 3: STATEFUL FRAUD DETECTION
=============================================================================

Building on NaiveFraudDetectionDemo, this demo introduces STATE:
- ValueState to remember previous transaction
- Pattern-based fraud detection
- Stateful KeyedProcessFunction

Concepts introduced:
- ValueState<T> - keyed state storing a single value
- State initialization in open()
- State read/write in process_element()
- State scoped per key (account_id)

Fraud Rule: Small transaction (< 100) followed by large transaction (> 50,000)

This pattern detects when attackers test with small amounts before draining.
"""
import sys
import os
import logging

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
    DeliveryGuarantee
)
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema

from model.transaction import Transaction
from processor.advanced_stateful_fraud_detection_processor import AdvancedStatefulFraudDetectionProcessor
from config.flink_config import (
    KAFKA_BOOTSTRAP,
    TRANSACTIONS_TOPIC,
    FRAUD_ALERTS_STATEFUL_TOPIC,
    create_stream_env,
    setup_logging
)

logger = logging.getLogger(__name__)


def main():
    """Main execution function."""
    setup_logging()
    logger.info("=== DEMO: Stateful Fraud Detection (with ValueState) ===")

    # 1. Create environment
    env = create_stream_env(enable_web_ui=True, parallelism=2)
    logger.info("Flink Web UI: http://localhost:8082")

    # 2. Kafka Source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topics(TRANSACTIONS_TOPIC) \
        .set_group_id("stateful-fraud-detection-demo") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Note: Kafka sink disabled due to Python-Java serialization issues
    # For production, consider using kafka-python library or Table API

    # 4. Read and parse transactions
    transactions = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    ).map(lambda json_str: Transaction.from_json(json_str)).name("JSON to Transaction")

    # 5. Apply STATEFUL fraud detection
    #    - Uses ValueState to remember previous transaction per account
    #    - Detects pattern: small tx followed by large tx
    logger.info("Fraud rule: small amount (< 100) followed by large amount (> 50,000)")
    fraud_alerts = transactions \
        .key_by(lambda t: t.src_account_id) \
        .process(AdvancedStatefulFraudDetectionProcessor()) \
        .name("Stateful Fraud Detection")

    # 6. Print for demo visibility
    fraud_alerts.print()

    # Execute
    logger.info("Starting Stateful Fraud Detection Demo")
    logger.info("Test pattern - send these two messages for same account:")
    logger.info('1) Small: {"transactionId":"tx-001","srcAccountId":"acc-123","destAccountId":"acc-456","amount":50.0,"currency":"EUR","eventTime":1702900000000}')
    logger.info('2) Large: {"transactionId":"tx-002","srcAccountId":"acc-123","destAccountId":"acc-789","amount":75000.0,"currency":"EUR","eventTime":1702900001000}')
    env.execute("Stateful Fraud Detection Demo")


if __name__ == "__main__":
    main()

