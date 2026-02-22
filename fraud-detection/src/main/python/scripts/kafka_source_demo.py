#!/usr/bin/env python3
"""
=============================================================================
DEMO 1: KAFKA SOURCE - READING FROM KAFKA
=============================================================================

This is the first demo introducing PyFlink basics:
- Creating a StreamExecutionEnvironment
- Configuring Kafka Source
- Simple map transformation (JSON parsing)
- Print sink for output

Concepts introduced:
- StreamExecutionEnvironment
- Kafka connector (source)
- Basic transformations
- Watermark strategy (none for now)

This demo simply reads from Kafka and prints transactions.
No fraud detection yet - just infrastructure setup.
"""
import logging

import numpy as np
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

from config.flink_config import (
    KAFKA_BOOTSTRAP,
    TRANSACTIONS_TOPIC,
    create_stream_env,
    setup_logging
)
from model.transaction import Transaction

logger = logging.getLogger(__name__)


def parse_transaction(json_str: str) -> Transaction:
    """Parse JSON string to Transaction object."""
    return Transaction.from_json(json_str)


def main():
    """Main execution function."""
    setup_logging()
    logger.info("=== DEMO: Kafka Source - Reading Transactions ===")

    # 1. Create Flink environment with Web UI
    env = create_stream_env(enable_web_ui=True, parallelism=2)
    logger.info(f"Flink Web UI: http://localhost:8082")

    # 2. Configure Kafka Source
    logger.info(f"Configuring Kafka source - topic: {TRANSACTIONS_TOPIC}")
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topics(TRANSACTIONS_TOPIC) \
        .set_group_id("kafka-source-demo") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 3. Read from Kafka
    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    # 4. Parse JSON to Transaction
    transactions = stream.map(parse_transaction).name("JSON to Transaction")

    multipliedTransaction = (transactions
                             .map(lambda tx: Transaction.multiplyByfactor(tx, np.pi))
                             .filter(lambda tx: tx.amount > 900.0))

    # 5. Print transactions
    transactions.print("TRANSACTIONS_TOPIC")
    multipliedTransaction.print("MULTIPLIED_TRANSACTIONS")

    # 6. Execute
    logger.info("Starting Kafka Source Demo")
    logger.info(f"Reading from topic: {TRANSACTIONS_TOPIC}")
    logger.info("Test by sending a message:")
    logger.info(
        '{"transactionId":"tx-001","srcAccountId":"acc-123","destAccountId":"acc-456","amount":1000.0,"currency":"EUR","eventTime":1702900000000}')

    env.execute("Kafka Source Demo")


if __name__ == "__main__":
    main()
