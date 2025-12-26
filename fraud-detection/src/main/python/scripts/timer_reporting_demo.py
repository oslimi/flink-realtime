#!/usr/bin/env python3
"""
=============================================================================
DEMO 4: TIMERS AND PERIODIC REPORTING
=============================================================================

Building on StatefulFraudDetectionDemo, this demo introduces TIMERS:
- Processing time timers for periodic actions
- ListState to collect alerts over time
- on_timer() callback for scheduled processing
- Chained processors (detection → aggregation)

Concepts introduced:
- ctx.timer_service().register_processing_time_timer()
- on_timer() callback method
- ListState<T> - keyed state storing a list
- Chaining multiple KeyedProcessFunctions

Features:
- Fraud detection (same as previous demo)
- Periodic reports every 60 seconds aggregating all alerts
"""
import logging

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer
)
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema

from model.transaction import Transaction
from processor.advanced_stateful_fraud_detection_processor import AdvancedStatefulFraudDetectionProcessor
from processor.fraud_report_aggregator_processor import FraudReportAggregatorProcessor
from config.flink_config import (
    KAFKA_BOOTSTRAP,
    TRANSACTIONS_TOPIC,
    FRAUD_ALERTS_TOPIC,
    FRAUD_REPORTS_TOPIC,
    create_stream_env,
    setup_logging
)

logger = logging.getLogger(__name__)


def main():
    """Main execution function."""
    setup_logging()
    logger.info("=== DEMO: Timers and Periodic Reporting ===")

    # 1. Create environment
    env = create_stream_env(enable_web_ui=True, parallelism=2)
    logger.info("Flink Web UI: http://localhost:8082")

    # 2. Kafka Source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topics(TRANSACTIONS_TOPIC) \
        .set_group_id("timer-reporting-demo") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Note: Kafka sinks disabled due to Python-Java serialization issues
    # For production, consider using kafka-python library or Table API

    # 3. Read and parse transactions
    transactions = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    ).map(lambda json_str: Transaction.from_json(json_str)).name("JSON to Transaction")

    # 4. First processor: Fraud detection (stateful)
    logger.info("Pipeline: Transactions → Fraud Detection → Report Aggregation")
    fraud_alerts = transactions \
        .key_by(lambda t: t.src_account_id) \
        .process(AdvancedStatefulFraudDetectionProcessor()) \
        .name("Stateful Fraud Detection")

    # 5. Second processor: Report aggregation with TIMERS
    #    - Receives alerts from previous processor
    #    - Collects alerts in ListState
    #    - Timer fires every 60 seconds to emit report
    fraud_reports = fraud_alerts \
        .key_by(lambda alert: alert.current_transaction.src_account_id) \
        .process(FraudReportAggregatorProcessor()) \
        .name("Timer-based Report Aggregator")

    # 6. Print for demo visibility
    fraud_alerts.map(lambda a: f"ALERT: {a.alert_id}").print()
    fraud_reports.map(lambda r: f"REPORT: {r.summary}").print()

    # Execute
    logger.info("Starting Timer Reporting Demo")
    logger.info(f"Alerts: {FRAUD_ALERTS_TOPIC}")
    logger.info(f"Reports: {FRAUD_REPORTS_TOPIC} (every 60 seconds)")
    logger.info("Test pattern:")
    logger.info(
        '1) {"transactionId":"tx-001","srcAccountId":"acc-123","destAccountId":"acc-456","amount":50.0,"currency":"EUR","eventTime":1702900000000}')
    logger.info(
        '2) {"transactionId":"tx-002","srcAccountId":"acc-123","destAccountId":"acc-789","amount":75000.0,"currency":"EUR","eventTime":1702900001000}')
    logger.info("Wait 60 seconds to see the report...")
    env.execute("Timer Reporting Demo")


if __name__ == "__main__":
    main()
