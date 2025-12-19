#!/usr/bin/env python3
"""
=============================================================================
DEMO 5: JDBC SINK - DATABASE INTEGRATION
=============================================================================

Building on TimerReportingDemo, this demo adds DATABASE integration:
- JDBC Sink to write to PostgreSQL
- Table API for JDBC operations
- Multiple sinks (Kafka + PostgreSQL)

Concepts introduced:
- PyFlink Table API
- JDBC connector
- Conversion between DataStream and Table
- Multiple sinks from same stream

Data Flow:
Transactions → Fraud Detection → Alerts → Report Aggregator → Reports
                                   ↓                            ↓
                             Kafka Sink                   Kafka Sink
                                                               ↓
                                                         PostgreSQL

Note: PyFlink JDBC requires the JDBC connector JAR to be available.
For simplicity, this demo writes to Kafka and shows how to integrate JDBC.
"""
import sys
import os
import logging

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
    DeliveryGuarantee
)
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.table.expressions import col

from model.transaction import Transaction
from processor.advanced_stateful_fraud_detection_processor import AdvancedStatefulFraudDetectionProcessor
from processor.fraud_report_aggregator_processor import FraudReportAggregatorProcessor
from config.flink_config import (
    KAFKA_BOOTSTRAP,
    TRANSACTIONS_TOPIC,
    FRAUD_ALERTS_TOPIC,
    FRAUD_REPORTS_TOPIC,
    POSTGRES_URL,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    create_stream_env,
    setup_logging
)

logger = logging.getLogger(__name__)


def main():
    """Main execution function."""
    setup_logging()
    logger.info("=== DEMO: JDBC Sink - PostgreSQL Integration ===")

    # 1. Create environment
    env = create_stream_env(enable_web_ui=True, parallelism=2)
    logger.info("Flink Web UI: http://localhost:8082")

    # Create Table Environment for JDBC
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 2. Kafka Source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topics(TRANSACTIONS_TOPIC) \
        .set_group_id("jdbc-sink-demo") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Note: Kafka sinks disabled due to Python-Java serialization issues
    # This demo focuses on JDBC sink functionality

    # 4. PostgreSQL JDBC Sink (using Table API)
    logger.info(f"Configuring PostgreSQL sink: {POSTGRES_URL}")

    # Create JDBC table
    table_env.execute_sql(f"""
        CREATE TABLE fraud_reports_sink (
            report_id STRING,
            report_timestamp BIGINT,
            window_start BIGINT,
            window_end BIGINT,
            account_id STRING,
            total_alerts INT,
            total_fraud_amount DOUBLE,
            summary STRING,
            PRIMARY KEY (report_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{POSTGRES_URL}',
            'table-name' = 'fraud_reports',
            'username' = '{POSTGRES_USER}',
            'password' = '{POSTGRES_PASSWORD}',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # 5. Read and parse transactions
    transactions = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    ).map(lambda json_str: Transaction.from_json(json_str)).name("JSON to Transaction")

    # 6. Fraud detection pipeline
    fraud_alerts = transactions \
        .key_by(lambda t: t.src_account_id) \
        .process(AdvancedStatefulFraudDetectionProcessor()) \
        .name("Stateful Fraud Detection")

    # 8. Report aggregation with timers
    fraud_reports = fraud_alerts \
        .key_by(lambda alert: alert.current_transaction.src_account_id) \
        .process(FraudReportAggregatorProcessor()) \
        .name("Report Aggregator")

    # 9. Convert fraud reports to Table and sink to PostgreSQL
    # Map to tuple format for Table API
    report_tuples = fraud_reports.map(
        lambda r: (
            r.report_id,
            r.report_timestamp,
            r.window_start,
            r.window_end,
            r.account_id,
            r.total_alerts,
            r.total_fraud_amount,
            r.summary
        )
    )

    # Convert to Table
    report_table = table_env.from_data_stream(
        report_tuples,
        col('report_id'),
        col('report_timestamp'),
        col('window_start'),
        col('window_end'),
        col('account_id'),
        col('total_alerts'),
        col('total_fraud_amount'),
        col('summary')
    )

    # Insert into JDBC table
    report_table.execute_insert('fraud_reports_sink')

    # 10. Print for demo visibility
    fraud_alerts.map(lambda a: f"ALERT: {a.alert_id}").print()
    fraud_reports.map(lambda r: f"REPORT: {r.summary}").print()

    # Execute
    logger.info("Starting JDBC Sink Demo")
    logger.info(f"Kafka topics: {FRAUD_ALERTS_TOPIC}, {FRAUD_REPORTS_TOPIC}")
    logger.info(f"PostgreSQL: {POSTGRES_URL} (table: fraud_reports)")
    logger.info("Test pattern:")
    logger.info('1) {"transactionId":"tx-001","srcAccountId":"acc-123","destAccountId":"acc-456","amount":50.0,"currency":"EUR","eventTime":1702900000000}')
    logger.info('2) {"transactionId":"tx-002","srcAccountId":"acc-123","destAccountId":"acc-789","amount":75000.0,"currency":"EUR","eventTime":1702900001000}')
    logger.info("Query PostgreSQL: SELECT * FROM fraud_reports;")
    env.execute("JDBC Sink Demo")


if __name__ == "__main__":
    main()

