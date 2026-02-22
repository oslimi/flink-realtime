#!/usr/bin/env python3
"""
=============================================================================
DEMO 5: JDBC SINK - DATABASE INTEGRATION (PyFlink Version)
=============================================================================

Building on TimerReportingDemo, this demo adds DATABASE integration:
- Table API JDBC Sink to write to PostgreSQL
- Kafka bridge pattern to avoid PyFlink serialization issues
- Multiple sinks (Print + PostgreSQL)

Concepts introduced:
- PyFlink Table API for JDBC
- Kafka as intermediate storage (workaround for serialization)
- SQL DDL for JDBC connector
- DataStream → Kafka → Table API → JDBC pattern

Data Flow:
Transactions → Fraud Detection → Alerts → Report Aggregator → Reports
                                   ↓                            ↓
                                Print                        Print
                                                               ↓
                                                            Kafka (JSON)
                                                               ↓
                                                         Table API SQL
                                                               ↓
                                                         PostgreSQL

Note: PyFlink cannot serialize Python objects directly to Table API.
We use Kafka as an intermediate bridge to work around this limitation.
"""
import sys
import os
import logging

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee
)
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types

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
    logger.info("=== DEMO: JDBC Sink - PostgreSQL Integration (PyFlink) ===")

    # 1. Create environment
    env = create_stream_env(enable_web_ui=True, parallelism=2)
    logger.info("Flink Web UI: http://localhost:8082")

    # 2. Create Table Environment - required for JDBC in PyFlink
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Add JDBC and Kafka JAR dependencies
    jdbc_jar = os.path.join(os.path.dirname(__file__), '..', 'lib', 'flink-connector-jdbc-3.3.0-1.20.jar')
    postgres_jar = os.path.join(os.path.dirname(__file__), '..', 'lib', 'postgresql-42.7.3.jar')
    kafka_jar = os.path.join(os.path.dirname(__file__), '..', 'lib', 'flink-sql-connector-kafka-3.3.0-1.20.jar')

    # Set pipeline jars - this configuration is used by both DataStream and Table API
    jar_urls = f"file://{os.path.abspath(jdbc_jar)};file://{os.path.abspath(postgres_jar)};file://{os.path.abspath(kafka_jar)}"
    table_env.get_config().get_configuration().set_string("pipeline.jars", jar_urls)

    logger.info(f"Added JARs: JDBC, PostgreSQL, Kafka connectors")

    # 3. Kafka Source
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topics(TRANSACTIONS_TOPIC) \
        .set_group_id("jdbc-sink-demo") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 4. Create JDBC Sink Table using SQL DDL (PyFlink way)
    logger.info(f"Configuring PostgreSQL sink: {POSTGRES_URL}")

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
            'driver' = 'org.postgresql.Driver',
            'sink.buffer-flush.max-rows' = '1000',
            'sink.buffer-flush.interval' = '200ms',
            'sink.max-retries' = '5'
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

    # 7. Report aggregation with timers
    fraud_reports = fraud_alerts \
        .key_by(lambda alert: alert.current_transaction.src_account_id) \
        .process(FraudReportAggregatorProcessor()) \
        .name("Report Aggregator")

    # 8. Workaround: Write to temp Kafka topic first, then use SQL to read and write to JDBC
    # This is necessary because PyFlink cannot serialize Python objects to Table Row properly

    # Create temporary Kafka table for fraud reports
    table_env.execute_sql(f"""
        CREATE TABLE fraud_reports_kafka (
            report_id STRING,
            report_timestamp BIGINT,
            window_start BIGINT,
            window_end BIGINT,
            account_id STRING,
            total_alerts INT,
            total_fraud_amount DOUBLE,
            summary STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'fraud-reports',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'jdbc-sink-demo',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # Convert FraudReport to JSON and write to Kafka first
    reports_kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("fraud-reports")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    # Explicitly specify output type as STRING to avoid byte array serialization
    fraud_reports_json = fraud_reports.map(
        lambda r: r.to_json(),
        output_type=Types.STRING()
    ).name("FraudReport to JSON")

    fraud_reports_json.sink_to(reports_kafka_sink).name("Reports to Kafka")

    # 9. Use Table API to copy from Kafka to JDBC (this works in PyFlink)
    # Execute async SQL job to continuously read from Kafka and write to PostgreSQL
    table_env.execute_sql("""
        INSERT INTO fraud_reports_sink
        SELECT 
            report_id,
            report_timestamp,
            window_start,
            window_end,
            account_id,
            total_alerts,
            total_fraud_amount,
            summary
        FROM fraud_reports_kafka
    """)

    # 10. Print for demo visibility
    fraud_alerts.map(lambda a: f"ALERT: {a.alert_id}").print()
    fraud_reports.map(lambda r: f"REPORT: {r.summary}").print()

    # Execute
    logger.info("Starting JDBC Sink Demo")
    logger.info(f"PostgreSQL: {POSTGRES_URL} (table: fraud_reports)")
    logger.info("Architecture: DataStream → Kafka → Table API → PostgreSQL")
    logger.info("")
    logger.info("⚠️  Note: PyFlink cannot serialize Python objects directly to JDBC")
    logger.info("This demo uses Kafka as an intermediate bridge to work around the limitation")
    logger.info("")
    logger.info("Test pattern:")
    logger.info(
        '1) {"transactionId":"tx-001","srcAccountId":"acc-123","destAccountId":"acc-456","amount":50.0,"currency":"EUR","eventTime":1702900000000}')
    logger.info(
        '2) {"transactionId":"tx-002","srcAccountId":"acc-123","destAccountId":"acc-789","amount":75000.0,"currency":"EUR","eventTime":1702900001000}')
    logger.info("Query PostgreSQL: SELECT * FROM fraud_reports;")

    # Execute the DataStream job
    env.execute("JDBC Sink Demo (PyFlink)")


if __name__ == "__main__":
    main()

