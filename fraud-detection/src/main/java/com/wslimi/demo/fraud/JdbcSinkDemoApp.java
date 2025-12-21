package com.wslimi.demo.fraud;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wslimi.demo.fraud.model.FraudAdvancedAlert;
import com.wslimi.demo.fraud.model.FraudReport;
import com.wslimi.demo.fraud.model.Transaction;
import com.wslimi.demo.fraud.processor.AdvancedStatefulFraudDetectionProcessor;
import com.wslimi.demo.fraud.processor.FraudReportAggregatorProcessor;
import com.wslimi.demo.fraud.serde.FraudAdvancedAlertSerializationSchema;
import com.wslimi.demo.fraud.serde.FraudReportSerializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

/**
 * =============================================================================
 * DEMO 5: JDBC SINK - DATABASE INTEGRATION
 * =============================================================================
 *
 * Building on TimerReportingDemoApp, this demo adds DATABASE integration:
 * - JDBC Sink to write to PostgreSQL
 * - Row type for structured data
 * - Multiple sinks (Kafka + PostgreSQL)
 *
 * Concepts introduced:
 * - JdbcSink configuration
 * - Row type for JDBC operations
 * - Batch execution options
 * - Connection pooling options
 * - Multiple sinks from same stream
 *
 * Data Flow:
 * Transactions → Fraud Detection → Alerts → Report Aggregator → Reports
 *                                    ↓                            ↓
 *                              Kafka Sink                   Kafka Sink
 *                                                                ↓
 *                                                          PostgreSQL
 */
@Slf4j
public class JdbcSinkDemoApp {

    public static final String KAFKA_BOOTSTRAP = "localhost:9092";
    public static final String TRANSACTIONS_TOPIC = "transactions";
    public static final String FRAUD_ALERTS_TOPIC = "fraud-alerts";
    public static final String FRAUD_REPORTS_TOPIC = "fraud-reports";

    // PostgreSQL configuration
    public static final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/streaming_demo";
    public static final String POSTGRES_USER = "app_user";
    public static final String POSTGRES_PASSWORD = "app_password";

    public static void main(String[] args) throws Exception {
        log.info("=== DEMO: JDBC Sink - PostgreSQL Integration ===");

        // 1. Create environment
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setParallelism(2);

        log.info("Flink Web UI: http://localhost:8082");

        ObjectMapper objectMapper = new ObjectMapper();

        // 2. Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(TRANSACTIONS_TOPIC)
                .setGroupId("jdbc-sink-demo")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 3. Kafka Sinks
        KafkaSink<FraudAdvancedAlert> alertsSink = KafkaSink.<FraudAdvancedAlert>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FraudAdvancedAlert>builder()
                        .setTopic(FRAUD_ALERTS_TOPIC)
                        .setValueSerializationSchema(new FraudAdvancedAlertSerializationSchema(objectMapper))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<FraudReport> reportsSink = KafkaSink.<FraudReport>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FraudReport>builder()
                        .setTopic(FRAUD_REPORTS_TOPIC)
                        .setValueSerializationSchema(new FraudReportSerializationSchema(objectMapper))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 4. PostgreSQL JDBC Sink using Row type
        log.info("Configuring PostgreSQL sink: {}", POSTGRES_URL);
        SinkFunction<Row> postgresSink = JdbcSink.sink(
                // SQL INSERT statement with UPSERT (ON CONFLICT)
                "INSERT INTO fraud_reports (report_id, report_timestamp, window_start, window_end, account_id, total_alerts, total_fraud_amount, summary) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (report_id) DO UPDATE SET " +
                        "total_alerts = EXCLUDED.total_alerts, " +
                        "total_fraud_amount = EXCLUDED.total_fraud_amount, " +
                        "summary = EXCLUDED.summary",
                // Statement setter - maps Row fields to SQL parameters
                (statement, row) -> {
                    statement.setString(1, (String) row.getField(0));   // report_id
                    statement.setLong(2, (Long) row.getField(1));       // report_timestamp
                    statement.setLong(3, (Long) row.getField(2));       // window_start
                    statement.setLong(4, (Long) row.getField(3));       // window_end
                    statement.setString(5, (String) row.getField(4));   // account_id
                    statement.setInt(6, (Integer) row.getField(5));     // total_alerts
                    statement.setDouble(7, (Double) row.getField(6));   // total_fraud_amount
                    statement.setString(8, (String) row.getField(7));   // summary
                },
                // Execution options - batching configuration
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)           // Batch up to 1000 records
                        .withBatchIntervalMs(200)      // Or flush every 200ms
                        .withMaxRetries(5)             // Retry up to 5 times on failure
                        .build(),
                // Connection options
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(POSTGRES_URL)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(POSTGRES_USER)
                        .withPassword(POSTGRES_PASSWORD)
                        .build()
        );

        // 5. Read and parse transactions
        DataStream<Transaction> transactions = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(json -> objectMapper.readValue(json, Transaction.class))
                .name("JSON to Transaction");

        // 6. Fraud detection pipeline
        DataStream<FraudAdvancedAlert> fraudAlerts = transactions
                .keyBy(Transaction::srcAccountId)
                .process(new AdvancedStatefulFraudDetectionProcessor())
                .name("Stateful Fraud Detection");

        // 7. Report aggregation with timers
        DataStream<FraudReport> fraudReports = fraudAlerts
                .keyBy(alert -> alert.currentTransaction().srcAccountId())
                .process(new FraudReportAggregatorProcessor())
                .name("Report Aggregator");

        // 8. Convert FraudReport to Row for JDBC sink
        DataStream<Row> fraudReportRows = fraudReports
                .map(report -> Row.of(
                        report.reportId(),
                        report.reportTimestamp(),
                        report.windowStart(),
                        report.windowEnd(),
                        report.accountId(),
                        report.totalAlerts(),
                        report.totalFraudAmount(),
                        report.summary()
                ))
                .name("FraudReport to Row");

        // 9. Sink to Kafka
        fraudAlerts.sinkTo(alertsSink).name("Alerts Kafka Sink");
        fraudReports.sinkTo(reportsSink).name("Reports Kafka Sink");

        // 10. Sink to PostgreSQL
        fraudReportRows.addSink(postgresSink).name("PostgreSQL Sink");

        // 11. Print for demo visibility
        fraudAlerts.print("ALERT");
        fraudReports.print("REPORT");

        // Execute
        log.info("Starting JDBC Sink Demo");
        log.info("Kafka topics: {}, {}", FRAUD_ALERTS_TOPIC, FRAUD_REPORTS_TOPIC);
        log.info("PostgreSQL: {} (table: fraud_reports)", POSTGRES_URL);
        log.info("Test pattern:");
        log.info("1) {\"transactionId\":\"tx-001\",\"srcAccountId\":\"acc-123\",\"destAccountId\":\"acc-456\",\"amount\":50.0,\"currency\":\"EUR\",\"eventTime\":1702900000000}");
        log.info("2) {\"transactionId\":\"tx-002\",\"srcAccountId\":\"acc-123\",\"destAccountId\":\"acc-789\",\"amount\":75000.0,\"currency\":\"EUR\",\"eventTime\":1702900001000}");
        log.info("Query PostgreSQL: SELECT * FROM fraud_reports;");
        env.execute("JDBC Sink Demo");
    }
}

