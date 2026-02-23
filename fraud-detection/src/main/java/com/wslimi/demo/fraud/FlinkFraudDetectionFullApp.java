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
 * FULL APPLICATION: COMPLETE FRAUD DETECTION PIPELINE
 * =============================================================================
 *
 * This is the COMPLETE application combining all previous demos:
 *
 * 1. Kafka Source - Read transactions
 * 2. JSON Deserialization - Parse to POJO
 * 3. Stateful Fraud Detection - Pattern detection with ValueState
 * 4. Timer-based Reporting - Periodic aggregation with ListState
 * 5. Multiple Sinks - Kafka + PostgreSQL
 *
 * Architecture:
 *
 *   Kafka (transactions)
 *          │
 *          ▼
 *   ┌─────────────────────┐
 *   │  Parse JSON → POJO  │
 *   └─────────────────────┘
 *          │
 *          ▼
 *   ┌─────────────────────┐
 *   │  keyBy(accountId)   │
 *   └─────────────────────┘
 *          │
 *          ▼
 *   ┌─────────────────────┐
 *   │  Fraud Detection    │ ← ValueState (previous transaction)
 *   │  (Stateful)         │
 *   └─────────────────────┘
 *          │
 *          ├──────────────────────────▶ Kafka (fraud-alerts)
 *          │
 *          ▼
 *   ┌─────────────────────┐
 *   │  keyBy(accountId)   │
 *   └─────────────────────┘
 *          │
 *          ▼
 *   ┌─────────────────────┐
 *   │  Report Aggregator  │ ← ListState (alerts) + Timer (60s)
 *   │  (Timer-based)      │
 *   └─────────────────────┘
 *          │
 *          ├──────────────────────────▶ Kafka (fraud-reports)
 *          │
 *          ▼
 *   ┌─────────────────────┐
 *   │  Convert to Row     │
 *   └─────────────────────┘
 *          │
 *          ▼
 *     PostgreSQL (fraud_reports table)
 */
@Slf4j
public class FlinkFraudDetectionFullApp {

    public static final String KAFKA_BOOTSTRAP = "localhost:9092";
    public static final String TRANSACTIONS_TOPIC = "transactions";
    public static final String FRAUD_ALERTS_TOPIC = "fraud-alerts";
    public static final String FRAUD_REPORTS_TOPIC = "fraud-reports";

    public static final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/streaming_demo";
    public static final String POSTGRES_USER = "app_user";
    public static final String POSTGRES_PASSWORD = "app_password";

    public static void main(String[] args) throws Exception {
        log.info("╔══════════════════════════════════════════════════════════════╗");
        log.info("║     FLINK FRAUD DETECTION - FULL APPLICATION                 ║");
        log.info("╚══════════════════════════════════════════════════════════════╝");

        // =====================================================================
        // ENVIRONMENT SETUP
        // =====================================================================
        // 1. Create environment with Web UI on port 8085
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8085);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(2);

        log.info("┌─────────────────────────────────────────────────────────────┐");
        log.info("│ Flink Web UI: http://localhost:8081                         │");
        log.info("└─────────────────────────────────────────────────────────────┘");

        ObjectMapper objectMapper = new ObjectMapper();

        // =====================================================================
        // SOURCES
        // =====================================================================
        log.info("Configuring Kafka source: {}", TRANSACTIONS_TOPIC);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(TRANSACTIONS_TOPIC)
                .setGroupId("fraud-detection-full")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // =====================================================================
        // SINKS
        // =====================================================================

        // Kafka sink for alerts
        KafkaSink<FraudAdvancedAlert> alertsSink = KafkaSink.<FraudAdvancedAlert>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FraudAdvancedAlert>builder()
                        .setTopic(FRAUD_ALERTS_TOPIC)
                        .setValueSerializationSchema(new FraudAdvancedAlertSerializationSchema(objectMapper))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Kafka sink for reports
        KafkaSink<FraudReport> reportsSink = KafkaSink.<FraudReport>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FraudReport>builder()
                        .setTopic(FRAUD_REPORTS_TOPIC)
                        .setValueSerializationSchema(new FraudReportSerializationSchema(objectMapper))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // PostgreSQL sink for reports
        log.info("Configuring PostgreSQL sink: {}", POSTGRES_URL);
        SinkFunction<Row> postgresSink = JdbcSink.sink(
                "INSERT INTO fraud_reports (report_id, report_timestamp, window_start, window_end, account_id, total_alerts, total_fraud_amount, summary) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (report_id) DO UPDATE SET " +
                        "total_alerts = EXCLUDED.total_alerts, " +
                        "total_fraud_amount = EXCLUDED.total_fraud_amount, " +
                        "summary = EXCLUDED.summary",
                (statement, row) -> {
                    statement.setString(1, (String) row.getField(0));
                    statement.setLong(2, (Long) row.getField(1));
                    statement.setLong(3, (Long) row.getField(2));
                    statement.setLong(4, (Long) row.getField(3));
                    statement.setString(5, (String) row.getField(4));
                    statement.setInt(6, (Integer) row.getField(5));
                    statement.setDouble(7, (Double) row.getField(6));
                    statement.setString(8, (String) row.getField(7));
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(POSTGRES_URL)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(POSTGRES_USER)
                        .withPassword(POSTGRES_PASSWORD)
                        .build()
        );

        // =====================================================================
        // PIPELINE
        // =====================================================================

        // Step 1: Read from Kafka and deserialize
        DataStream<Transaction> transactions = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(json -> objectMapper.readValue(json, Transaction.class))
                .name("[JAVA] JSON to Transaction");

        // Step 2: Stateful fraud detection
        DataStream<FraudAdvancedAlert> fraudAlerts = transactions
                .keyBy(Transaction::srcAccountId)
                .process(new AdvancedStatefulFraudDetectionProcessor())
                .name("[JAVA] Stateful Fraud Detection");

        // Step 3: Timer-based report aggregation
        DataStream<FraudReport> fraudReports = fraudAlerts
                .keyBy(alert -> alert.currentTransaction().srcAccountId())
                .process(new FraudReportAggregatorProcessor())
                .name("[JAVA] Report Aggregator (60s timer)");

        // Step 4: Convert to Row for PostgreSQL
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
                .name("[JAVA] FraudReport to Row");

        // =====================================================================
        // OUTPUT
        // =====================================================================

        // Kafka outputs
        fraudAlerts.sinkTo(alertsSink).name("Kafka Alerts Sink");
        fraudReports.sinkTo(reportsSink).name("Kafka Reports Sink");

        // PostgreSQL output
        fraudReportRows.addSink(postgresSink).name("PostgreSQL Sink");

        // Console output for demo
        fraudAlerts.print("🚨 ALERT");
        fraudReports.print("📊 REPORT");

        // =====================================================================
        // EXECUTE
        // =====================================================================
        log.info("┌─────────────────────────────────────────────────────────────┐");
        log.info("│ PIPELINE STARTED                                            │");
        log.info("├─────────────────────────────────────────────────────────────┤");
        log.info("│ Input:  Kafka topic '{}'\t\t\t\t│", TRANSACTIONS_TOPIC);
        log.info("│ Output: Kafka topic '{}'\t\t\t\t│", FRAUD_ALERTS_TOPIC);
        log.info("│ Output: Kafka topic '{}'\t\t\t│", FRAUD_REPORTS_TOPIC);
        log.info("│ Output: PostgreSQL table 'fraud_reports'\t\t\t│");
        log.info("├─────────────────────────────────────────────────────────────┤");
        log.info("│ Fraud Rule: small tx (<100) → large tx (>50,000)\t\t│");
        log.info("│ Reports: Every 60 seconds\t\t\t\t\t│");
        log.info("└─────────────────────────────────────────────────────────────┘");
        log.info("");
        log.info("Test with these messages (same account):");
        log.info("1) {\"transactionId\":\"tx-001\",\"srcAccountId\":\"acc-123\",\"destAccountId\":\"acc-456\",\"amount\":50.0,\"currency\":\"EUR\",\"eventTime\":1702900000000}");
        log.info("2) {\"transactionId\":\"tx-002\",\"srcAccountId\":\"acc-123\",\"destAccountId\":\"acc-789\",\"amount\":75000.0,\"currency\":\"EUR\",\"eventTime\":1702900001000}");

        env.execute("Flink Fraud Detection - Full Application");
    }
}

