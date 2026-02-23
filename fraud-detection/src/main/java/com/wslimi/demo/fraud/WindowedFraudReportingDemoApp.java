package com.wslimi.demo.fraud;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wslimi.demo.fraud.model.FraudAdvancedAlert;
import com.wslimi.demo.fraud.model.FraudReport;
import com.wslimi.demo.fraud.model.Transaction;
import com.wslimi.demo.fraud.processor.AdvancedStatefulFraudDetectionProcessor;
import com.wslimi.demo.fraud.processor.WindowedFraudReportProcessor;
import com.wslimi.demo.fraud.serde.FraudAdvancedAlertSerializationSchema;
import com.wslimi.demo.fraud.serde.FraudReportSerializationSchema;
import com.wslimi.demo.fraud.util.EnvironmentDetector;
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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

/**
 * Windowed Fraud Reporting Demo
 *
 * Demonstrates:
 * - Fraud detection with stateful processing
 * - Window aggregation with time windows
 * - Watermark strategy
 * - Multiple sinks (Kafka + PostgreSQL)
 *
 * Architecture:
 *   Kafka (transactions)
 *        │
 *        ▼
 *   ┌──────────────────────┐
 *   │ Parse JSON → POJO    │
 *   └──────────────────────┘
 *        │
 *        ▼
 *   ┌──────────────────────┐
 *   │ keyBy(accountId)     │
 *   └──────────────────────┘
 *        │
 *        ▼
 *   ┌──────────────────────┐
 *   │ Fraud Detection      │ ← ValueState
 *   │ (Stateful)           │
 *   └──────────────────────┘
 *        │
 *        ├──────────────▶ Kafka (fraud-alerts)
 *        │
 *        ▼
 *   ┌──────────────────────┐
 *   │ keyBy(accountId)     │
 *   └──────────────────────┘
 *        │
 *        ▼
 *   ┌──────────────────────┐
 *   │ 5-Second Window      │
 *   │ (Tumbling)           │
 *   └──────────────────────┘
 *        │
 *        ▼
 *   ┌──────────────────────┐
 *   │ Aggregate Reports    │
 *   │ (Window Function)    │
 *   └──────────────────────┘
 *        │
 *        ├──────────────▶ Kafka (fraud-reports)
 *        │
 *        ▼
 *   ┌──────────────────────┐
 *   │ Convert to Row       │
 *   └──────────────────────┘
 *        │
 *        ▼
 *     PostgreSQL
 */
@Slf4j
public class WindowedFraudReportingDemoApp {

    public static final String TRANSACTIONS_TOPIC = "transactions";
    public static final String FRAUD_ALERTS_TOPIC = "fraud-alerts-windowed";
    public static final String FRAUD_REPORTS_TOPIC = "fraud-reports-windowed";

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = EnvironmentDetector.getKafkaBootstrapServers();
        String postgresUrl = EnvironmentDetector.getPostgresUrl();
        String postgresUser = EnvironmentDetector.getPostgresUser();
        String postgresPassword = EnvironmentDetector.getPostgresPassword();

        log.info("╔══════════════════════════════════════════════════════════════╗");
        log.info("║  WINDOWED FRAUD REPORTING DEMO                              ║");
        log.info("║  Using: Watermarks + Tumbling Windows                       ║");
        log.info("╚══════════════════════════════════════════════════════════════╝");

        // 1. Create environment with Web UI on port 8081
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(2);

        ObjectMapper mapper = new ObjectMapper();

        // Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(TRANSACTIONS_TOPIC)
                .setGroupId("java-windowed-fraud-reporting")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Sinks
        KafkaSink<FraudAdvancedAlert> alertsSink = KafkaSink.<FraudAdvancedAlert>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FraudAdvancedAlert>builder()
                        .setTopic(FRAUD_ALERTS_TOPIC)
                        .setValueSerializationSchema(new FraudAdvancedAlertSerializationSchema(mapper))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<FraudReport> reportsSink = KafkaSink.<FraudReport>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FraudReport>builder()
                        .setTopic(FRAUD_REPORTS_TOPIC)
                        .setValueSerializationSchema(new FraudReportSerializationSchema(mapper))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        SinkFunction<Row> postgresSink = JdbcSink.sink(
                "INSERT INTO fraud_reports (report_id, report_timestamp, window_start, window_end, account_id, total_alerts, total_fraud_amount, alert_ids, summary) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (report_id) DO UPDATE SET " +
                        "total_alerts = EXCLUDED.total_alerts, " +
                        "total_fraud_amount = EXCLUDED.total_fraud_amount, " +
                        "alert_ids = EXCLUDED.alert_ids, " +
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
                    statement.setString(9, (String) row.getField(8));
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(postgresUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(postgresUser)
                        .withPassword(postgresPassword)
                        .build()
        );

        // Pipeline
        DataStream<Transaction> transactions = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(json -> mapper.readValue(json, Transaction.class))
                .name("[JAVA] JSON to Transaction");

        DataStream<FraudAdvancedAlert> fraudAlerts = transactions
                .keyBy(Transaction::srcAccountId)
                .process(new AdvancedStatefulFraudDetectionProcessor())
                .name("[JAVA] Stateful Fraud Detection");

        DataStream<FraudReport> fraudReports = fraudAlerts
                .keyBy(alert -> alert.currentTransaction().srcAccountId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new WindowedFraudReportProcessor())
                .name("[JAVA] Windowed Report Aggregation");

        DataStream<Row> fraudReportRows = fraudReports
                .map(report -> Row.of(
                        report.reportId(),
                        report.reportTimestamp(),
                        report.windowStart(),
                        report.windowEnd(),
                        report.accountId(),
                        report.totalAlerts(),
                        report.totalFraudAmount(),
                        report.alertIds(),
                        report.summary()
                ))
                .name("[JAVA] FraudReport to Row");

        // Outputs
        fraudAlerts.sinkTo(alertsSink).name("[JAVA] Kafka Alerts Sink");
        fraudReports.sinkTo(reportsSink).name("[JAVA] Kafka Reports Sink");
        fraudReportRows.addSink(postgresSink).name("[JAVA] PostgreSQL Sink");

        fraudAlerts.print("🚨 ALERT");
        fraudReports.print("📊 REPORT");

        log.info("┌─────────────────────────────────────────────────────────────┐");
        log.info("│ Windowed Fraud Reporting Started                            │");
        log.info("├─────────────────────────────────────────────────────────────┤");
        log.info("│ Input:  Kafka topic '{}'", TRANSACTIONS_TOPIC);
        log.info("│ Output: Kafka topic '{}' (alerts)", FRAUD_ALERTS_TOPIC);
        log.info("│ Output: Kafka topic '{}' (reports)", FRAUD_REPORTS_TOPIC);
        log.info("│ Output: PostgreSQL table 'fraud_reports'");
        log.info("│ Window: 5 seconds (Tumbling)");
        log.info("│ Web UI: http://localhost:8084");
        log.info("└─────────────────────────────────────────────────────────────┘");

        env.execute("Windowed Fraud Reporting Demo");
    }
}

