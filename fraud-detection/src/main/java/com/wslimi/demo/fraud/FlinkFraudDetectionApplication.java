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

@Slf4j
public class FlinkFraudDetectionApplication {

    public static final String KAFKA_BOOTSTRAP = "localhost:9092";
    public static final String TRANSACTIONS_TOPIC = "transactions";
    public static final String FRAUD_ALERTS_TOPIC = "fraud-alerts";
    public static final String FRAUD_REPORTS_TOPIC = "fraud-reports";

    // PostgreSQL configuration
    public static final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/streaming_demo";
    public static final String POSTGRES_USER = "app_user";
    public static final String POSTGRES_PASSWORD = "app_password";

    public static void main(String[] args) throws Exception {
        log.info("Creating Flink Execution Environment");

        // Enable Flink Web UI on localhost:8081
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8082);  // Use 8082 to avoid conflict with Docker Flink

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);

        log.info("Flink Web UI available at: http://localhost:8082");

        ObjectMapper objectMapper = new ObjectMapper();

        log.info("Creating Kafka Source");
        // Create Kafka source for transactions
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(TRANSACTIONS_TOPIC)
                .setGroupId("fraud-detection-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create Kafka sink for fraud alerts
        log.info("Creating Kafka Sink for fraud alerts");
        KafkaSink<FraudAdvancedAlert> alertsSink = KafkaSink.<FraudAdvancedAlert>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FraudAdvancedAlert>builder()
                        .setTopic(FRAUD_ALERTS_TOPIC)
                        .setValueSerializationSchema(new FraudAdvancedAlertSerializationSchema(objectMapper))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Create Kafka sink for fraud reports
        log.info("Creating Kafka Sink for fraud reports");
        KafkaSink<FraudReport> reportsSink = KafkaSink.<FraudReport>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FraudReport>builder()
                        .setTopic(FRAUD_REPORTS_TOPIC)
                        .setValueSerializationSchema(new FraudReportSerializationSchema(objectMapper))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Create PostgreSQL JDBC sink for fraud reports
        log.info("Creating PostgreSQL Sink for fraud reports");
        SinkFunction<FraudReport> postgresSink = JdbcSink.sink(
                "INSERT INTO fraud_reports (report_id, report_timestamp, window_start, window_end, account_id, total_alerts, total_fraud_amount, summary) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (report_id) DO UPDATE SET " +
                "total_alerts = EXCLUDED.total_alerts, " +
                "total_fraud_amount = EXCLUDED.total_fraud_amount, " +
                "summary = EXCLUDED.summary",
                (statement, report) -> {
                    statement.setString(1, report.reportId());
                    statement.setLong(2, report.reportTimestamp());
                    statement.setLong(3, report.windowStart());
                    statement.setLong(4, report.windowEnd());
                    statement.setString(5, report.accountId());
                    statement.setInt(6, report.totalAlerts());
                    statement.setDouble(7, report.totalFraudAmount());
                    statement.setString(8, report.summary());
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

        // Read from Kafka and parse JSON to Transaction
        log.info("Creating DataStream from Kafka Source");
        DataStream<Transaction> transactions = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Transactions Source")
                .map(json -> objectMapper.readValue(json, Transaction.class))
                .name("Parse JSON to Transaction");

        log.info("Applying Fraud Detection Processor");
        // Step 1: Fraud Detection - detects fraud patterns and emits alerts
        DataStream<FraudAdvancedAlert> fraudAlerts = transactions
                .keyBy(Transaction::srcAccountId)
                .process(new AdvancedStatefulFraudDetectionProcessor())
                .name("Fraud Detection");

        log.info("Applying Fraud Report Aggregator");
        // Step 2: Report Aggregation - receives alerts and generates periodic reports (every 1 minute)
        DataStream<FraudReport> fraudReports = fraudAlerts
                .keyBy(alert -> alert.currentTransaction().srcAccountId())
                .process(new FraudReportAggregatorProcessor())
                .name("Fraud Report Aggregator");

        // Sink fraud alerts to Kafka
        fraudAlerts
                .sinkTo(alertsSink)
                .name("Kafka Fraud Alerts Sink");
        fraudAlerts
                .print("ALERT");

        // Sink fraud reports to Kafka
        fraudReports
                .sinkTo(reportsSink)
                .name("Kafka Fraud Reports Sink");
        fraudReports
                .print("REPORT");

        // Sink fraud reports to PostgreSQL
        fraudReports
                .addSink(postgresSink)
                .name("PostgreSQL Fraud Reports Sink");

        log.info("Starting Fraud Detection Job - listening on topic: " + TRANSACTIONS_TOPIC);
        log.info("Fraud alerts will be sent to: " + FRAUD_ALERTS_TOPIC);
        log.info("Fraud reports will be sent to: " + FRAUD_REPORTS_TOPIC + " (every 1 minute)");
        log.info("Fraud reports will also be stored in PostgreSQL database: " + POSTGRES_URL);

        // Execute the Flink job
        env.execute("Fraud Detection Job");
    }

}


