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
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * =============================================================================
 * DEMO 4: TIMERS AND PERIODIC REPORTING
 * =============================================================================
 *
 * Building on StatefulFraudDetectionDemo, this demo introduces TIMERS:
 * - Processing time timers for periodic actions
 * - ListState to collect alerts over time
 * - onTimer() callback for scheduled processing
 * - Chained processors (detection → aggregation)
 *
 * Concepts introduced:
 * - context.timerService().registerProcessingTimeTimer()
 * - onTimer() callback method
 * - ListState<T> - keyed state storing a list
 * - Chaining multiple KeyedProcessFunctions
 *
 * Features:
 * - Fraud detection (same as previous demo)
 * - Periodic reports every 60 seconds aggregating all alerts
 */
@Slf4j
public class TimerReportingDemo {

    public static final String KAFKA_BOOTSTRAP = "localhost:9092";
    public static final String TRANSACTIONS_TOPIC = "transactions";
    public static final String FRAUD_ALERTS_TOPIC = "fraud-alerts";
    public static final String FRAUD_REPORTS_TOPIC = "fraud-reports";

    public static void main(String[] args) throws Exception {
        log.info("=== DEMO: Timers and Periodic Reporting ===");

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
                .setGroupId("timer-reporting-demo")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 3. Kafka Sink for alerts
        KafkaSink<FraudAdvancedAlert> alertsSink = KafkaSink.<FraudAdvancedAlert>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FraudAdvancedAlert>builder()
                        .setTopic(FRAUD_ALERTS_TOPIC)
                        .setValueSerializationSchema(new FraudAdvancedAlertSerializationSchema(objectMapper))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 4. Kafka Sink for periodic reports
        log.info("Reports will be generated every 60 seconds");
        KafkaSink<FraudReport> reportsSink = KafkaSink.<FraudReport>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FraudReport>builder()
                        .setTopic(FRAUD_REPORTS_TOPIC)
                        .setValueSerializationSchema(new FraudReportSerializationSchema(objectMapper))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 5. Read and parse transactions
        DataStream<Transaction> transactions = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(json -> objectMapper.readValue(json, Transaction.class))
                .name("JSON to Transaction");

        // 6. First processor: Fraud detection (stateful)
        log.info("Pipeline: Transactions → Fraud Detection → Report Aggregation");
        DataStream<FraudAdvancedAlert> fraudAlerts = transactions
                .keyBy(Transaction::srcAccountId)
                .process(new AdvancedStatefulFraudDetectionProcessor())
                .name("Stateful Fraud Detection");

        // 7. Second processor: Report aggregation with TIMERS
        //    - Receives alerts from previous processor
        //    - Collects alerts in ListState
        //    - Timer fires every 60 seconds to emit report
        DataStream<FraudReport> fraudReports = fraudAlerts
                .keyBy(alert -> alert.currentTransaction().srcAccountId())
                .process(new FraudReportAggregatorProcessor())
                .name("Timer-based Report Aggregator");

        // 8. Sink to Kafka
        fraudAlerts.sinkTo(alertsSink).name("Alerts Kafka Sink");
        fraudReports.sinkTo(reportsSink).name("Reports Kafka Sink");

        // 9. Print for demo visibility
        fraudAlerts.print("ALERT");
        fraudReports.print("REPORT");

        // Execute
        log.info("Starting Timer Reporting Demo");
        log.info("Alerts: {}", FRAUD_ALERTS_TOPIC);
        log.info("Reports: {} (every 60 seconds)", FRAUD_REPORTS_TOPIC);
        log.info("Test pattern:");
        log.info("1) {\"transactionId\":\"tx-001\",\"srcAccountId\":\"acc-123\",\"destAccountId\":\"acc-456\",\"amount\":50.0,\"currency\":\"EUR\",\"eventTime\":1702900000000}");
        log.info("2) {\"transactionId\":\"tx-002\",\"srcAccountId\":\"acc-123\",\"destAccountId\":\"acc-789\",\"amount\":75000.0,\"currency\":\"EUR\",\"eventTime\":1702900001000}");
        log.info("Wait 60 seconds to see the report...");
        env.execute("Timer Reporting Demo");
    }
}

