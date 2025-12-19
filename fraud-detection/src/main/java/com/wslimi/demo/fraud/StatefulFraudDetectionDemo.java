package com.wslimi.demo.fraud;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wslimi.demo.fraud.model.FraudAdvancedAlert;
import com.wslimi.demo.fraud.model.Transaction;
import com.wslimi.demo.fraud.processor.AdvancedStatefulFraudDetectionProcessor;
import com.wslimi.demo.fraud.serde.FraudAdvancedAlertSerializationSchema;
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
 * DEMO 3: STATEFUL FRAUD DETECTION
 * =============================================================================
 *
 * Building on NaiveFraudDetectionDemo, this demo introduces STATE:
 * - ValueState to remember previous transaction
 * - Pattern-based fraud detection
 * - Stateful KeyedProcessFunction
 *
 * Concepts introduced:
 * - ValueState<T> - keyed state storing a single value
 * - State initialization in open()
 * - State read/write in processElement()
 * - State scoped per key (accountId)
 *
 * Fraud Rule: Small transaction (< 100) followed by large transaction (> 50,000)
 *
 * This pattern detects when attackers test with small amounts before draining.
 */
@Slf4j
public class StatefulFraudDetectionDemo {

    public static final String KAFKA_BOOTSTRAP = "localhost:9092";
    public static final String TRANSACTIONS_TOPIC = "transactions";
    public static final String FRAUD_ALERTS_TOPIC = "fraud-alerts-stateful";

    public static void main(String[] args) throws Exception {
        log.info("=== DEMO: Stateful Fraud Detection (with ValueState) ===");

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
                .setGroupId("stateful-fraud-detection-demo")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 3. Kafka Sink for fraud alerts
        log.info("Configuring Kafka sink - topic: {}", FRAUD_ALERTS_TOPIC);
        KafkaSink<FraudAdvancedAlert> alertsSink = KafkaSink.<FraudAdvancedAlert>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FraudAdvancedAlert>builder()
                        .setTopic(FRAUD_ALERTS_TOPIC)
                        .setValueSerializationSchema(new FraudAdvancedAlertSerializationSchema(objectMapper))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 4. Read and parse transactions
        DataStream<Transaction> transactions = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(json -> objectMapper.readValue(json, Transaction.class))
                .name("JSON to Transaction");

        // 5. Apply STATEFUL fraud detection
        //    - Uses ValueState to remember previous transaction per account
        //    - Detects pattern: small tx followed by large tx
        log.info("Fraud rule: small amount (< 100) followed by large amount (> 50,000)");
        DataStream<FraudAdvancedAlert> fraudAlerts = transactions
                .keyBy(Transaction::srcAccountId)
                .process(new AdvancedStatefulFraudDetectionProcessor())
                .name("Stateful Fraud Detection");

        // 6. Sink alerts to Kafka
        fraudAlerts.sinkTo(alertsSink).name("Kafka Alerts Sink");

        // 7. Print for demo visibility
        fraudAlerts.print("FRAUD_ALERT");

        // Execute
        log.info("Starting Stateful Fraud Detection Demo");
        log.info("Test pattern - send these two messages for same account:");
        log.info("1) Small: {\"transactionId\":\"tx-001\",\"srcAccountId\":\"acc-123\",\"destAccountId\":\"acc-456\",\"amount\":50.0,\"currency\":\"EUR\",\"eventTime\":1702900000000}");
        log.info("2) Large: {\"transactionId\":\"tx-002\",\"srcAccountId\":\"acc-123\",\"destAccountId\":\"acc-789\",\"amount\":75000.0,\"currency\":\"EUR\",\"eventTime\":1702900001000}");
        env.execute("Stateful Fraud Detection Demo");
    }
}

