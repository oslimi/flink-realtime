package com.wslimi.demo.fraud;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wslimi.demo.fraud.model.FraudNaiveAlert;
import com.wslimi.demo.fraud.model.Transaction;
import com.wslimi.demo.fraud.processor.NaiveFraudDetectionProcessor;
import com.wslimi.demo.fraud.serde.FraudNaiveAlertSerializationSchema;
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
 * DEMO 2: NAIVE FRAUD DETECTION (STATELESS)
 * =============================================================================
 *
 * Building on KafkaSourceDemoApp, this demo adds:
 * - Simple fraud detection rule (amount > threshold)
 * - KeyedProcessFunction (stateless version)
 * - Kafka Sink for sending alerts
 *
 * Concepts introduced:
 * - keyBy() for stream partitioning
 * - KeyedProcessFunction
 * - Kafka Sink configuration
 * - Custom serialization schema
 *
 * Fraud Rule: Flag any transaction with amount > 10,000
 *
 * This is a NAIVE approach - no state, no pattern detection.
 */
@Slf4j
public class NaiveFraudDetectionDemo {

    public static final String KAFKA_BOOTSTRAP = "localhost:9092";
    public static final String TRANSACTIONS_TOPIC = "transactions";
    public static final String FRAUD_ALERTS_TOPIC = "fraud-alerts-naive";

    public static void main(String[] args) throws Exception {
        log.info("=== DEMO: Naive Fraud Detection (Stateless) ===");

        // 1. Create environment with Web UI on port 8085
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8085);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(2);

        log.info("Flink Web UI: http://localhost:8085");

        ObjectMapper objectMapper = new ObjectMapper();

        // 2. Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(TRANSACTIONS_TOPIC)
                .setGroupId("naive-fraud-detection-demo")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 3. Kafka Sink for fraud alerts
        log.info("Configuring Kafka sink - topic: {}", FRAUD_ALERTS_TOPIC);
        KafkaSink<FraudNaiveAlert> alertsSink = KafkaSink.<FraudNaiveAlert>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setRecordSerializer(KafkaRecordSerializationSchema.<FraudNaiveAlert>builder()
                        .setTopic(FRAUD_ALERTS_TOPIC)
                        .setValueSerializationSchema(new FraudNaiveAlertSerializationSchema(objectMapper))
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // 4. Read and parse transactions
        DataStream<Transaction> transactions = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(json -> objectMapper.readValue(json, Transaction.class))
                .name("[JAVA] JSON to Transaction");

        // 5. Apply naive fraud detection
        //    - keyBy() partitions data by srcAccountId (required for KeyedProcessFunction)
        //    - process() applies our stateless fraud detection logic
        log.info("Fraud rule: amount > 10,000 triggers alert");
        DataStream<FraudNaiveAlert> fraudAlerts = transactions
                .keyBy(Transaction::srcAccountId)
                .process(new NaiveFraudDetectionProcessor())
                .name("[JAVA] Naive Fraud Detection");

        // 6. Sink alerts to Kafka
        fraudAlerts.sinkTo(alertsSink).name("[JAVA] Kafka Alerts Sink");

        // 7. Also print for demo visibility
        fraudAlerts.print("FRAUD_ALERT");

        // Execute
        log.info("Starting Naive Fraud Detection Demo");
        log.info("Test with amount > 10000:");
        log.info("{\"transactionId\":\"tx-001\",\"srcAccountId\":\"acc-123\",\"destAccountId\":\"acc-456\",\"amount\":15000.0,\"currency\":\"EUR\",\"eventTime\":1702900000000}");
        env.execute("Naive Fraud Detection Demo");
    }
}

