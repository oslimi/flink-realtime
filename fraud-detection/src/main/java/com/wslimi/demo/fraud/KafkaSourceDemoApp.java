package com.wslimi.demo.fraud;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wslimi.demo.fraud.model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

/**
 * =============================================================================
 * DEMO 1: KAFKA SOURCE WITH JSON DESERIALIZATION
 * =============================================================================
 *
 * This is the foundation demo showing:
 * - How to create a Flink execution environment
 * - How to configure a Kafka source
 * - How to deserialize JSON to POJO
 * - How to print stream to console
 *
 * Concepts introduced:
 * - StreamExecutionEnvironment
 * - KafkaSource
 * - DataStream
 * - map() transformation
 * - print() sink
 *
 * Run this demo first to verify Kafka connectivity.
 */
@Slf4j
public class KafkaSourceDemoApp {

    public static final String KAFKA_BOOTSTRAP = "localhost:9092";
    public static final String TRANSACTIONS_TOPIC = "transactions";

    public static void main(String[] args) throws Exception {
        log.info("=== DEMO: Kafka Source with JSON Deserialization ===");

        // 1. Create Flink execution environment with Web UI
        Configuration config = new Configuration();
        config.set(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setParallelism(2);

        log.info("Flink Web UI: http://localhost:8082");

        // 2. Create ObjectMapper for JSON deserialization
        ObjectMapper objectMapper = new ObjectMapper();

        // 3. Configure Kafka source
        log.info("Configuring Kafka source - topic: {}", TRANSACTIONS_TOPIC);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(TRANSACTIONS_TOPIC)
                .setGroupId("kafka-source-demo")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 4. Create DataStream from Kafka source
        DataStream<String> rawStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .name("Kafka Raw Stream");

        // 5. Parse JSON to Transaction POJO
        DataStream<Transaction> transactions = rawStream
                .map(json -> objectMapper.readValue(json, Transaction.class))
                .name("JSON to Transaction");

        DataStream<Transaction> multipliedTransactions = transactions
                .map(tx -> tx.multiplyBy(Math.PI))
                .filter(tx -> tx.amount() > 900.0 && Objects.equals(tx.currency(), "EUR"));


        // 6. Print to console (simple sink for debugging)
        transactions.print("TRANSACTION");

        multipliedTransactions.print("MULTIPLIED_TRANSACTION_EUR");

        // 7. Execute the job
        log.info("Starting Kafka Source Demo");
        log.info("Send test message:");
        log.info("{\"transactionId\":\"tx-001\",\"srcAccountId\":\"acc-123\",\"destAccountId\":\"acc-456\",\"amount\":100.0,\"currency\":\"EUR\",\"eventTime\":1702900000000}");
        env.execute("Kafka Source Demo");
    }
}

