package com.wslimi.demo.fraud;

import com.wslimi.demo.fraud.config.GeneratorConfigFactory;
import com.wslimi.demo.fraud.datagen.GeneratorConfig;
import com.wslimi.demo.fraud.model.Transaction;
import com.wslimi.demo.fraud.serde.TransactionSerializationSchema;
import com.wslimi.demo.fraud.source.TransactionSourceFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * =============================================================================
 * FLINK TRANSACTION DATA GENERATOR
 * =============================================================================
 *
 * Flink-based application that generates and sends Transaction data to Kafka.
 *
 * This generator uses Flink's DataStream API to:
 * - Generate transaction data using a custom SourceFunction
 * - Control generation rate and patterns
 * - Write to Kafka using Flink's Kafka connector
 * - Leverage Flink's parallelism and fault tolerance
 *
 * This producer is designed to feed data to Flink demo applications:
 * - NaiveFraudDetectionDemo
 * - StatefulFraudDetectionDemoApp
 * - TimerReportingDemoApp
 * - FlinkFraudDetectionFullApp
 *
 * Features:
 * - Configurable generation rate and patterns
 * - Optional fraud pattern injection for testing
 * - Flink's built-in checkpointing and fault tolerance
 * - Parallel generation capability
 */
@Slf4j
public class TransactionDataGeneratorApp {

    /**
     * Main entry point - Different configuration profiles
     */
    public static void main(String[] args) throws Exception {
        log.info("=".repeat(80));
        log.info("FLINK TRANSACTION DATA GENERATOR");
        log.info("=".repeat(80));

        // Parse command line arguments for profile selection
        String profile = args.length > 0 ? args[0] : "default";

        // Create configuration using factory
        GeneratorConfig config = GeneratorConfigFactory.createConfig(profile);

        // Print configuration
        logConfiguration(config);

        // Create Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable Web UI for local execution (only if running locally, not when deployed)
        if (env.getClass().getSimpleName().contains("Local")) {
            org.apache.flink.configuration.Configuration flinkConfig = new org.apache.flink.configuration.Configuration();
            flinkConfig.setString("rest.port", "8181");
            env.configure(flinkConfig);
        }

        // Parallelism is set via deployment script or uses default from Flink config
        // Don't set it here to allow flexibility during deployment

        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(10000); // Checkpoint every 10 seconds

        log.info("Creating Transaction Source...");

        // Create source
        DataStream<Transaction> transactionStream = env
            .addSource(new TransactionSourceFunction(config))
            .name("Transaction Generator Source")
            .uid("transaction-generator-source");

        // Add watermarks if needed (for event time processing)
        DataStream<Transaction> streamWithWatermarks = transactionStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Transaction>forMonotonousTimestamps()
                    .withTimestampAssigner((transaction, timestamp) -> transaction.eventTime())
            );

        log.info("Creating Kafka Sink...");

        // Create Kafka Sink
        KafkaSink<Transaction> kafkaSink = KafkaSink.<Transaction>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(config.getTopic())
                    .setValueSerializationSchema(new TransactionSerializationSchema())
                    .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        // Sink to Kafka
        streamWithWatermarks
            .sinkTo(kafkaSink)
            .name("Kafka Transaction Sink")
            .uid("kafka-transaction-sink");

        // Get the REST address and port dynamically
        String restAddress = env.getConfiguration().get(RestOptions.ADDRESS);
        if (restAddress == null || restAddress.isEmpty()) {
            restAddress = "localhost";
        }

        int restPort = env.getConfiguration().get(RestOptions.PORT);
        log.info("Starting Flink Data Generator Job...");
        log.info("Flink Web UI available at: http://{}:{}", restAddress, restPort);
        log.info("Press Ctrl+C to stop...");
        log.info("=".repeat(80));

        env.execute("Transaction Data Generator");
    }

    /**
     * Log the current configuration
     */
    private static void logConfiguration(GeneratorConfig config) {
        log.info("Configuration:");
        log.info("  Kafka Broker: {}", config.getKafkaBootstrapServers());
        log.info("  Topic: {}", config.getTopic());
        log.info("  Messages/sec: {}", config.getMessagesPerSecond());
        log.info("  Total messages: {}", config.getTotalMessages() == 0 ? "∞ (infinite)" : config.getTotalMessages());
        log.info("  Amount range: {} - {}", config.getMinAmount(), config.getMaxAmount());
        log.info("  Number of accounts: {}", config.getNumberOfAccounts());
        log.info("  Currencies: {}", config.getCurrencies());
        log.info("  Fraud patterns enabled: {}", config.isEnableFraudPatterns());
        if (config.isEnableFraudPatterns()) {
            log.info("    - Fraud probability: {}%", config.getFraudPatternProbability() * 100);
            log.info("    - Small amount: {}", config.getFraudSmallAmount());
            log.info("    - Large amount: {}", config.getFraudLargeAmount());
        }
        log.info("=".repeat(80));
    }
}
