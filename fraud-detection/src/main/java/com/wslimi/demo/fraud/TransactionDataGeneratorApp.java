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

@Slf4j
public class TransactionDataGeneratorApp {

    public static void main(String[] args) throws Exception {
        String profile = args.length > 0 ? args[0] : "default";
        GeneratorConfig config = GeneratorConfigFactory.createConfig(profile);

        log.info("Starting Transaction Data Generator - profile: {}", profile);
        logConfiguration(config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);

        DataStream<Transaction> transactions = env
            .addSource(new TransactionSourceFunction(config))
            .name("Transaction Source")
            .uid("tx-source")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Transaction>forMonotonousTimestamps()
                    .withTimestampAssigner((tx, ts) -> tx.eventTime())
            );

        KafkaSink<Transaction> sink = KafkaSink.<Transaction>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(config.getTopic())
                    .setValueSerializationSchema(new TransactionSerializationSchema())
                    .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        transactions.sinkTo(sink).name("Kafka Sink").uid("kafka-sink");

        log.info("Flink Web UI: http://localhost:{}", env.getConfiguration().get(RestOptions.PORT));
        env.execute("Transaction Data Generator");
    }

    private static void logConfiguration(GeneratorConfig config) {
        log.info("Kafka: {} | Topic: {} | Rate: {}/sec | Accounts: {}",
            config.getKafkaBootstrapServers(), config.getTopic(),
            config.getMessagesPerSecond(), config.getNumberOfAccounts());
        if (config.isEnableFraudPatterns()) {
            log.info("Fraud patterns enabled: {}% probability", config.getFraudPatternProbability() * 100);
        }
    }
}
