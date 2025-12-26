package com.wslimi.demo.fraud.config;

import com.wslimi.demo.fraud.datagen.GeneratorConfig;
import com.wslimi.demo.fraud.util.EnvironmentDetector;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class GeneratorConfigFactory {

    private GeneratorConfigFactory() {}

    public static GeneratorConfig createConfig(String profile) {
        return switch (profile.toLowerCase()) {
            case "fraud" -> createFraudTestConfig();
            case "high" -> createHighVolumeConfig();
            case "low" -> createLowVolumeConfig();
            case "demo" -> createDemoConfig();
            default -> createDefaultConfig();
        };
    }

    public static GeneratorConfig createDefaultConfig() {
        return GeneratorConfig.builder()
            .kafkaBootstrapServers(EnvironmentDetector.getKafkaBootstrapServers())
            .topic("transactions")
            .messagesPerSecond(10)
            .totalMessages(0)
            .minAmount(10.0)
            .maxAmount(5000.0)
            .numberOfAccounts(100)
            .verbose(false)
            .build();
    }

    public static GeneratorConfig createFraudTestConfig() {
        log.info("Using FRAUD TEST profile");
        return GeneratorConfig.builder()
            .kafkaBootstrapServers(EnvironmentDetector.getKafkaBootstrapServers())
            .topic("transactions")
            .messagesPerSecond(5)
            .totalMessages(1_000)
            .minAmount(10.0)
            .maxAmount(500000.0)
            .numberOfAccounts(50)
            .enableFraudPatterns(true)
            .fraudPatternProbability(0.5)
            .fraudSmallAmount(50.0)
            .fraudLargeAmount(500000.0)
            .verbose(true)
            .build();
    }

    public static GeneratorConfig createHighVolumeConfig() {
        log.info("Using HIGH VOLUME profile");
        return GeneratorConfig.builder()
            .kafkaBootstrapServers(EnvironmentDetector.getKafkaBootstrapServers())
            .topic("transactions")
            .messagesPerSecond(100)
            .totalMessages(10_000)
            .minAmount(10.0)
            .maxAmount(100000.0)
            .numberOfAccounts(1000)
            .enableFraudPatterns(true)
            .fraudPatternProbability(0.02)
            .verbose(false)
            .build();
    }

    public static GeneratorConfig createLowVolumeConfig() {
        log.info("Using LOW VOLUME profile");
        return GeneratorConfig.builder()
            .kafkaBootstrapServers(EnvironmentDetector.getKafkaBootstrapServers())
            .topic("transactions")
            .messagesPerSecond(1)
            .totalMessages(100)
            .minAmount(10.0)
            .maxAmount(1000.0)
            .numberOfAccounts(10)
            .verbose(true)
            .build();
    }

    public static GeneratorConfig createDemoConfig() {
        log.info("Using DEMO profile");
        return GeneratorConfig.builder()
            .kafkaBootstrapServers(EnvironmentDetector.getKafkaBootstrapServers())
            .topic("transactions")
            .messagesPerSecond(5)
            .totalMessages(100_000)
            .minAmount(10.0)
            .maxAmount(5000.0)
            .numberOfAccounts(20)
            .enableFraudPatterns(true)
            .fraudPatternProbability(0.15)
            .fraudSmallAmount(50.0)
            .fraudLargeAmount(75000.0)
            .verbose(true)
            .build();
    }
}
