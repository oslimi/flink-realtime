package com.wslimi.demo.fraud.config;

import com.wslimi.demo.fraud.datagen.GeneratorConfig;
import com.wslimi.demo.fraud.util.EnvironmentDetector;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory class for creating GeneratorConfig instances
 * with different profiles
 */
@Slf4j
public final class GeneratorConfigFactory {

    private GeneratorConfigFactory() {
        // Factory class - prevent instantiation
    }

    /**
     * Create configuration based on profile name
     */
    public static GeneratorConfig createConfig(String profile) {
        return switch (profile.toLowerCase()) {
            case "fraud" -> createFraudTestConfig();
            case "high" -> createHighVolumeConfig();
            case "low" -> createLowVolumeConfig();
            case "demo" -> createDemoConfig();
            default -> createDefaultConfig();
        };
    }

    /**
     * Default configuration: Moderate rate, no fraud patterns
     */
    public static GeneratorConfig createDefaultConfig() {
        return GeneratorConfig.builder()
            .kafkaBootstrapServers(EnvironmentDetector.getKafkaBootstrapServers())
            .topic("transactions")
            .messagesPerSecond(10)
            .totalMessages(0) // Infinite
            .minAmount(10.0)
            .maxAmount(5000.0)
            .numberOfAccounts(100)
            .verbose(false)
            .build();
    }

    /**
     * Fraud test configuration: Enabled fraud patterns for testing
     */
    public static GeneratorConfig createFraudTestConfig() {
        log.info("Using FRAUD TEST profile - fraud patterns enabled");
        return GeneratorConfig.builder()
            .kafkaBootstrapServers(EnvironmentDetector.getKafkaBootstrapServers())
            .topic("transactions")
            .messagesPerSecond(5)
            .totalMessages(100_000)
            .minAmount(10.0)
            .maxAmount(500000.0)
            .numberOfAccounts(50)
            .enableFraudPatterns(true)
            .fraudPatternProbability(0.1) // 10% fraud rate for testing
            .fraudSmallAmount(50.0)
            .fraudLargeAmount(75000.0)
            .verbose(true)
            .build();
    }

    /**
     * High volume configuration: For performance testing
     */
    public static GeneratorConfig createHighVolumeConfig() {
        log.info("Using HIGH VOLUME profile");
        return GeneratorConfig.builder()
            .kafkaBootstrapServers(EnvironmentDetector.getKafkaBootstrapServers())
            .topic("transactions")
            .messagesPerSecond(100)
            .totalMessages(100_00)
            .minAmount(10.0)
            .maxAmount(100000.0)
            .numberOfAccounts(1000)
            .enableFraudPatterns(true)
            .fraudPatternProbability(0.02)
            .verbose(false)
            .build();
    }

    /**
     * Low volume configuration: For easy observation
     */
    public static GeneratorConfig createLowVolumeConfig() {
        log.info("Using LOW VOLUME profile");
        return GeneratorConfig.builder()
            .kafkaBootstrapServers(EnvironmentDetector.getKafkaBootstrapServers())
            .topic("transactions")
            .messagesPerSecond(1)
            .totalMessages(100) // Limited messages
            .minAmount(10.0)
            .maxAmount(1000.0)
            .numberOfAccounts(10)
            .verbose(true)
            .build();
    }

    /**
     * Demo configuration: Balanced for presentation
     */
    public static GeneratorConfig createDemoConfig() {
        log.info("Using DEMO profile - optimized for presentations");
        return GeneratorConfig.builder()
            .kafkaBootstrapServers(EnvironmentDetector.getKafkaBootstrapServers())
            .topic("transactions")
            .messagesPerSecond(5)
            .totalMessages(100_000)
            .minAmount(10.0)
            .maxAmount(5000.0)
            .numberOfAccounts(20)
            .enableFraudPatterns(true)
            .fraudPatternProbability(0.15) // 15% for demo visibility
            .fraudSmallAmount(50.0)
            .fraudLargeAmount(75000.0)
            .verbose(true)
            .build();
    }
}

