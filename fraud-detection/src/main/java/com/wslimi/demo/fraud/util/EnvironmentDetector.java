package com.wslimi.demo.fraud.util;

import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for detecting runtime environment
 * and providing environment-specific configurations
 */
@Slf4j
public final class EnvironmentDetector {

    private EnvironmentDetector() {
        // Utility class - prevent instantiation
    }

    /**
     * Automatically detect Kafka bootstrap servers based on environment
     * - Docker deployment: uses broker:29092
     * - IDE/local execution: uses localhost:9092
     */
    public static String getKafkaBootstrapServers() {
        // Check if running in Docker by looking for common Docker environment indicators
        String hostname = System.getenv("HOSTNAME");
        String inDocker = System.getenv("IN_DOCKER");

        // If KAFKA_BOOTSTRAP_SERVERS is explicitly set, use it
        String explicitKafka = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (explicitKafka != null && !explicitKafka.isEmpty()) {
            log.info("Using explicit KAFKA_BOOTSTRAP_SERVERS: {}", explicitKafka);
            return explicitKafka;
        }

        // Auto-detect: if hostname starts with 'flink-' or contains 'taskmanager', we're in Docker
        if (hostname != null && (hostname.startsWith("flink-") || hostname.contains("taskmanager") || "true".equals(inDocker))) {
            log.info("Detected Docker environment, using internal broker address: broker:29092");
            return "broker:29092";
        }

        // Default to localhost for IDE/local execution
        log.info("Detected local environment, using localhost: localhost:9092");
        return "localhost:9092";
    }

    /**
     * Check if running in Docker environment
     */
    public static boolean isDockerEnvironment() {
        String hostname = System.getenv("HOSTNAME");
        String inDocker = System.getenv("IN_DOCKER");
        return (hostname != null && (hostname.startsWith("flink-") || hostname.contains("taskmanager")))
               || "true".equals(inDocker);
    }

    /**
     * Check if running in local/IDE environment
     */
    public static boolean isLocalEnvironment() {
        return !isDockerEnvironment();
    }
}

