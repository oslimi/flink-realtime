package com.wslimi.demo.fraud.util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class EnvironmentDetector {

    private EnvironmentDetector() {}

    public static String getKafkaBootstrapServers() {
        String explicitKafka = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (explicitKafka != null && !explicitKafka.isEmpty()) {
            return explicitKafka;
        }

        if (isDockerEnvironment()) {
            log.debug("Docker environment detected");
            return "broker:29092";
        }

        return "localhost:9092";
    }

    public static boolean isDockerEnvironment() {
        String hostname = System.getenv("HOSTNAME");
        String inDocker = System.getenv("IN_DOCKER");
        return (hostname != null && (hostname.startsWith("flink-") || hostname.contains("taskmanager")))
               || "true".equals(inDocker);
    }

    public static boolean isLocalEnvironment() {
        return !isDockerEnvironment();
    }
}

