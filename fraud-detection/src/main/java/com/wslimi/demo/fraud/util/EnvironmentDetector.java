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

    public static String getPostgresUrl() {
        String explicitUrl = System.getenv("POSTGRES_URL");
        if (explicitUrl != null && !explicitUrl.isEmpty()) {
            return explicitUrl;
        }

        if (isDockerEnvironment()) {
            return "jdbc:postgresql://postgresql:5432/streaming_demo";
        }

        return "jdbc:postgresql://localhost:5432/streaming_demo";
    }

    public static String getPostgresUser() {
        String user = System.getenv("POSTGRES_USER");
        return (user != null && !user.isEmpty()) ? user : "app_user";
    }

    public static String getPostgresPassword() {
        String password = System.getenv("POSTGRES_PASSWORD");
        return (password != null && !password.isEmpty()) ? password : "app_password";
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

