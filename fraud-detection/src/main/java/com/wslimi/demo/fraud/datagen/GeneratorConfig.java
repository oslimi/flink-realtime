package com.wslimi.demo.fraud.datagen;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;

/**
 * Configuration for the Transaction Data Generator
 *
 * This class allows fine-grained control over:
 * - Rate of data generation
 * - Transaction amounts and patterns
 * - Account distribution
 * - Fraud simulation patterns
 */
@Getter
@Builder
public class GeneratorConfig implements Serializable {

    // Kafka Configuration
    @Builder.Default
    private String kafkaBootstrapServers = "localhost:9092";

    @Builder.Default
    private String topic = "transactions";

    // Generation Rate
    @Builder.Default
    private long messagesPerSecond = 10;

    @Builder.Default
    private long totalMessages = 10_000; // 0 = infinite

    // Transaction Amounts
    @Builder.Default
    private double minAmount = 1.0;

    @Builder.Default
    private double maxAmount = 100000.0;

    @Builder.Default
    private List<String> currencies = List.of("USD", "EUR", "GBP");

    // Account Configuration
    @Builder.Default
    private int numberOfAccounts = 100;

    @Builder.Default
    private String accountPrefix = "acc-";

    // Fraud Simulation (optional)
    @Builder.Default
    private boolean enableFraudPatterns = false;

    @Builder.Default
    private double fraudPatternProbability = 0.70; // 5% chance of fraud pattern

    @Builder.Default
    private double fraudSmallAmount = 50.0; // Small test transaction

    @Builder.Default
    private double fraudLargeAmount = 75000.0; // Large fraudulent transaction

    // Logging
    @Builder.Default
    private boolean verbose = false;

    /**
     * Calculate delay between messages in milliseconds
     */
    public long getDelayMillis() {
        return 1000L / messagesPerSecond;
    }

    /**
     * Get account ID by index
     */
    public String getAccountId(int index) {
        return accountPrefix + String.format("%04d", index);
    }
}

