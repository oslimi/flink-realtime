package com.wslimi.demo.fraud.datagen;

import com.wslimi.demo.fraud.model.Transaction;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Transaction Generator
 *
 * Generates realistic transaction data with configurable patterns:
 * - Random transactions within configured ranges
 * - Optional fraud pattern injection (small tx followed by large tx)
 * - Realistic account distribution
 */
@Slf4j
public class TransactionGenerator {

    private final GeneratorConfig config;
    private final Random random;
    private long messageCount = 0;

    // State for fraud pattern generation
    private String lastFraudAccount = null;
    private boolean fraudPatternInProgress = false;

    public TransactionGenerator(GeneratorConfig config) {
        this.config = config;
        this.random = new Random();
    }

    /**
     * Generate next transaction
     *
     * @return Transaction object
     */
    public Transaction generateTransaction() {
        messageCount++;

        // Decide if we should inject fraud pattern
        if (config.isEnableFraudPatterns() && shouldTriggerFraudPattern()) {
            return generateFraudPatternTransaction();
        }

        // Otherwise generate normal transaction
        return generateNormalTransaction();
    }

    /**
     * Generate a normal random transaction
     */
    private Transaction generateNormalTransaction() {
        String txId = "tx-" + UUID.randomUUID().toString().substring(0, 8);
        String srcAccount = getRandomAccountId();
        String destAccount = getRandomAccountId();

        // Ensure src and dest are different
        while (srcAccount.equals(destAccount)) {
            destAccount = getRandomAccountId();
        }

        double amount = ThreadLocalRandom.current().nextDouble(
            config.getMinAmount(),
            config.getMaxAmount()
        );

        String currency = getRandomCurrency();
        long eventTime = System.currentTimeMillis();

        Transaction tx = new Transaction(txId, srcAccount, destAccount,
                                        Math.round(amount * 100.0) / 100.0,
                                        currency, eventTime);

        if (config.isVerbose()) {
            log.info("Generated normal transaction: {}", tx);
        }

        return tx;
    }

    /**
     * Generate fraud pattern: small transaction followed by large one
     * This simulates the pattern detected by StatefulFraudDetectionDemoApp
     */
    private Transaction generateFraudPatternTransaction() {
        String txId = "tx-fraud-" + UUID.randomUUID().toString().substring(0, 8);
        String srcAccount;
        String destAccount;
        double amount;

        if (!fraudPatternInProgress) {
            // First transaction: small amount
            srcAccount = getRandomAccountId();
            lastFraudAccount = srcAccount;
            destAccount = getRandomAccountId();
            amount = config.getFraudSmallAmount();
            fraudPatternInProgress = true;

            log.warn("🚨 FRAUD PATTERN STARTED: Small transaction ({}) from account {}",
                     amount, srcAccount);
        } else {
            // Second transaction: large amount from same account
            srcAccount = lastFraudAccount;
            destAccount = getRandomAccountId();
            amount = config.getFraudLargeAmount();
            fraudPatternInProgress = false;
            lastFraudAccount = null;

            log.warn("🚨 FRAUD PATTERN COMPLETED: Large transaction ({}) from account {}",
                     amount, srcAccount);
        }

        while (srcAccount.equals(destAccount)) {
            destAccount = getRandomAccountId();
        }

        String currency = getRandomCurrency();
        long eventTime = System.currentTimeMillis();

        return new Transaction(txId, srcAccount, destAccount,
                              Math.round(amount * 100.0) / 100.0,
                              currency, eventTime);
    }

    /**
     * Decide whether to trigger a fraud pattern
     */
    private boolean shouldTriggerFraudPattern() {
        // Don't start new pattern if one is in progress
        if (fraudPatternInProgress) {
            return true; // Complete the current pattern
        }

        // Random chance to start new fraud pattern
        return random.nextDouble() < config.getFraudPatternProbability();
    }

    /**
     * Get random account ID from configured range
     */
    private String getRandomAccountId() {
        int accountIndex = random.nextInt(config.getNumberOfAccounts());
        return config.getAccountId(accountIndex);
    }

    /**
     * Get random currency from configured list
     */
    private String getRandomCurrency() {
        return config.getCurrencies().get(
            random.nextInt(config.getCurrencies().size())
        );
    }

    /**
     * Get total messages generated so far
     */
    public long getMessageCount() {
        return messageCount;
    }

    /**
     * Check if generation should continue
     */
    public boolean shouldContinue() {
        if (config.getTotalMessages() == 0) {
            return true; // Infinite mode
        }
        return messageCount < config.getTotalMessages();
    }
}

