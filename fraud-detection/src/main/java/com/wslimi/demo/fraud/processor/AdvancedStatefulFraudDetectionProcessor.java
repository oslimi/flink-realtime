package com.wslimi.demo.fraud.processor;

import com.wslimi.demo.fraud.model.FraudAdvancedAlert;
import com.wslimi.demo.fraud.model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.UUID;

/**
 * Stateful fraud detection processor.
 * Detects fraud pattern: small transaction (< 100) followed by large transaction (> 50000).
 */
@Slf4j
public class AdvancedStatefulFraudDetectionProcessor extends KeyedProcessFunction<String, Transaction, FraudAdvancedAlert> {

    private static final double SMALL_AMOUNT_THRESHOLD = 100.0;
    private static final double LARGE_AMOUNT_THRESHOLD = 50000.0;

    // State to track the previous transaction
    private transient ValueState<Transaction> previousTransactionState;

    @Override
    public void open(Configuration parameters) {
        previousTransactionState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("previous-transaction", Transaction.class));
    }

    @Override
    public void processElement(Transaction transaction,
                               KeyedProcessFunction<String, Transaction, FraudAdvancedAlert>.Context context,
                               Collector<FraudAdvancedAlert> collector) throws Exception {

        Transaction previousTransaction = previousTransactionState.value();

        // Check for fraud pattern: small transaction followed by large transaction
        if (previousTransaction != null
                && previousTransaction.amount() < SMALL_AMOUNT_THRESHOLD
                && transaction.amount() > LARGE_AMOUNT_THRESHOLD) {

            log.info("🚨 FRAUD PATTERN DETECTED! Account {} had a small transaction ({}) followed by large transaction ({})",
                    transaction.srcAccountId(),
                    previousTransaction.amount(),
                    transaction.amount());

            FraudAdvancedAlert alert = FraudAdvancedAlert.builder()
                    .AlertId(UUID.randomUUID().toString())
                    .timestamp(System.currentTimeMillis())
                    .previousTransaction(previousTransaction)
                    .currentTransaction(transaction)
                    .comment(String.format("[JAVA] Fraud pattern: small transaction (<%.2f) followed by large transaction (>%.2f)",
                            SMALL_AMOUNT_THRESHOLD, LARGE_AMOUNT_THRESHOLD))
                    .processingDuration(context.timerService().currentProcessingTime() - transaction.eventTime())
                    .build();

            // Emit alert
            collector.collect(alert);

            // Reset state after detecting fraud
            previousTransactionState.clear();
        } else {
            // Update state with current transaction
            if (transaction.amount() < SMALL_AMOUNT_THRESHOLD) {
                log.debug("📝 Small transaction detected for account {}: amount {}",
                        transaction.srcAccountId(),
                        transaction.amount());
            } else if (transaction.amount() <= LARGE_AMOUNT_THRESHOLD) {
                log.info("✅ Normal transaction for account {}: amount {}",
                        transaction.srcAccountId(),
                        transaction.amount());
            }
            previousTransactionState.update(transaction);
        }
    }
}

