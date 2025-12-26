package com.wslimi.demo.fraud.processor;

import com.wslimi.demo.fraud.model.FraudNaiveAlert;
import com.wslimi.demo.fraud.model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Simple fraud detection processor.
 * Flags a transaction as fraudulent if the amount exceeds a threshold.
 */
@Slf4j
public class NaiveFraudDetectionProcessor extends KeyedProcessFunction<String, Transaction, FraudNaiveAlert> {

    private static final double FRAUD_THRESHOLD = 10000.0;

    @Override
    public void processElement(Transaction transaction,
                               KeyedProcessFunction<String, Transaction, FraudNaiveAlert>.Context context,
                               Collector<FraudNaiveAlert> collector) throws Exception {

        if (transaction.amount() > FRAUD_THRESHOLD) {
            log.warn("🚨 FRAUD DETECTED! Transaction {} from account {} with amount {} exceeds threshold {}",
                    transaction.transactionId(),
                    transaction.srcAccountId(),
                    transaction.amount(),
                    FRAUD_THRESHOLD);

            // Emit fraudulent transaction
            FraudNaiveAlert alert = new FraudNaiveAlert(transaction.transactionId(), System.currentTimeMillis(), transaction, "Amount exceeds threshold");
            collector.collect(alert);
        } else {
            log.info("✅ Transaction {} is valid. Amount: {}",
                    transaction.transactionId(),
                    transaction.amount());
        }
    }
}

