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

@Slf4j
public class AdvancedStatefulFraudDetectionProcessor extends KeyedProcessFunction<String, Transaction, FraudAdvancedAlert> {

    private static final double SMALL_THRESHOLD = 100.0;
    private static final double LARGE_THRESHOLD = 50000.0;

    private transient ValueState<Transaction> prevTxState;

    @Override
    public void open(Configuration parameters) {
        prevTxState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("prev-tx", Transaction.class));
    }

    @Override
    public void processElement(Transaction tx, Context ctx, Collector<FraudAdvancedAlert> out) throws Exception {
        Transaction prevTx = prevTxState.value();

        if (prevTx != null && prevTx.amount() < SMALL_THRESHOLD && tx.amount() > LARGE_THRESHOLD) {
            log.info("FRAUD: {} small({}) -> large({})", tx.srcAccountId(), prevTx.amount(), tx.amount());

            out.collect(FraudAdvancedAlert.builder()
                    .AlertId(UUID.randomUUID().toString())
                    .timestamp(System.currentTimeMillis())
                    .previousTransaction(prevTx)
                    .currentTransaction(tx)
                    .comment(String.format("[JAVA] Fraud pattern: small transaction (<%.2f) followed by large transaction (>%.2f)",
                            SMALL_THRESHOLD, LARGE_THRESHOLD))
                    .processingDuration(ctx.timerService().currentProcessingTime() - tx.eventTime())
                    .build());

            prevTxState.clear();
        } else {
            prevTxState.update(tx);
        }
    }
}
