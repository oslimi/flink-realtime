package com.wslimi.demo.fraud.source;

import com.wslimi.demo.fraud.datagen.GeneratorConfig;
import com.wslimi.demo.fraud.datagen.TransactionGenerator;
import com.wslimi.demo.fraud.model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * Custom Flink SourceFunction for generating transactions
 *
 * This source function:
 * - Generates transactions using TransactionGenerator
 * - Respects Flink's checkpointing mechanism
 * - Supports parallel execution
 * - Can be cancelled gracefully
 */
@Slf4j
public class TransactionSourceFunction extends RichParallelSourceFunction<Transaction> {

    private volatile boolean isRunning = true;
    private final GeneratorConfig config;
    private TransactionGenerator generator;

    public TransactionSourceFunction(GeneratorConfig config) {
        this.config = config;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        this.generator = new TransactionGenerator(config);

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();

        log.info("Transaction Generator started - subtask {}/{}",
                 subtaskIndex + 1, parallelism);
    }

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        log.info("Starting transaction generation...");

        while (isRunning && generator.shouldContinue()) {
            Transaction transaction = generator.generateTransaction();

            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(transaction);
            }

            // Throttle to configured rate
            Thread.sleep(config.getDelayMillis());
        }

        log.info("Transaction generation completed. Total: {}", generator.getMessageCount());
    }

    @Override
    public void cancel() {
        log.info("Cancelling transaction generator...");
        isRunning = false;
    }
}

