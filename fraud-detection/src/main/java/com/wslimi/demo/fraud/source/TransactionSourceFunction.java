package com.wslimi.demo.fraud.source;

import com.wslimi.demo.fraud.datagen.GeneratorConfig;
import com.wslimi.demo.fraud.datagen.TransactionGenerator;
import com.wslimi.demo.fraud.model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

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
        log.info("Generator started - subtask {}/{}",
            getRuntimeContext().getIndexOfThisSubtask() + 1,
            getRuntimeContext().getNumberOfParallelSubtasks());
    }

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        while (isRunning && generator.shouldContinue()) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(generator.generateTransaction());
            }
            Thread.sleep(config.getDelayMillis());
        }
        log.info("Generation completed. Total: {}", generator.getMessageCount());
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

