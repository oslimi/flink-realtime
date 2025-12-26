package com.wslimi.demo.fraud.processor;

import com.wslimi.demo.fraud.model.FraudAdvancedAlert;
import com.wslimi.demo.fraud.model.FraudReport;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Fraud Report Aggregator Processor.
 * Receives fraud alerts as input and generates periodic reports every minute.
 * Only aggregates alerts - does not detect fraud.
 */
@Slf4j
public class FraudReportAggregatorProcessor extends KeyedProcessFunction<String, FraudAdvancedAlert, FraudReport> {

    private static final long REPORT_INTERVAL_MS = 60_000L; // 1 minute

    // State to collect alerts during the period
    private transient ListState<FraudAdvancedAlert> alertsState;

    // State for timer tracking
    private transient ValueState<Long> windowStartState;
    private transient ValueState<Long> nextTimerState;

    @Override
    public void open(Configuration parameters) {
        alertsState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("alerts-list", FraudAdvancedAlert.class));

        windowStartState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("window-start", Types.LONG));

        nextTimerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("next-timer", Types.LONG));
    }

    @Override
    public void processElement(FraudAdvancedAlert alert,
                               KeyedProcessFunction<String, FraudAdvancedAlert, FraudReport>.Context context,
                               Collector<FraudReport> collector) throws Exception {

        // Initialize timer if not set
        if (nextTimerState.value() == null) {
            long currentTime = context.timerService().currentProcessingTime();
            long nextTimer = currentTime + REPORT_INTERVAL_MS;
            context.timerService().registerProcessingTimeTimer(nextTimer);
            nextTimerState.update(nextTimer);
            windowStartState.update(currentTime);
            log.info("📅 Timer initialized for account {}, first report at {}", context.getCurrentKey(), nextTimer);
        }

        // Store alert for the periodic report
        alertsState.add(alert);
        log.debug("📥 Alert {} added to aggregation for account {}", alert.AlertId(), context.getCurrentKey());
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<String, FraudAdvancedAlert, FraudReport>.OnTimerContext ctx,
                        Collector<FraudReport> out) throws Exception {

        Long windowStart = windowStartState.value();
        windowStart = windowStart == null ? timestamp - REPORT_INTERVAL_MS : windowStart;

        // Collect all alerts from the period
        List<FraudAdvancedAlert> periodAlerts = new ArrayList<>();
        List<String> alertIds = new ArrayList<>();
        double totalFraudAmount = 0.0;

        for (FraudAdvancedAlert alert : alertsState.get()) {
            periodAlerts.add(alert);
            alertIds.add(alert.AlertId());
            totalFraudAmount += alert.currentTransaction().amount();
        }

        int alertCount = periodAlerts.size();

        // Only emit report if there were alerts
        if (alertCount > 0) {
            FraudReport report = FraudReport.builder()
                    .reportId(UUID.randomUUID().toString())
                    .reportTimestamp(timestamp)
                    .windowStart(windowStart)
                    .windowEnd(timestamp)
                    .accountId(ctx.getCurrentKey())
                    .totalAlerts(alertCount)
                    .summary(String.format("Account %s: %d fraud alerts in the last minute, total fraud amount: %.2f",
                            ctx.getCurrentKey(), alertCount, totalFraudAmount))
                    .build();

            log.info("📊 FRAUD REPORT for account {}: {} alerts, total fraud amount: {}, alert IDs: {}",
                    ctx.getCurrentKey(), alertCount, totalFraudAmount, alertIds);

            out.collect(report);
        } else {
            log.debug("📊 No alerts for account {} in the last minute, skipping report", ctx.getCurrentKey());
        }

        // Clear alerts for the next period
        alertsState.clear();

        // Update window start and schedule next timer
        windowStartState.update(timestamp);
        long nextTimer = timestamp + REPORT_INTERVAL_MS;
        ctx.timerService().registerProcessingTimeTimer(nextTimer);
        nextTimerState.update(nextTimer);
    }
}

