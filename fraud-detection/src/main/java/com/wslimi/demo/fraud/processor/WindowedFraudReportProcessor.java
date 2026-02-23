package com.wslimi.demo.fraud.processor;

import com.wslimi.demo.fraud.model.FraudAdvancedAlert;
import com.wslimi.demo.fraud.model.FraudReport;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.UUID;

/**
 * Windowed Fraud Report Processor
 *
 * Aggregates fraud alerts using Flink's window abstraction.
 * This approach uses tumbling windows instead of timer-based reporting.
 *
 * Key differences from Timer-based approach:
 * - Window boundaries are defined upfront (5 seconds)
 * - Elements are grouped by key and window
 * - onWindow() fires when window closes (based on watermark)
 * - Better integration with late data handling
 * - Easier to reason about semantics
 */
@Slf4j
public class WindowedFraudReportProcessor
        extends ProcessWindowFunction<FraudAdvancedAlert, FraudReport, String, TimeWindow> {

    @Override
    public void process(String accountId,
                        ProcessWindowFunction<FraudAdvancedAlert, FraudReport, String, TimeWindow>.Context ctx,
                        Iterable<FraudAdvancedAlert> alerts,
                        Collector<FraudReport> out) throws Exception {

        int alertCount = 0;
        double totalFraudAmount = 0.0;
        StringBuilder alertIdsBuilder = new StringBuilder();
        long firstAlertTime = Long.MAX_VALUE;

        for (FraudAdvancedAlert alert : alerts) {
            alertCount++;
            totalFraudAmount += alert.currentTransaction().amount();
            firstAlertTime = Math.min(firstAlertTime, alert.timestamp());

            if (alertIdsBuilder.length() > 0) {
                alertIdsBuilder.append(",");
            }
            alertIdsBuilder.append(alert.AlertId());
        }

        if (alertCount > 0) {
            TimeWindow window = ctx.window();
            long windowStart = window.getStart();
            long windowEnd = window.getEnd();

            FraudReport report = FraudReport.builder()
                    .reportId(UUID.randomUUID().toString())
                    .reportTimestamp(System.currentTimeMillis())
                    .windowStart(windowStart)
                    .windowEnd(windowEnd)
                    .accountId(accountId)
                    .totalAlerts(alertCount)
                    .totalFraudAmount(totalFraudAmount)
                    .alertIds(alertIdsBuilder.toString())
                    .summary(String.format(
                            "Account %s: %d fraud alerts in window [%d - %d], total fraud amount: %.2f EUR",
                            accountId, alertCount, windowStart, windowEnd, totalFraudAmount))
                    .build();

            log.info("📊 Window Report Generated");
            log.info("   Account: {}", accountId);
            log.info("   Window: [{} - {}]", windowStart, windowEnd);
            log.info("   Alerts: {}", alertCount);
            log.info("   Total Amount: {:.2f} EUR", totalFraudAmount);
            log.info("   Alert IDs: {}", alertIdsBuilder.toString());

            out.collect(report);
        } else {
            log.debug("📊 No alerts for account {} in window [{} - {}]",
                    accountId, ctx.window().getStart(), ctx.window().getEnd());
        }
    }
}

