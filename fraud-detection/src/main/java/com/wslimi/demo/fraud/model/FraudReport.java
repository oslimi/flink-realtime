package com.wslimi.demo.fraud.model;

import lombok.Builder;

import java.io.Serializable;
import java.util.List;

/**
 * Periodic fraud report containing all alerts generated during a time window.
 */
@Builder
public record FraudReport(
        String reportId,
        Long reportTimestamp,
        long windowStart,
        long windowEnd,
        String accountId,
        int totalAlerts,
        double totalFraudAmount,
        List<String> alertIds,
        String summary
) implements Serializable {
}

