package com.wslimi.demo.fraud.model;

import lombok.Builder;

import java.io.Serializable;

@Builder
public record FraudReport(
        String reportId,
        Long reportTimestamp,
        long windowStart,
        long windowEnd,
        String accountId,
        int totalAlerts,
        double totalFraudAmount,
        String alertIds,
        String summary
) implements Serializable {
}

