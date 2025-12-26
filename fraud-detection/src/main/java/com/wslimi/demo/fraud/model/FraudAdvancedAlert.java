package com.wslimi.demo.fraud.model;

import lombok.Builder;

import java.io.Serializable;

@Builder
public record FraudAdvancedAlert(
        String AlertId,
        Long timestamp,
        Transaction previousTransaction,
        Transaction currentTransaction,
        String comment,
        Long processingDuration

) implements Serializable {
}
