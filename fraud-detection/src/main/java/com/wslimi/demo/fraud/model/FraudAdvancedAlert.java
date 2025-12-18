package com.wslimi.demo.fraud.model;

import lombok.Builder;

import java.io.Serializable;
import java.util.Map;

@Builder
public record FraudAdvancedAlert(
        String AlertId,
        Long timestamp,
        Transaction previousTransaction,
        Transaction currentTransaction,
        String comment

) implements Serializable {
}
