package com.wslimi.demo.fraud.model;

import java.io.Serializable;

public record FraudNaiveAlert(
        String AlertId,
        Long timestamp,
        Transaction transaction,
        String comment

) implements Serializable {
}
