package com.wslimi.demo.fraud.model;

import java.io.Serializable;


public record Transaction(
        String transactionId,
        String srcAccountId,
        String destAccountId,
        Double amount,
        String currency,
        Long eventTime

) implements Serializable {
}
