package com.wslimi.streaming.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Fraud alert generated when suspicious activity detected.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FraudAlert implements Serializable {

    private static final long serialVersionUID = 1L;

    private String alertId;
    private String accountId;
    private String transactionId;
    private String reason;
    private Double amount;
    private Long alertTime;

}
