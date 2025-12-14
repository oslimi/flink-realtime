package com.wslimi.streaming.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Financial transaction event.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction implements Serializable {

    private static final long serialVersionUID = 1L;

    private String transactionId;
    private String accountId;
    private Double amount;
    private String currency;
    private String country;
    private Long eventTime;

}
