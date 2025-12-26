package com.wslimi.demo.fraud.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.wslimi.demo.fraud.model.Transaction;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Serialization schema for Transaction objects to JSON
 */
public class TransactionSerializationSchema implements SerializationSchema<Transaction> {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    @Override
    public byte[] serialize(Transaction transaction) {
        try {
            return objectMapper.writeValueAsBytes(transaction);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Transaction", e);
        }
    }
}

