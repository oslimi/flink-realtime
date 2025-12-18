package com.wslimi.demo.fraud.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wslimi.demo.fraud.model.FraudNaiveAlert;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Serialization schema for FraudNaiveAlert to JSON bytes.
 */
public class FraudNaiveAlertSerializationSchema implements SerializationSchema<FraudNaiveAlert> {

    private final ObjectMapper objectMapper;

    public FraudNaiveAlertSerializationSchema(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(FraudNaiveAlert fraudAlert) {
        try {
            return objectMapper.writeValueAsBytes(fraudAlert);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize FraudNaiveAlert", e);
        }
    }
}

