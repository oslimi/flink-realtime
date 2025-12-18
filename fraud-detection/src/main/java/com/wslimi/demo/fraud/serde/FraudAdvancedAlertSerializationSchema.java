package com.wslimi.demo.fraud.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wslimi.demo.fraud.model.FraudAdvancedAlert;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Serialization schema for FraudAdvancedAlert to JSON bytes.
 */
public class FraudAdvancedAlertSerializationSchema implements SerializationSchema<FraudAdvancedAlert> {

    private final ObjectMapper objectMapper;

    public FraudAdvancedAlertSerializationSchema(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(FraudAdvancedAlert fraudAlert) {
        try {
            return objectMapper.writeValueAsBytes(fraudAlert);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize FraudAdvancedAlert", e);
        }
    }
}

