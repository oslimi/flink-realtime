package com.wslimi.demo.fraud.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wslimi.demo.fraud.model.FraudReport;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Serialization schema for FraudReport to JSON bytes.
 */
public class FraudReportSerializationSchema implements SerializationSchema<FraudReport> {

    private final ObjectMapper objectMapper;

    public FraudReportSerializationSchema(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(FraudReport report) {
        try {
            return objectMapper.writeValueAsBytes(report);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize FraudReport", e);
        }
    }
}

