package com.demo.producer.custom.truck;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class TruckSerializer implements Serializer<TruckCoordinates> {
    @Override
    public byte[] serialize(String topic, TruckCoordinates data) {
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] bytes = new byte[0];
        try {
            bytes = objectMapper.writeValueAsString(data).getBytes();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return bytes;
    }
}
