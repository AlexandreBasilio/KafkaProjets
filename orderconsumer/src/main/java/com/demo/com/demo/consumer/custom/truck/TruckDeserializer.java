package com.demo.com.demo.consumer.custom.truck;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

public class TruckDeserializer implements Deserializer<TruckCoordinates> {

    @Override
    public TruckCoordinates deserialize(String topic, byte[] data) {
        ObjectMapper objectMapper = new ObjectMapper();
        TruckCoordinates truckCoordinates = null;
        try {
            truckCoordinates = objectMapper.readValue(data, TruckCoordinates.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return truckCoordinates;
    }
}
