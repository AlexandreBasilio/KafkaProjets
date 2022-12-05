package com.demo.com.demo.consumer.custom.truck;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TruckCoordinates {

    private String id;
    private Double latitude;
    private Double longitude;
}
