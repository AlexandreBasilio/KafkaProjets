package com.demo.producer.custom.order;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Order {

    private String customerName;
    private String product;
    private int quantity;
}
