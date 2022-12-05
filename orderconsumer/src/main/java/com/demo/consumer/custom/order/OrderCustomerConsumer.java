package com.demo.consumer.custom.order;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderCustomerConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
       // props.setProperty("bootstrap.servers", "172.17.71.169:9092");
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", OrderDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderPartitionedTopic"));

        try {
            while (true) {
                ConsumerRecords<String, Order> orders = consumer.poll(Duration.ofMillis(20000));

                for (ConsumerRecord<String, Order> order : orders) {
                    System.out.println(" Customer name=" + order.key());
                    System.out.println(" prod name=" + order.value().getProduct());
                    System.out.println(" qte =" + order.value().getQuantity());
                    System.out.println(" partition=" + order.partition());
                }
            }
        } finally {
            consumer.close();
        }

    }
}

