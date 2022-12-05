package com.demo.com.demo.consumer.custom.truck;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TruckCoordinatesConsumer {

    public static void main (String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", TruckDeserializer.class.getName());
        props.setProperty("group.id", "TruckGroup");

        KafkaConsumer<String, TruckCoordinates> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("TruckPartitionedTopic"));

        try {
            while (true) {
                ConsumerRecords<String, TruckCoordinates> coordinates = consumer.poll(Duration.ofMillis(6000));
                for (ConsumerRecord<String, TruckCoordinates> coord : coordinates) {
                    System.out.println(" id-key="+ coord.key());
                    System.out.println(" id="+ coord.value().getId());
                    System.out.println(" lat="+ coord.value().getLatitude());
                    System.out.println(" long="+ coord.value().getLongitude());
                    System.out.println(" partition=" + coord.partition());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
