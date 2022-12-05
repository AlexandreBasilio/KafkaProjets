package com.alex.kafka.avro.deserializers.truck;

import com.alex.kafka.avro.TruckCoordinates;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TruckCoordinatesConsumer {

    public static void main (String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "172.17.71.169:9092");
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("group.id", "TruckGroup");
        props.setProperty("schema.registry.url", "http://172.17.71.169:8081");
        props.setProperty("specific.avro.reader", "true");

        KafkaConsumer<String, TruckCoordinates> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("truchTopic"));

        try {
            while (true) {
                ConsumerRecords<String, TruckCoordinates> coordinates = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, TruckCoordinates> coord : coordinates) {
                    System.out.println(" id-key="+ coord.key());
                    System.out.println(" id="+ coord.value().getId());
                    System.out.println(" lat="+ coord.value().getLatitude());
                    System.out.println(" long="+ coord.value().getLongitude());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
