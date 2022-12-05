package com.alex.kafka.avro.deserializers.order;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class GenericOrderConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "172.17.71.169:9092");
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");
        props.setProperty("schema.registry.url", "http://172.17.71.169:8081");

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderAvroGRTopic"));

        ConsumerRecords<String, GenericRecord> orders = consumer.poll(Duration.ofMillis(5000));
        System.out.println("size=" + orders.isEmpty());

        for (ConsumerRecord<String, GenericRecord> order : orders) {
            System.out.println(" product name=" + order.key());
            System.out.println(" prod name=" + order.value().get("product"));
            System.out.println(" quant name=" + order.value().get("quantity"));
        }

        consumer.close();

    }
}

