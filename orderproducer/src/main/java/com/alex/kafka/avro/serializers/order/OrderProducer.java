package com.alex.kafka.avro.serializers.order;

import com.alex.kafka.avro.Order;
import com.demo.producer.custom.order.OrderCallBack;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class OrderProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "172.17.71.169:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://172.17.71.169:8081");

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        Order order = new Order("Mary", "Sony vaiooaayyyyyyyyyyyao", 25);
        ProducerRecord<String, Order> record = new ProducerRecord<String, Order>("OrderAvroTopic", order.getCustomerName().toString(), order);

        // ASyncro calls. we not wait for the response. When its ready(the response is there, the OrderCallBack classes is called
        try {
           // producer.send(record, new OrderCallBack());
            producer.send(record, new OrderCallBack());
            System.out.println("record sent=" + record.toString());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }
}

