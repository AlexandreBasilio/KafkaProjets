package com.demo.producer.custom.order;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class OrderCustomerProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        //props.setProperty("bootstrap.servers", "172.17.71.169:9092");
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "com.demo.producer.custom.OrderSerializer");
        props.setProperty("partitioner.class", VIPPartitioner.class.getName());

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        Order order = new Order("Maryu", "Sony vaio", 20);
        ProducerRecord<String, Order> record = new ProducerRecord<String, Order>("OrderPartitionedTopic", order.getCustomerName(), order);

        // ASyncro calls. we not wait for the response. When its ready(the response is there, the OrderCallBack classes is called
        try {
            //producer.send(record, new OrderCallBack());
            producer.send(record);
            System.out.println("gertado=" + record);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }
}

