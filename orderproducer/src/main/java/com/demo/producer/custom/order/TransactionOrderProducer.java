package com.demo.producer.custom.order;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class TransactionOrderProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-producer-1");
        // the send method can not block than 1 second
        // also coomitTransactions, aborttransctions are affected by this property
       // props.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000");

        KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        ProducerRecord<String, Integer> record  = new ProducerRecord<>("OrderTopic", "MacBookPraaao", 10);
        ProducerRecord<String, Integer> record2 = new ProducerRecord<>("OrderTopic", "DELL BookPraaao", 20);
        ProducerRecord<String, Integer> record3 = new ProducerRecord<>("OrderTopic", "SONYBookPraaao", 30);

        try {
            producer.beginTransaction();
            producer.send(record); // you dont need the orderCallback because it already handle with Transactions
            producer.send(record2);
            producer.send(record3);
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }
}

