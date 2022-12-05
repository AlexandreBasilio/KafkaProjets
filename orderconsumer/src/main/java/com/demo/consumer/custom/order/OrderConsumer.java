package com.demo.consumer.custom.order;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OrderGroup");  // for consumer group

        props.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1024");  // tells broker to wait until it has so much data to be sent to the consumer DEfault 1MB.
                                                                           // higher nuymber is better. to minimize the exchange of data entre broker and consumer
        props.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "200"); // default 500 ms. time broker wait before send data to consumer


        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000"); // default. every time the consumer must send a heartbeat to the coordinator
                                                                                // HEARTBEATR must 1/3 of SESSION TIMEOUT
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3000"); // tells the broker how long consumer can go without send a heartbeat info
                                                                             // if after 3s the consumer dont send a heartbeat, its considered DEAD.
                                                                             // Coordinattor trigger a REBALANCE

        // default: 1MB
        // max number of bytes the broker returns to consumer
        // ex: 1MB. 30 partitions and 5 consumers, then each consumer gets 6 partitions
        // so we have to ensure each consumer has at LEAST 6MB space. But normally we put more, because consumers can dead. We give 12MB
        props.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1MB");

        // controls the consumer behavior if it starts reading a partition that does not have a COMMITED Offset
        // latest: read the lastest records from partition (so, after consumer starts)
        // earliest : read from the beginning of the partition. il willprocess all the records
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // can be any unique string value. Used by broker to logmetrics and quota allocations purposes
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "OrderConsumer" );

        // max number of records the pool method can return. Control the amount of dat the application will nedd to process
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "400" );

        // here we configure a class
        // the partitioner assigner is reponsible for assigning partitions to consumers
        // There is 2 assigners:
        //   RangeAssignor.class  (default)
        //   RoundRobinAssignor.class
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderTopic"));
        ConsumerRecords<String, Integer> orders = consumer.poll(Duration.ofMillis(20000));

        for (ConsumerRecord<String, Integer> order : orders) {
            System.out.println(" product name=" + order.key());
            System.out.println(" qte name=" + order.value());
        }

        consumer.close();

    }
}

