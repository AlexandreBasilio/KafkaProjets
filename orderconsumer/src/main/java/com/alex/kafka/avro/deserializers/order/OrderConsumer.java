package com.alex.kafka.avro.deserializers.order;

import com.alex.kafka.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class OrderConsumer {

    private static final String BOOTSTRAP_SERVER = "localhost";

    private static Map<TopicPartition, OffsetAndMetadata> currentOffsetProcessed = new HashMap<>();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVER + ":9092");
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("group.id", "OrderGroup");
        props.setProperty("schema.registry.url", "http://1" + BOOTSTRAP_SERVER + "8081");
        props.setProperty("specific.avro.reader", "true");

        // for auto commit
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // default is true (autocommit)
        //props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500"); // default 5000 ms

//        props.setProperty(ConsumerConfig.OFFSE)

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);

        // inner class
        // we will pass the instance of RebalanceHandler to the subscribe methode
        class RebalanceHandler implements ConsumerRebalanceListener {

            // when rebalance triggered and before partitions are being are revoked from this particular consumer
            // so, if there are offset not commited, here it s a place to commit them
            // and so, we can minimize the risks if there is a rebalancing (and a commit is losed)
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(currentOffsetProcessed);
                currentOffsetProcessed.clear();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }


        }

        consumer.subscribe(Collections.singletonList("OrderAvroTopic"), new RebalanceHandler());
        // consumer.subscribe(Collections.singletonList("OrderAvroTopic"));

        try {
            while (true) {
                ConsumerRecords<String, Order> orders = consumer.poll(Duration.ofMillis(5000));
                System.out.println("size=" + orders.isEmpty());

                int count = 0;
                for (ConsumerRecord<String, Order> order : orders) {
                    System.out.println(" product name=" + order.key());
                    System.out.println(" qte name=" + order.value());
                    currentOffsetProcessed.put(new TopicPartition(order.topic(), order.partition()), new OffsetAndMetadata(order.offset() + 1));

                    if (count % 10 == 20) {
                        consumer.commitAsync(currentOffsetProcessed, new OffsetCommitCallback() {
                            @Override
                            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                                if (exception != null) {
                                    System.out.println("log erro - Commit failed for offset:" + offsets);
                                }
                            }
                        });
                    }

                    count++;
                }
                // consumer.commitSync(); // commit entire curent offset. it intelligent. it retry if its possible (unless is unrecoverable exception)

                // wont retry is its a fail
//                consumer.commitAsync(new OffsetCommitCallback() {
//
//                    @Override
//                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
//                        if (exception != null) {
//                            System.out.println("log erro - Commit failed for offset:" + offsets);
//                        }
//                    };
//
//                });
            }
        } finally {
            consumer.close();
        }

    }
}

