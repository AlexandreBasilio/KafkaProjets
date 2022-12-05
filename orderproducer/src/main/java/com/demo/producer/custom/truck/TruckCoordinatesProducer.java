package com.demo.producer.custom.truck;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TruckCoordinatesProducer {

    public static void main (String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", TruckSerializer.class.getName());
        props.setProperty("partitioner.class", TruckVIPPartitioner.class.getName());

        KafkaProducer<String, TruckCoordinates> producer = new KafkaProducer<>(props);
        var truckCoordinates =  new TruckCoordinates("12", 100.20, 20.36);
        ProducerRecord<String, TruckCoordinates> record = new ProducerRecord<>("TruckPartitionedTopic", truckCoordinates.getId().toString(), truckCoordinates);

        try {
            producer.send(record);
            System.out.println("gravado....=" + record.toString());
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
