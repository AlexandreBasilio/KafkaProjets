package com.alex.kafka.avro.serializers.truck;

import com.alex.kafka.avro.TruckCoordinates;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class TruckCoordinatesProducer {

    public static void main (String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "172.17.71.169:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://172.17.71.169:8081");

        KafkaProducer<String, TruckCoordinates> producer = new KafkaProducer<>(props);
        var truckCoordinates =  TruckCoordinates.newBuilder()
                .setId("1")
                .setLatitude(100.20)
                .setLongitude(20.36).build();
        ProducerRecord<String, TruckCoordinates> record = new ProducerRecord<>("truchTopic", truckCoordinates.getId().toString(), truckCoordinates);

        try {
            producer.send(record);
            System.out.println("gravado=" + record.toString());
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
