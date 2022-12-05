package com.alex.kafka.avro.serializers.order;

import com.demo.producer.custom.order.OrderCallBack;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class GenericOrderProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "172.17.71.169:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://172.17.71.169:8081");

        //Generic record
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "  \"namespace\" : \"com.alex.kafka.avro\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"Order\",\n" +
                "  \"fields\" : [\n" +
                "  {\"name\": \"customerName\",\"type\":\"string\"},\n" +
                "  {\"name\": \"product\", \"type\":\"string\"},\n" +
                "  {\"name\": \"quantity\", \"type\":\"int\"}\n" +
                "  ]\n" +
                "}");

        GenericData.Record order = new GenericData.Record(schema);
        order.put("customerName", "Joao");
        order.put("product", "nissan rogue");
        order.put("quantity", 200);

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<String, GenericRecord>("OrderAvroGRTopic", order.get("customerName").toString(), order);

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

