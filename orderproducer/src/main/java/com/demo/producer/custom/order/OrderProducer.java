package com.demo.producer.custom.order;

import com.demo.producer.custom.order.OrderCallBack;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class OrderProducer {

    public static void main(String[] args) {
        // main properties required
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        // 0 : producer send the msg et consider the msg is sent successfully, event if it was a error (dont wait for response from broker). msg could be lost.
        // 1 : producer receivers a msg from broker if the leader consummer received the msg . Plus safe
        // all : producer receives ack from the broker only once ALL replicas received the msg (more latency)
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all" ); // 0, 1 or all : how many partitions should receive the message before the prodcucer send or write successufully
        // buffer used by producer to bufer the messages before they are sent broker. Default 256MB
        // if buffer is not big enough the producer can block, if there are too many mnsg produced
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "343434/");
        // by default the msg is not compressed. values:
        // snappy -  from google (does a good job in utilising CPU. if yoou are ok on CPU utilisation)
        // gzip - from google (copression ratio higher. will save a lot of network bandwitdh). it s better than snappy
                //- if you want less CPU to be used
        // lz4 -
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        // retry if there some errors (ex: leader down).
        // producer wait 100 milli seconds before each retry
        // 0: never retry
        // 1: rerty once , 2: retry twice
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "2"); // only retry if the error is recoverable
        props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000"); // wait 1s before each retry. 100 it s too high)

        // memmory allocated for batch in bytes. A higher number is good. DEfault 16kb
        // mem you use a hig number, the producer dont wait for this batch memory to be filled in the msgs
        // the producer will hand over the msg as soon as the sender is available
        // but set a small size can be a problem because the batch will be very small (decreasing the throughput)
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1024");

        //  complements the batch size
        //  mille second value (producer will wait cet time before it hands over the msgs to the sender thread when it is available)
        // even it available we can add more msg to the batch before send to broker
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "200");

        // in millseconds : time producer wait for a response from the broker.
        // if there  timeout.. the producer will retry
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,"200");

        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // default is false


        KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic", "MacBookPraaao", 10);

        // Syncro calls : we wait for the responses
//        try {
//            RecordMetadata recordMetadata = producer.send(record).get();
//            System.out.println("Partition=" + recordMetadata.partition());
//            System.out.println("OffSet=" + recordMetadata.offset());
//            System.out.println("message sent");
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            producer.close();
//        }

        // ASyncro calls. we not wait for the response. When its ready(the response is there, the OrderCallBack classes is called
        try {
            producer.send(record, new OrderCallBack());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }
}

