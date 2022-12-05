package com.demo.producer.custom.order;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class OrderCallBack implements Callback {

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println("Partition...=" + recordMetadata.partition());
        System.out.println("OffSet=" + recordMetadata.offset());
        System.out.println("message sent");

        if (e!=null) {
            e.printStackTrace();
        }
    }
}
