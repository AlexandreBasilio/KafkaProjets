package com.alex.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class DataFlowStreamPractice {

    public static void main(String [] args) {

        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "practice-streams-dataflow");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("streams-dataflow-practice-input");
        stream.filter((key, value) -> (Integer.parseInt(value) % 2 == 0))
                 .mapValues(value -> String.valueOf(Math.multiplyExact(Integer.parseInt(value),3)))
                 .to("streams-dataflow-practice-output");
        Topology topology = builder.build();
        System.out.println("topology:" + topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    // testar depois
//    public static void main(String[] args) {
//        Properties props = new Properties();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-triple");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
//
//        StreamsBuilder builder = new StreamsBuilder();
//        KStream<String, Integer> stream = builder.stream("streams-triple-input");
//        stream.foreach((key, value) -> System.out.println("KEY, VAL: " + key + ", " + value));
//
//        stream.filter((key, value) -> {
//            try {
//                return value % 2 == 1;
//            } catch (Exception e) {
//                return false;
//            }
//        })
//                .mapValues(value -> value*3)
//                .to("streams-triple-output");
//
//        Topology topology = builder.build();
//
//        KafkaStreams streams = new KafkaStreams(topology, props);
//        streams.start();
//
//        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
//    }
}
