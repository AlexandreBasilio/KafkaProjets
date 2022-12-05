package com.alex.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class DataFlowStream {

    public static void main (String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-dataflow");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // create topology
        StreamsBuilder builder = new StreamsBuilder(); // topology builder
        KStream<String, String> stream = builder.stream("streams-dataflow-input");
        stream.foreach((key, value) -> System.out.println("Key and Value " +  key + " " + value));
        KStream<String, String> filterStream= stream.filter((key, value) -> value.contains("token"))
                //.mapValues(value-> value.toUpperCase())
                //.map((key, value) -> new KeyValue<>(key, value.toUpperCase()));
                .map((key, value) -> KeyValue.pair(key, value.toUpperCase()));
        filterStream.to("streams-dataflow-output");
        Topology topology = builder.build();
        System.out.println("topology:" + topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        // acces the current process (this program runtime) and add a shutdown hook to it
        // when the program ends we close the stream
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

