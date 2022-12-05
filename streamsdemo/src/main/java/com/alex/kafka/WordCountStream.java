package com.alex.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class WordCountStream {

    public static void main (String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-streams-dataflow");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // required for the kTable. KTable maintain a state, and it will take some time before the context are stremmed
                                                                     // default is 1 byte. we set to 0 in dev to test.

        // create topology
        StreamsBuilder builder = new StreamsBuilder(); // topology builder

//        ValueMapper vm = new ValueMapper() {
//            @Override
//            public Object apply(Object value) {
//                return value;
//            }
//        };
//        vm.apply(stream.toString());
//
//        String value = stream.toString();
//        List<String> strings = Arrays.asList(value.toLowerCase().split(" "));
//        KStream<String, String> stringStringKStream2 = stream.flatMapValues(value1 ->{
//            System.out.println(value1);
//            return null;
//        });

        KStream<String, String> stream = builder.stream("streams-wordcount-input");
        System.out.println("stream=" + stream);
        KStream<String, String> kStream = stream.flatMapValues(valor -> Arrays.asList(valor.toLowerCase().split(" ")));
        KGroupedStream<String, String> groupStream = kStream.groupBy((key, value) -> value);
        KTable<String, Long> countsTable = groupStream.count();
        countsTable.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();
        System.out.println("topology:" + topology.describe());


        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        // acces the current process (this program runtime) and add a shutdown hook to it
        // when the program ends we close the stream
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

