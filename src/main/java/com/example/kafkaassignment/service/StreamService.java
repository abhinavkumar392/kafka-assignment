package com.example.kafkaassignment.service;

import com.example.kafkaassignment.constants.AppConstants;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
//@EnableKafkaStreams
public class StreamService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamService.class);

    public void kstream(){

        try {
            StreamsBuilder builder = new StreamsBuilder();
            Properties properties = new Properties();
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "json-processor");
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

            KStream<String, String> stream = builder.stream("Auth-Topic");

            LOGGER.info("stream start");

            stream
                    .filter((key, value) -> value.contains("\" SUB\":"))
                    .mapValues(value -> substrx(value, "\"SUB\":"))
                    .to(AppConstants.SubscriberTopic,Produced.with(Serdes.String(),Serdes.String()));

            stream
                    .filter((key, value) -> value.contains("\"CAS\":"))
                    .mapValues(value -> substrx(value, "\"CAS\":"))
                    .to(AppConstants.CaseTopic,Produced.with(Serdes.String(),Serdes.String()));

            stream
                    .filter((key, value) -> value.contains("\"SVC\":"))
                    .mapValues(value -> substrx(value, "\"SVC\":"))
                    .to(AppConstants.ServiceTopic,Produced.with(Serdes.String(),Serdes.String()));

            stream
                    .filter((key, value) -> value.contains("\"PAT\":"))
                    .mapValues(value -> substrx(value, "\"PAT\":"))
                    .to(AppConstants.PatientTopic,Produced.with(Serdes.String(),Serdes.String()));

            Topology topo = builder.build();
            KafkaStreams streams = new KafkaStreams(topo, properties);
            streams.start();

        } catch (Exception e) {
            LOGGER.error("Runtime Exception",e);
            throw new RuntimeException(e);
        }
    }

    private String substrx(String value, String key) {
        int start = value.indexOf(key) + key.length();
        int end = value.indexOf("}", start) + 1;
        return value.substring(start, end).trim();
    }
}

