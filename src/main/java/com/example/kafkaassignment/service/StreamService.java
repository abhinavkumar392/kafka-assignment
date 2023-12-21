package com.example.kafkaassignment.service;

import com.example.kafkaassignment.constants.AppConstants;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class StreamService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamService.class);

    public void kstream(){

        try {
            StreamsBuilder builder = new StreamsBuilder();
            KStream<String,String> stream = builder.stream(AppConstants.AuthTopic, Consumed.with(Serdes.String(),Serdes.String()));

            LOGGER.info("stream start");

            stream
                    .filter((key, value) -> value.contains("\"SUB\":"))
                    .mapValues(value -> substrx(value, "\"SUB\":"))
                    .to(AppConstants.SubscriberTopic);

            stream
                    .filter((key, value) -> value.contains("\"CAS\":"))
                    .mapValues(value -> substrx(value, "\"CAS\":"))
                    .to(AppConstants.CaseTopic);

            stream
                    .filter((key, value) -> value.contains("\"SVC\":"))
                    .mapValues(value -> substrx(value, "\"SVC\":"))
                    .to(AppConstants.ServiceTopic);

            stream
                    .filter((key, value) -> value.contains("\"PAT\":"))
                    .mapValues(value -> substrx(value, "\"PAT\":"))
                    .to(AppConstants.PatientTopic);

            LOGGER.info("stream end");

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

