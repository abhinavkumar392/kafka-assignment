package com.example.kafkaassignment.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class KafkaProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);
    private final KafkaTemplate<String, Map<String, Map<String, String>>> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, Map<String, Map<String, String>>> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(Map<String, Map<String, String>> message) {

        try {
            LOGGER.info(String.format("Message sent %s",message));
            kafkaTemplate.send("Auth-Topic", message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
