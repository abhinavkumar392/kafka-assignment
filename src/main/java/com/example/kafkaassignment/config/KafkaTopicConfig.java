package com.example.kafkaassignment.config;

import com.example.kafkaassignment.constants.AppConstants;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    @Bean
    public NewTopic kafkaAssignmentTopic() {
        return TopicBuilder.name("Auth-Topic").build();
    }

    @Bean
    public NewTopic serviceTopic() {
        return TopicBuilder.name(AppConstants.ServiceTopic).build();
    }

    @Bean
    public NewTopic subscriberTopic() {
        return TopicBuilder.name(AppConstants.SubscriberTopic).build();
    }

    @Bean
    public NewTopic patientTopic() {return TopicBuilder.name(AppConstants.PatientTopic).build();}

    @Bean
    public NewTopic caseInfoTopic() {
        return TopicBuilder.name(AppConstants.CaseTopic).build();
    }
}
