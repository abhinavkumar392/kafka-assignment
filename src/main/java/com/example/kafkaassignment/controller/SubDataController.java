package com.example.kafkaassignment.controller;

import com.example.kafkaassignment.service.KafkaProducer;
import com.example.kafkaassignment.service.TextToJson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@RequestMapping("/api/v1/kafka")
public class SubDataController {

    private KafkaProducer kafkaProducer;

    @Autowired
    private TextToJson txtToJson;
    @GetMapping("/publish")
    public ResponseEntity<String> publish() {
        try {
            txtToJson.transformTxtToJson();
            return ResponseEntity.ok("Txt file converted to JSON");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
