package com.lineate.sqs.producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ProducerDemoApp {

    public static void main(String[] args) {
        SpringApplication.run(ProducerDemoApp.class, args);
    }
}
