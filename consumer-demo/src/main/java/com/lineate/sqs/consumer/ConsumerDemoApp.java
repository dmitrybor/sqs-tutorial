package com.lineate.sqs.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ConsumerDemoApp {
    public static void main(String[] args) {
        SpringApplication.run(ConsumerDemoApp.class, args);
    }
}
