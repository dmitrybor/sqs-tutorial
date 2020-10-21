package com.lineate.sqs.consumer.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsClient;

@Configuration
public class AwsSqsConfiguration {

    @Bean
    public SqsClient getSqsClient(AwsSqsProperties properties) {

        AwsCredentials credentials = AwsBasicCredentials.create(properties.getAccessKey(),
                properties.getSecretKey());

        return SqsClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .region(properties.getRegion())
                .build();
    }

}
