package com.lineate.sqs.producer.service;

import com.lineate.sqs.producer.configuration.AwsSqsProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import javax.annotation.PostConstruct;

@Service
public class MessageSendingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageSendingService.class);
    private SqsClient sqsClient;
    private AwsSqsProperties awsSqsProperties;
    private String queueUrl;
    private int messageCount = 0;

    @Autowired
    public MessageSendingService(SqsClient sqsClient, AwsSqsProperties awsSqsProperties) {
        this.sqsClient = sqsClient;
        this.awsSqsProperties = awsSqsProperties;
    }

    @PostConstruct
    public void initializeQueue() {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(awsSqsProperties.getQueueName())
                .build();
        try {
            LOGGER.info("Checking that queue named {} exists", awsSqsProperties.getQueueName());
            GetQueueUrlResponse getQueueUrlResponse = sqsClient.getQueueUrl(getQueueUrlRequest);
            queueUrl = getQueueUrlResponse.queueUrl();
            LOGGER.info("Queue found, url: {}", queueUrl);
        } catch (QueueDoesNotExistException e) {
            LOGGER.info("Queue does not exist. Creating.");
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(awsSqsProperties.getQueueName())
                    .build();
            CreateQueueResponse createQueueResponse = sqsClient.createQueue(createQueueRequest);
            queueUrl = createQueueResponse.queueUrl();
            LOGGER.info("Queue created, url: {}", queueUrl);
        }
    }

    @Scheduled(initialDelay = 2000L, fixedDelay = 5000L)
    public void sendMessageToQueue() {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody("Message " + messageCount)
                .build();
        LOGGER.info("Sending message {}", sendMessageRequest.messageBody());
        try {
            sqsClient.sendMessage(sendMessageRequest);
            messageCount++;
        } catch (SdkException e) {
            LOGGER.warn("Exception was thrown while sending message to SQS", e);
        }
    }
}
