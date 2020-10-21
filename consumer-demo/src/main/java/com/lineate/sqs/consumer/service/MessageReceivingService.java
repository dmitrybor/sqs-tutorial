package com.lineate.sqs.consumer.service;

import com.lineate.sqs.consumer.configuration.AwsSqsProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import javax.annotation.PostConstruct;

@Service
public class MessageReceivingService {

    private final static Logger LOGGER = LoggerFactory.getLogger(MessageReceivingService.class);

    private SqsClient sqsClient;
    private AwsSqsProperties awsSqsProperties;
    private String queueUrl;

    @Autowired
    public MessageReceivingService(SqsClient sqsClient, AwsSqsProperties awsSqsProperties) {
        this.sqsClient = sqsClient;
        this.awsSqsProperties = awsSqsProperties;
    }

    @PostConstruct
    public void initQueueUrl() {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(awsSqsProperties.getQueueName())
                .build();
        try {
            LOGGER.info("Checking that queue named {} exists", awsSqsProperties.getQueueName());
            GetQueueUrlResponse getQueueUrlResponse = sqsClient.getQueueUrl(getQueueUrlRequest);
            queueUrl = getQueueUrlResponse.queueUrl();
            LOGGER.info("Queue found, url: {}", queueUrl);
        } catch (QueueDoesNotExistException e) {
            LOGGER.info("Queue does not exist.");
            queueUrl = null;
        }
    }

    @Scheduled(initialDelay = 2000L, fixedDelay = 5000L)
    public void receiveMessagesFromQueue() {
        if (queueUrl == null) {
            LOGGER.info("Queue url is not initialized. Trying to initialize.");
            initQueueUrl();
            if (queueUrl == null) {
                return;
            }
        }
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(3)
                .waitTimeSeconds(5)
                .build();
        try {
            ReceiveMessageResponse receiveMessageResponse = sqsClient.receiveMessage(receiveMessageRequest);
            receiveMessageResponse.messages()
                    .forEach(message -> {
                        LOGGER.info("Message received: {}", message.body());
                        LOGGER.info("Deleting message with receipt handle {}", message.receiptHandle());
                        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                                .queueUrl(queueUrl)
                                .receiptHandle(message.receiptHandle())
                                .build();
                        sqsClient.deleteMessage(deleteMessageRequest);
                    });
        } catch (SdkException e) {
            LOGGER.warn("Exception was thrown while receiving messages from SQS", e);
        }
    }
}
