package com.lineate.sqs.demo;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class SqsQueueMessageDemo {

    public static void main(String[] args) {
        final String queueName = "test_queue";

        AwsCredentials credentials = AwsBasicCredentials.create("AKIAWOPQUQF4GSTIBQQV",
                "howgLE0oeUL2iGnM1aWmZqLtnZWAeOzfb8x5pCjQ");

        SqsClient client = SqsClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .region(Region.EU_NORTH_1)
                .build();

        String queueUrl = createQueue(client, queueName);

//        String sentMessageBody = "Hello world!";
//        System.out.println("Sending message: " + sentMessageBody);
//        sendMessage(client, queueUrl, sentMessageBody);
//
//        Optional<Message> receivedMessage = receiveMessage(client, queueUrl);
//        receivedMessage.ifPresentOrElse(
//                message -> {
//                    System.out.println("Message received: " + message.body());
//                    System.out.println("Deleting processed message with receipt handle " + message.receiptHandle());
//                    deleteMessage(client, queueUrl, message.receiptHandle());
//                },
//                () -> System.out.println("No messages have been received from the queue.")
//        );


//        System.out.println("---------------------------------");
//        List<String> messageBodies = new ArrayList<>();
//        for (int i = 0; i < 5; i++) {
//            messageBodies.add("Message " + i);
//        }
//        System.out.println("Sending messages in a batch: ");
//        messageBodies.forEach(messageBody -> System.out.println("     " + messageBody));
//        sendMultipleMessages(client, queueUrl, messageBodies);

        System.out.println("Receiving multiple messages.");
        List<Message> messages = receiveMultipleMessages(client, queueUrl);
        messages.forEach(message -> System.out.println("    " + message.body()));

        System.out.println("Deleting processed messages.");
        messages.forEach(message -> {
            System.out.println("Deleting message with receipt handle " + message.receiptHandle());
            deleteMessage(client, queueUrl, message.receiptHandle());
        });

    }

    private static String createQueue(SqsClient client, String queueName) {

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

        CreateQueueResponse createQueueResponse = client.createQueue(createQueueRequest);
        return createQueueResponse.queueUrl();
    }

    private static String sendMessage(SqsClient client, String queueUrl, String messageBody) {
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();

        SendMessageResponse sendMessageResponse = client.sendMessage(sendMessageRequest);
        return sendMessageResponse.messageId();
    }

    private static Optional<Message> receiveMessage(SqsClient client, String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .waitTimeSeconds(10)
                .maxNumberOfMessages(1)
                .build();

        ReceiveMessageResponse receiveMessageResponse = client.receiveMessage(receiveMessageRequest);
        return receiveMessageResponse.messages().stream().findFirst();
    }

    private static void deleteMessage(SqsClient client, String queueUrl, String messageReceiptHandle) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(messageReceiptHandle)
                .build();

        client.deleteMessage(deleteMessageRequest);
    }

    private static void sendMultipleMessages(SqsClient client, String queueUrl, List<String> messageBodies) {

        List<SendMessageBatchRequestEntry> batchEntries = messageBodies.stream()
                .map(messageBody ->
                        SendMessageBatchRequestEntry.builder()
                                .id(UUID.randomUUID().toString())
                                .messageBody(messageBody).build())
                .collect(Collectors.toList());

        SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(batchEntries)
                .build();

        client.sendMessageBatch(sendMessageBatchRequest);
    }

    private static List<Message> receiveMultipleMessages(SqsClient client, String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(10)
                .build();

        ReceiveMessageResponse receiveMessageResponse = client.receiveMessage(receiveMessageRequest);
        return receiveMessageResponse.messages();
    }
}
