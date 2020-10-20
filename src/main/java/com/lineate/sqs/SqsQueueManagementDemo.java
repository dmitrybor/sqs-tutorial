package com.lineate.sqs;

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;

import java.util.List;

public class SqsQueueManagementDemo {
    public static void main(String[] args) {
        SqsClient sqsClient = SqsClient.builder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .region(Region.EU_NORTH_1)
                .build();

        final String queueName = "test_queue_12345";

        System.out.println("Creating queue named " + queueName);
        String queueUrl = createQueue(sqsClient, queueName);
        System.out.println("Created queue URL: " + queueUrl);
        System.out.println("Getting URL for queue " + queueName);
        String queueUrlNew = getQueueUrl(sqsClient, queueName);
        System.out.println("Queue URL: " + queueUrlNew);
        System.out.println("Deleting queue " + queueName);
        deleteQueue(sqsClient, queueUrl);

        final String testQueue1Name = "test_queue_1";
        final String testQueue2Name = "test_queue_2";
        System.out.println("Creating queue named " + testQueue1Name);
        createQueue(sqsClient, testQueue1Name);
        System.out.println("Creating queue named " + testQueue2Name);
        createQueue(sqsClient, testQueue2Name);
        final String queueNamePrefix = testQueue1Name.substring(0, 4);
        System.out.println("Listing urls for queues with names starting with " + queueNamePrefix);
        List<String> queueUrls = listQueues(sqsClient, queueNamePrefix);
        System.out.println("URLs: ");
        queueUrls.forEach(url -> System.out.println("   " + url));



    }

    private static String createQueue(SqsClient sqsClient, String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

        CreateQueueResponse createQueueResponse = sqsClient.createQueue(createQueueRequest);
        return createQueueResponse.queueUrl();
    }

    private static void deleteQueue(SqsClient client, String queueUrl) {
        try {
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();
            client.deleteQueue(deleteQueueRequest);
        } catch (AwsServiceException | SdkClientException e) {
            e.printStackTrace();
        }
    }

    private static String getQueueUrl(SqsClient client, String queueName) {
        GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();

        GetQueueUrlResponse getQueueUrlResponse = client.getQueueUrl(getQueueUrlRequest);
        return getQueueUrlResponse.queueUrl();
    }

    private static List<String> listQueues(SqsClient client, String queueNamePrefix) {
        ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder()
                .queueNamePrefix(queueNamePrefix)
                .build();
        ListQueuesResponse listQueuesResponse = client.listQueues(listQueuesRequest);
        return listQueuesResponse.queueUrls();
    }


}
