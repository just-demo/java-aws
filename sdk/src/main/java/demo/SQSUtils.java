package demo;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.text.SimpleDateFormat;
import java.util.Date;

import static software.amazon.awssdk.regions.Region.US_EAST_1;

public class SQSUtils {
    private static final SqsClient sqsClient = SqsClient.builder()
            .region(US_EAST_1)
            .build();

    public static void main(String[] args) throws Exception {
        String queueName = "SQS-demo";
        String queueUrl = getQueueUrl(queueName);
        System.out.println(queueUrl);
        sendMessage(queueUrl, "Hello1 " + timestamp());
        sendMessage(queueUrl, "Hello2 " + timestamp());
        // Some message wont be received immediately
        retrieveMessages(queueUrl);
    }

    public static void sendMessage(String queueUrl, String message) {
        SendMessageResponse response = sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(message)
                .build());
        System.out.println("sent " + response.messageId());
    }

    public static void retrieveMessages(String queueUrl) {
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(5)
                .build();

        sqsClient.receiveMessage(receiveMessageRequest).messages().forEach(message -> {
            System.out.println("received " + message.messageId() + ": " + message.body());
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqsClient.deleteMessage(deleteMessageRequest);
        });
    }

    public static String getQueueUrl(String queueName) {
        GetQueueUrlResponse getQueueUrlResponse =
                sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
        return getQueueUrlResponse.queueUrl();
    }

    private static String timestamp() {
        return new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSS").format(new Date());
    }
}
