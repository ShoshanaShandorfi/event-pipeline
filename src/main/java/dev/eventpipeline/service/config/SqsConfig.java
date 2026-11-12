package dev.eventpipeline.service.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletionException;

/**
 * The single {@link SqsAsyncClient} shared by the publisher (ingestion side) and the listener
 * (processing side), plus a startup check that both queues exist.
 */
@Configuration
@Slf4j
public class SqsConfig {

    @Bean
    public SqsAsyncClient sqsAsyncClient(AwsProperties awsProperties, SqsProperties sqsProperties,
                                         SqsConsumerProperties consumerProperties) {
        SqsAsyncClientBuilder builder = SqsAsyncClient.builder()
                .region(Region.of(awsProperties.region()))
                // In real AWS this picks up the task/instance IAM role via the default credentials
                // provider chain; no credentials are ever embedded here. Against LocalStack it picks up
                // the AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY test values from the environment.
                .credentialsProvider(DefaultCredentialsProvider.create())
                // Longer than the long-poll wait time, so a poll is never mistaken for a timeout.
                .overrideConfiguration(o -> o.apiCallTimeout(Duration.ofSeconds(consumerProperties.waitTimeSeconds() + 10L)));

        String endpoint = sqsProperties.endpoint();
        if (endpoint != null && !endpoint.isBlank()) {
            // Only ever set for LocalStack / local development; absent entirely against real AWS.
            builder.endpointOverride(URI.create(endpoint));
        }
        return builder.build();
    }

    /**
     * Fails startup with a clear message when a queue is missing. The application never talks to the
     * DLQ directly (SQS redrives on its own), but a missing DLQ would silently break redrive.
     */
    @Bean
    public SmartInitializingSingleton sqsQueuesStartupValidator(SqsAsyncClient sqsAsyncClient, SqsProperties sqsProperties) {
        return () -> {
            requireQueue(sqsAsyncClient, sqsProperties.queueName());
            requireQueue(sqsAsyncClient, sqsProperties.dlqName());
        };
    }

    private void requireQueue(SqsAsyncClient client, String queueName) {
        try {
            client.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build()).join();
            log.info("Resolved SQS queue '{}'", queueName);
        } catch (CompletionException ex) {
            if (ex.getCause() instanceof QueueDoesNotExistException) {
                throw new IllegalStateException("SQS queue '" + queueName + "' does not exist. Create it "
                        + "(see localstack/init/ready.d for local development) before starting the application.", ex.getCause());
            }
            throw ex;
        }
    }
}
