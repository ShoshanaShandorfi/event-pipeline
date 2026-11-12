package dev.eventpipeline.service.messaging.processing;

import dev.eventpipeline.service.config.SqsConsumerProperties;
import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.QueueNotFoundStrategy;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.time.Duration;

/**
 * Listener container settings for {@link SqsEventListener}. Entirely absent from the context when
 * {@code aws.sqs.consumer.enabled=false}, so API-only tests never start a background poller.
 */
@Configuration
@ConditionalOnProperty(prefix = "aws.sqs.consumer", name = "enabled", havingValue = "true", matchIfMissing = true)
public class SqsConsumerConfig {

    @Bean
    public SqsMessageListenerContainerFactory<Object> defaultSqsListenerContainerFactory(
            SqsAsyncClient sqsAsyncClient, SqsConsumerProperties consumerProperties) {
        return SqsMessageListenerContainerFactory.builder()
                .sqsAsyncClient(sqsAsyncClient)
                .configure(options -> options
                        // Delete only when the listener explicitly acknowledges.
                        .acknowledgementMode(AcknowledgementMode.MANUAL)
                        // A missing queue must fail startup, never be created implicitly.
                        .queueNotFoundStrategy(QueueNotFoundStrategy.FAIL)
                        .maxConcurrentMessages(consumerProperties.workerThreads())
                        // The container requires maxMessagesPerPoll <= maxConcurrentMessages.
                        .maxMessagesPerPoll(Math.min(consumerProperties.maxMessages(), consumerProperties.workerThreads()))
                        .pollTimeout(Duration.ofSeconds(consumerProperties.waitTimeSeconds()))
                        .messageVisibility(Duration.ofSeconds(consumerProperties.visibilityTimeoutSeconds()))
                        // In-flight messages get a bounded time to finish on shutdown.
                        .listenerShutdownTimeout(Duration.ofSeconds(consumerProperties.shutdownWaitSeconds())))
                .build();
    }
}
