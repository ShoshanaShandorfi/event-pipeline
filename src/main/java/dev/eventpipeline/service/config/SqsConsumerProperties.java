package dev.eventpipeline.service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Consumer (processing side) SQS settings. All values are non-secret and have safe defaults in
 * {@code application.yml}; they may be overridden via environment variables.
 *
 * @param enabled                   switches the whole consumer off, e.g. for API-only tests that must
 *                                  not start a background poller.
 * @param maxMessages               max messages fetched per {@code ReceiveMessage} poll (SQS max is 10).
 * @param waitTimeSeconds           long-poll wait time in seconds.
 * @param visibilityTimeoutSeconds  visibility timeout applied to received messages.
 * @param workerThreads             number of long-polling worker threads (bounds concurrency).
 * @param shutdownWaitSeconds       bounded time to let in-flight messages finish before closing the client.
 */
@ConfigurationProperties(prefix = "aws.sqs.consumer")
public record SqsConsumerProperties(
        boolean enabled,
        int maxMessages,
        int waitTimeSeconds,
        int visibilityTimeoutSeconds,
        int workerThreads,
        int shutdownWaitSeconds) {
}
