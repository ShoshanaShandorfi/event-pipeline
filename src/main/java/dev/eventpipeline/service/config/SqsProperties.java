package dev.eventpipeline.service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Common SQS connection settings shared by the publisher (ingestion side) and the consumer
 * (processing side). Queue URLs are never hard-coded: they are resolved from the queue name.
 *
 * @param endpoint   Endpoint override, e.g. the LocalStack URL. Must be absent/blank against real AWS,
 *                   where the SDK talks to the regional SQS endpoint directly.
 * @param queueName  Name of the main queue.
 * @param dlqName    Name of the dead-letter queue (used only to validate it exists at startup; the
 *                   redrive policy itself is configured on the queue, not in application code).
 */
@ConfigurationProperties(prefix = "aws.sqs")
public record SqsProperties(String endpoint, String queueName, String dlqName) {
}
