package dev.eventpipeline.service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Top-level AWS settings shared by the publisher and the consumer.
 *
 * @param region AWS region (e.g. {@code us-east-1}).
 */
@ConfigurationProperties(prefix = "aws")
public record AwsProperties(String region) {
}
