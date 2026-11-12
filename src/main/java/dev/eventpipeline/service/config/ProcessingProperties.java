package dev.eventpipeline.service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Retry policy. maxAttempts MUST equal the main SQS queue redrive maxReceiveCount. */
@ConfigurationProperties(prefix = "pipeline.processing")
public record ProcessingProperties(
        @DefaultValue("3") int maxAttempts,
        @DefaultValue("5") int retryBackoffBaseSeconds,
        @DefaultValue("60") int retryBackoffCapSeconds) {
    public ProcessingProperties {
        if (maxAttempts < 1 || retryBackoffBaseSeconds < 1 || retryBackoffCapSeconds < retryBackoffBaseSeconds
                || retryBackoffCapSeconds > 43200) {
            throw new IllegalArgumentException("Invalid processing attempt budget or SQS retry backoff");
        }
    }
}
