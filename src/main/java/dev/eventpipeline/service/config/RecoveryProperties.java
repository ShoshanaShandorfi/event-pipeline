package dev.eventpipeline.service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Non-secret settings for processing recovery. The stale-PROCESSING threshold MUST be larger than
 * both the SQS visibility timeout and the maximum expected business processing time.
 */
@ConfigurationProperties(prefix = "pipeline.recovery")
public record RecoveryProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("10") int intervalSeconds,
        @DefaultValue("300") int staleProcessingThresholdSeconds) {

    public RecoveryProperties {
        if (intervalSeconds < 1 || staleProcessingThresholdSeconds < 1) {
            throw new IllegalArgumentException("Recovery interval and stale threshold must be positive");
        }
    }
}
