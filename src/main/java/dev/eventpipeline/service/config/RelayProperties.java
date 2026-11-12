package dev.eventpipeline.service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Non-secret settings for the ingestion-side RECEIVED event relay. */
@ConfigurationProperties(prefix = "pipeline.relay")
public record RelayProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("10") int intervalSeconds,
        @DefaultValue("60") int staleReceivedThresholdSeconds,
        @DefaultValue("100") int batchSize) {

    public RelayProperties {
        if (intervalSeconds < 1 || staleReceivedThresholdSeconds < 1 || batchSize < 1) {
            throw new IllegalArgumentException("Relay interval, stale threshold and batch size must be positive");
        }
    }
}
