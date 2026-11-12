package dev.eventpipeline.service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Provides a single injectable {@link Clock} so server-side timestamps (createdAt/updatedAt) are
 * deterministic and mockable in tests.
 */
@Configuration
public class ClockConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
