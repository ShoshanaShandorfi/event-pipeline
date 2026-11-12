package dev.eventpipeline.service.service.processing;

import dev.eventpipeline.service.config.ProcessingProperties;
import dev.eventpipeline.service.config.RecoveryProperties;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Repairs failure E (a worker crashed while owning PROCESSING) entirely through MongoDB. It never
 * publishes: an eligible event returns to RECEIVED and the ingestion relay republishes only after
 * its additional stale-RECEIVED window, a deliberate delay that keeps the two sides independent.
 *
 * <p>A live worker slower than the stale threshold may still execute the business step twice. The
 * attempts fencing token prevents it overwriting a newer result, but the business step must itself
 * be idempotent. Set the threshold above both visibility timeout and expected processing duration.
 */
@Component
@ConditionalOnProperty(prefix = "pipeline.recovery", name = "enabled", havingValue = "true", matchIfMissing = true)
@Slf4j
public class StuckProcessingRecovery {

    private final EventRepository eventRepository;
    private final Clock clock;
    private final RecoveryProperties properties;
    private final ProcessingProperties processingProperties;

    public StuckProcessingRecovery(EventRepository eventRepository, Clock clock,
                                   RecoveryProperties properties, ProcessingProperties processingProperties) {
        this.eventRepository = eventRepository;
        this.clock = clock;
        this.properties = properties;
        this.processingProperties = processingProperties;
    }

    @Scheduled(fixedDelayString = "${pipeline.recovery.interval-seconds:10}", timeUnit = TimeUnit.SECONDS)
    public void recoverStaleEvents() {
        Instant now = clock.instant();
        Instant staleBefore = now.minusSeconds(properties.staleProcessingThresholdSeconds());
        int recovered = 0;
        int failed = 0;
        try {
            while (true) {
                Optional<Event> changed = eventRepository.recoverStaleProcessing(
                        staleBefore, now, processingProperties.maxAttempts());
                if (changed.isEmpty()) {
                    break;
                }
                if (changed.get().getStatus() == EventStatus.FAILED) {
                    // Failure D: FAILED stays terminal. Its retained SQS message can reach the DLQ;
                    // operations must inspect it and explicitly reset state to request reprocessing.
                    failed++;
                } else {
                    recovered++;
                }
            }
        } catch (RuntimeException failure) {
            // Failure B: leave remaining persisted work untouched and keep future ticks alive.
            log.warn("PROCESSING recovery run interrupted; retrying next tick errorType={}",
                    failure.getClass().getSimpleName());
        }
        if (recovered > 0 || failed > 0) {
            log.info("PROCESSING recovery recovered={} failed={}", recovered, failed);
        }
    }
}
