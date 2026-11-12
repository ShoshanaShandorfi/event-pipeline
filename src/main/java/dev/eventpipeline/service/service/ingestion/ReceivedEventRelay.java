package dev.eventpipeline.service.service.ingestion;

import dev.eventpipeline.service.config.RelayProperties;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.repository.EventRepository;
import dev.eventpipeline.service.messaging.ingestion.SqsEventPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Repairs failure E (crash between Mongo insert and send) using Mongo as the durable source of work.
 * The atomic updatedAt lease prevents different instances from publishing the same stale event in
 * one stale window. SQS redelivery and later relay windows can still create duplicates (failure C);
 * the processing claim makes those harmless.
 */
@Component
@ConditionalOnProperty(prefix = "pipeline.relay", name = "enabled", havingValue = "true", matchIfMissing = true)
@Slf4j
public class ReceivedEventRelay {

    private final EventRepository eventRepository;
    private final SqsEventPublisher eventPublisher;
    private final Clock clock;
    private final RelayProperties properties;

    public ReceivedEventRelay(EventRepository eventRepository, SqsEventPublisher eventPublisher,
                              Clock clock, RelayProperties properties) {
        this.eventRepository = eventRepository;
        this.eventPublisher = eventPublisher;
        this.clock = clock;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${pipeline.relay.interval-seconds:10}", timeUnit = TimeUnit.SECONDS)
    public void relayStaleEvents() {
        Instant now = clock.instant();
        Instant staleBefore = now.minusSeconds(properties.staleReceivedThresholdSeconds());
        int republished = 0;
        try {
            for (int index = 0; index < properties.batchSize(); index++) {
                Optional<Event> leased = eventRepository.leaseStaleReceived(staleBefore, now);
                if (leased.isEmpty()) {
                    break;
                }
                String eventId = leased.get().getEventId();
                try {
                    eventPublisher.publish(eventId);
                    republished++;
                } catch (RuntimeException failure) {
                    // A failed hand-off keeps its lease and becomes eligible after another stale
                    // window. The asynchronous publisher also leaves failed sends recoverable here.
                    log.warn("eventId={} relay publish failed; eligible after stale window errorType={}",
                            eventId, failure.getClass().getSimpleName());
                }
            }
        } catch (RuntimeException failure) {
            // Failure B: Mongo can be unavailable. No attempt is consumed by this lease, persisted
            // work remains safe, and an exception must never terminate future scheduled runs.
            log.warn("RECEIVED relay run interrupted; retrying next tick errorType={}",
                    failure.getClass().getSimpleName());
        }
        if (republished > 0) {
            log.info("RECEIVED relay republished={}", republished);
        }
    }
}
