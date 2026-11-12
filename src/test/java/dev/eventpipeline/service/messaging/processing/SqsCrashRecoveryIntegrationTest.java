package dev.eventpipeline.service.messaging.processing;

import dev.eventpipeline.service.config.EventCollectionInitializer;
import dev.eventpipeline.service.config.LocalStackTestcontainersConfig;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventType;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** E: both crash gaps recover through separately scheduled jobs and actual SQS delivery. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Import(SqsCrashRecoveryIntegrationTest.RecoveryClockConfiguration.class)
class SqsCrashRecoveryIntegrationTest extends LocalStackTestcontainersConfig {

    @DynamicPropertySource
    static void recoveryProperties(DynamicPropertyRegistry registry) {
        registry.add("aws.sqs.consumer.enabled", () -> true);
        registry.add("aws.sqs.consumer.wait-time-seconds", () -> 1);
        registry.add("aws.sqs.consumer.visibility-timeout-seconds", () -> 1);
        registry.add("aws.sqs.consumer.worker-threads", () -> 1);
        registry.add("pipeline.processing.max-attempts", () -> MAX_RECEIVE_COUNT);
        registry.add("pipeline.processing.retry-backoff-base-seconds", () -> 1);
        registry.add("pipeline.processing.retry-backoff-cap-seconds", () -> 1);
        registry.add("pipeline.relay.enabled", () -> true);
        registry.add("pipeline.relay.interval-seconds", () -> 1);
        registry.add("pipeline.relay.stale-received-threshold-seconds", () -> 1);
        registry.add("pipeline.relay.batch-size", () -> 100);
        registry.add("pipeline.recovery.enabled", () -> true);
        registry.add("pipeline.recovery.interval-seconds", () -> 1);
        // Greater than the 1s visibility timeout and this test's near-instant business step.
        registry.add("pipeline.recovery.stale-processing-threshold-seconds", () -> 3);
    }

    @Autowired private EventRepository eventRepository;
    @Autowired private MongoTemplate mongoTemplate;
    @Autowired private AdjustableClock clock;

    @BeforeEach
    void initializeValidator() throws Exception {
        clock.reset();
        mongoTemplate.getDb().drop();
        new EventCollectionInitializer().initializeEventCollection(mongoTemplate)
                .run(new DefaultApplicationArguments());
    }

    @Test
    void insertSucceededButSendNeverHappened_relayEventuallyDeliversAndProcesses() {
        Instant now = now();
        eventRepository.insert(event("insert-send-crash", EventStatus.RECEIVED, 0, now));
        assertThat(mainQueueMessageCount()).isZero();

        clock.advanceSeconds(2);
        assertProcessed("insert-send-crash", 1);
        await().atMost(Duration.ofSeconds(10)).until(() -> mainQueueMessageCount() == 0 && dlqMessageCount() == 0);
    }

    @Test
    void processingCrash_recoveryResetsThenIndependentRelayPublishesAndConsumerCompletes() {
        Instant stale = now().minusSeconds(10);
        eventRepository.insert(event("processing-crash", EventStatus.PROCESSING, 1, stale));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Event recovered = eventRepository.findById("processing-crash").orElseThrow();
            assertThat(recovered.getStatus()).isEqualTo(EventStatus.RECEIVED);
            assertThat(recovered.getFailureReason()).isEqualTo("recovered after processing timeout");
            assertThat(recovered.getAttempts()).isEqualTo(1);
            assertThat(recovered.getUpdatedAt()).isAfter(stale);
        });

        // Recovery only updates Mongo. The deliberate extra stale-RECEIVED window lets the
        // ingestion relay publish later while keeping processing independent of EventPublisher.
        clock.advanceSeconds(2);
        assertProcessed("processing-crash", 2);
        await().atMost(Duration.ofSeconds(10)).until(() -> mainQueueMessageCount() == 0 && dlqMessageCount() == 0);
    }

    private void assertProcessed(String eventId, int attempts) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Event processed = eventRepository.findById(eventId).orElseThrow();
            assertThat(processed.getStatus()).isEqualTo(EventStatus.PROCESSED);
            assertThat(processed.getAttempts()).isEqualTo(attempts);
            assertThat(processed.getFailureReason()).isNull();
            assertThat(processed.getProcessedAt()).isNotNull();
        });
    }

    private Event event(String eventId, EventStatus status, int attempts, Instant updatedAt) {
        return new Event(eventId, "cust-crash", EventType.PAYMENT, new BigDecimal("10.00"), "USD",
                updatedAt, status, updatedAt, updatedAt, null, attempts, null, null);
    }

    private Instant now() {
        return clock.instant().truncatedTo(ChronoUnit.MILLIS);
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class RecoveryClockConfiguration {
        @Bean
        @Primary
        AdjustableClock recoveryTestClock() {
            return new AdjustableClock();
        }
    }

    /**
     * Scheduled ticks remain real, but lease age advances only when the test requests it. This
     * makes the intermediate RECEIVED state and fresh PROCESSING checks independent of host pauses.
     */
    static class AdjustableClock extends Clock {
        private static final Instant START = Instant.parse("2026-01-01T12:00:00Z");
        private final AtomicReference<Instant> current;
        private final ZoneId zone;

        AdjustableClock() {
            this(new AtomicReference<>(START), ZoneOffset.UTC);
        }

        private AdjustableClock(AtomicReference<Instant> current, ZoneId zone) {
            this.current = current;
            this.zone = zone;
        }

        void reset() {
            current.set(START);
        }

        void advanceSeconds(long seconds) {
            current.updateAndGet(value -> value.plusSeconds(seconds));
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId requestedZone) {
            return new AdjustableClock(current, requestedZone);
        }

        @Override
        public Instant instant() {
            return current.get();
        }
    }
}
