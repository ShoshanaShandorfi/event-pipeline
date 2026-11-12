package dev.eventpipeline.service.service.processing;

import dev.eventpipeline.service.config.EventCollectionInitializer;
import dev.eventpipeline.service.config.MongoTestcontainersConfig;
import dev.eventpipeline.service.config.ProcessingProperties;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventType;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Exercises the idempotent claim/complete status-transition logic directly against a real MongoDB
 * (via Testcontainers), independent of SQS message delivery mechanics (those are covered separately by
 * {@code SqsEventConsumerIntegrationTest}). The service is constructed around the real repository and
 * a per-test processor spy, so this class reuses the Mongo slice shared by repository tests without
 * starting an application server, publisher, consumer or LocalStack. Calls to
 * {@link EventProcessingService#process(String, String)} simulate delivery/redelivery/concurrency directly.
 */
@DataMongoTest
class EventProcessingServiceIntegrationTest extends MongoTestcontainersConfig {

    private EventProcessingService processingService;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    private EventProcessor eventProcessor;

    @BeforeEach
    void setUp() throws Exception {
        mongoTemplate.getDb().drop();
        new EventCollectionInitializer().initializeEventCollection(mongoTemplate)
                .run(new DefaultApplicationArguments());
        eventProcessor = spy(new EventProcessor());
        processingService = new EventProcessingService(eventRepository, eventProcessor,
                Clock.systemUTC(), new ProcessingProperties(3, 5, 60));
    }

    @Test
    void successfulClaimProcessesEventExactlyOnceAndMarksProcessed() {
        Event event = newReceivedEvent("evt-process-ok");
        eventRepository.save(event);

        ProcessingOutcome outcome = processingService.process("evt-process-ok", "message-1");

        assertThat(outcome).isEqualTo(ProcessingOutcome.PROCESSED);
        verify(eventProcessor, times(1)).process(argThat(e -> e.getEventId().equals("evt-process-ok")));

        Event stored = eventRepository.findById("evt-process-ok").orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(EventStatus.PROCESSED);
        assertThat(stored.getAttempts()).isEqualTo(1);
        assertThat(stored.getProcessedAt()).isNotNull();
    }

    @Test
    void redeliveredMessageForAlreadyProcessedEventIsAckedWithoutReprocessing() {
        Event event = newReceivedEvent("evt-process-redelivered");
        eventRepository.save(event);

        ProcessingOutcome first = processingService.process("evt-process-redelivered", "message-1");
        assertThat(first).isEqualTo(ProcessingOutcome.PROCESSED);
        verify(eventProcessor, times(1)).process(argThat(e -> e.getEventId().equals("evt-process-redelivered")));

        // Simulates the same message being redelivered by SQS after the event is already PROCESSED.
        ProcessingOutcome second = processingService.process("evt-process-redelivered", "message-1");

        assertThat(second).isEqualTo(ProcessingOutcome.ALREADY_PROCESSED);
        // The business step must not run a second time for the duplicate delivery.
        verify(eventProcessor, times(1)).process(argThat(e -> e.getEventId().equals("evt-process-redelivered")));
    }

    @Test
    void processorExceptionResetsEventForRetryAndDoesNotMarkProcessed() {
        Event event = newReceivedEvent("evt-process-throws");
        eventRepository.save(event);
        doThrow(new RuntimeException("simulated business failure"))
                .when(eventProcessor).process(argThat(e -> e.getEventId().equals("evt-process-throws")));

        ProcessingOutcome outcome = processingService.process("evt-process-throws", "message-1");

        assertThat(outcome).isEqualTo(ProcessingOutcome.PROCESSING_FAILED);
        Event stored = eventRepository.findById("evt-process-throws").orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(EventStatus.RECEIVED);
        assertThat(stored.getAttempts()).isEqualTo(1);
        assertThat(stored.getProcessedAt()).isNull();
        assertThat(stored.getFailureReason()).isEqualTo("java.lang.RuntimeException: simulated business failure");
        assertRetryDocumentTypes(stored.getEventId());
    }

    @Test
    void retryThenSuccessUnsetsFailureReasonAndPreservesValidatorTypes() {
        String eventId = "evt-process-retry-success";
        eventRepository.save(newReceivedEvent(eventId));
        doThrow(new IllegalStateException("temporary business failure")).doNothing()
                .when(eventProcessor).process(argThat(event -> event.getEventId().equals(eventId)));

        assertThat(processingService.process(eventId, "message-1")).isEqualTo(ProcessingOutcome.PROCESSING_FAILED);
        assertRetryDocumentTypes(eventId);
        assertThat(processingService.process(eventId, "message-1")).isEqualTo(ProcessingOutcome.PROCESSED);

        Event stored = eventRepository.findById(eventId).orElseThrow();
        assertThat(stored.getAttempts()).isEqualTo(2);
        assertThat(stored.getFailureReason()).isNull();
        Document raw = mongoTemplate.getCollection("events").find(new Document("_id", eventId)).first();
        assertThat(raw).isNotNull().doesNotContainKey("failureReason");
        assertThat(raw.get("attempts")).isInstanceOf(Integer.class);
        assertThat(raw.get("processedAt")).isInstanceOf(java.util.Date.class);
    }

    @Test
    void manyWorkersRacingOnTheSameEventExactlyOneClaimsIt() throws Exception {
        Event event = newReceivedEvent("evt-process-race");
        eventRepository.save(event);

        int workerCount = 20;
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        CountDownLatch readyLatch = new CountDownLatch(workerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        try {
            List<Future<ProcessingOutcome>> futures = new ArrayList<>();
            for (int i = 0; i < workerCount; i++) {
                futures.add(pool.submit(() -> {
                    readyLatch.countDown();
                    startLatch.await();
                    return processingService.process("evt-process-race", "message-1");
                }));
            }
            readyLatch.await();
            startLatch.countDown();

            AtomicInteger processedCount = new AtomicInteger();
            for (Future<ProcessingOutcome> future : futures) {
                if (future.get(10, TimeUnit.SECONDS) == ProcessingOutcome.PROCESSED) {
                    processedCount.incrementAndGet();
                }
            }

            assertThat(processedCount.get()).isEqualTo(1);
        } finally {
            pool.shutdown();
            pool.awaitTermination(10, TimeUnit.SECONDS);
        }

        // Exactly one worker ever ran the business step for this event, no matter how many raced the claim.
        verify(eventProcessor, times(1)).process(argThat(e -> e.getEventId().equals("evt-process-race")));

        Event stored = eventRepository.findById("evt-process-race").orElseThrow();
        assertThat(stored.getAttempts()).isEqualTo(1);
    }

    private Event newReceivedEvent(String eventId) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        return new Event(eventId, "cust-1", EventType.PAYMENT, new BigDecimal("10.00"), "USD",
                now, EventStatus.RECEIVED, now, now, null, 0, null, null);
    }

    private void assertRetryDocumentTypes(String eventId) {
        Document raw = mongoTemplate.getCollection("events").find(new Document("_id", eventId)).first();
        assertThat(raw).isNotNull();
        assertThat(raw.get("attempts")).isInstanceOf(Integer.class);
        assertThat(raw.get("updatedAt")).isInstanceOf(java.util.Date.class);
        assertThat(raw.get("failureReason")).isInstanceOf(String.class);
    }
}
