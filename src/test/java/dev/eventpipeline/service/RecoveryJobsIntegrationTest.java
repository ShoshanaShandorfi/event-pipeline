package dev.eventpipeline.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.eventpipeline.service.config.EventCollectionInitializer;
import dev.eventpipeline.service.testsupport.IngestionIntegrationTestSupport;
import dev.eventpipeline.service.config.ProcessingProperties;
import dev.eventpipeline.service.config.RecoveryProperties;
import dev.eventpipeline.service.config.RelayProperties;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventType;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import dev.eventpipeline.service.messaging.ingestion.SqsEventPublisher;
import dev.eventpipeline.service.service.ingestion.ReceivedEventRelay;
import dev.eventpipeline.service.service.processing.StuckProcessingRecovery;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.data.mongodb.core.MongoTemplate;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real strict-validator MongoDB leases and LocalStack sends, with deterministic job clocks. */
class RecoveryJobsIntegrationTest extends IngestionIntegrationTestSupport {

    private static final Instant NOW = Instant.parse("2026-01-01T12:00:00Z").truncatedTo(ChronoUnit.MILLIS);
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final RelayProperties RELAY = new RelayProperties(false, 1, 60, 100);
    private static final RecoveryProperties RECOVERY = new RecoveryProperties(false, 1, 300);

    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    private EventRepository eventRepository;
    @Autowired
    private SqsEventPublisher eventPublisher;
    @Autowired
    private ProcessingProperties processingProperties;
    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void initializeStrictCollection() throws Exception {
        mongoTemplate.getDb().drop();
        new EventCollectionInitializer().initializeEventCollection(mongoTemplate)
                .run(new DefaultApplicationArguments());
    }

    @Test
    void relayPublishesOnlyStaleReceivedOncePerLeaseWindowAndPreservesSchemaTypes() throws Exception {
        eventRepository.insert(event("old-received", EventStatus.RECEIVED, NOW.minusSeconds(61), 0));
        eventRepository.insert(event("fresh-received", EventStatus.RECEIVED, NOW, 0));
        eventRepository.insert(event("old-processing", EventStatus.PROCESSING, NOW.minusSeconds(61), 1));
        eventRepository.insert(event("old-processed", EventStatus.PROCESSED, NOW.minusSeconds(61), 1));
        eventRepository.insert(event("old-failed", EventStatus.FAILED, NOW.minusSeconds(61), 3));

        ReceivedEventRelay relay = new ReceivedEventRelay(eventRepository, eventPublisher, CLOCK, RELAY);
        relay.relayStaleEvents();
        relay.relayStaleEvents();

        await().atMost(Duration.ofSeconds(10)).until(() -> mainQueueMessageCount() == 1);
        assertThat(receiveEventIds(1)).containsExactly("old-received");
        Event leased = eventRepository.findById("old-received").orElseThrow();
        assertThat(leased.getUpdatedAt()).isEqualTo(NOW);
        assertThat(leased.getAttempts()).isZero();
        assertSchemaTypes("old-received", false);
        assertThat(eventRepository.findById("old-processing").orElseThrow().getUpdatedAt())
                .isEqualTo(NOW.minusSeconds(61));
        assertThat(mainQueueMessageCount()).isZero();
    }

    @Test
    void simultaneousRelayInstancesPublishEveryStaleEventExactlyOncePerWindow() throws Exception {
        int count = 20;
        for (int index = 0; index < count; index++) {
            eventRepository.insert(event("concurrent-" + index, EventStatus.RECEIVED, NOW.minusSeconds(61), 0));
        }
        Map<String, AtomicInteger> publishCounts = new ConcurrentHashMap<>();
        SqsEventPublisher countedPublisher = Mockito.mock(SqsEventPublisher.class);
        Mockito.doAnswer(invocation -> {
            String eventId = invocation.getArgument(0);
            publishCounts.computeIfAbsent(eventId, ignored -> new AtomicInteger()).incrementAndGet();
            eventPublisher.publish(eventId);
            return null;
        }).when(countedPublisher).publish(ArgumentMatchers.anyString());
        ReceivedEventRelay first = new ReceivedEventRelay(eventRepository, countedPublisher, CLOCK, RELAY);
        ReceivedEventRelay second = new ReceivedEventRelay(eventRepository, countedPublisher, CLOCK, RELAY);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> firstRun = pool.submit(() -> runAfter(start, first));
            Future<?> secondRun = pool.submit(() -> runAfter(start, second));
            start.countDown();
            firstRun.get(10, TimeUnit.SECONDS);
            secondRun.get(10, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        first.relayStaleEvents();
        second.relayStaleEvents();

        assertThat(publishCounts).hasSize(count);
        assertThat(publishCounts.values()).allSatisfy(value -> assertThat(value.get()).isEqualTo(1));
        await().atMost(Duration.ofSeconds(10)).until(() -> mainQueueMessageCount() == count);
        assertThat(receiveEventIds(count)).hasSize(count).doesNotHaveDuplicates();
        assertThat(eventRepository.findAll()).allSatisfy(event -> assertThat(event.getUpdatedAt()).isEqualTo(NOW));
    }

    @Test
    void recoveryResetsOldProcessingFailsExhaustedAndLeavesFreshProcessingUntouched() {
        eventRepository.insert(event("recover-retry", EventStatus.PROCESSING, NOW.minusSeconds(301), 1));
        eventRepository.insert(event("recover-exhausted", EventStatus.PROCESSING, NOW.minusSeconds(301), 3));
        eventRepository.insert(event("recover-fresh", EventStatus.PROCESSING, NOW.minusSeconds(299), 1));
        eventRepository.insert(event("terminal-failed", EventStatus.FAILED, NOW.minusSeconds(301), 3));
        StuckProcessingRecovery recovery = new StuckProcessingRecovery(
                eventRepository, CLOCK, RECOVERY, processingProperties);

        recovery.recoverStaleEvents();
        recovery.recoverStaleEvents();

        Event retry = eventRepository.findById("recover-retry").orElseThrow();
        assertThat(retry.getStatus()).isEqualTo(EventStatus.RECEIVED);
        assertThat(retry.getAttempts()).isEqualTo(1);
        assertThat(retry.getFailureReason()).isEqualTo("recovered after processing timeout");
        assertThat(retry.getUpdatedAt()).isEqualTo(NOW);
        Event exhausted = eventRepository.findById("recover-exhausted").orElseThrow();
        assertThat(exhausted.getStatus()).isEqualTo(EventStatus.FAILED);
        assertThat(exhausted.getAttempts()).isEqualTo(3);
        assertThat(exhausted.getFailureReason()).isEqualTo("processing timeout after 3 attempts");
        assertThat(exhausted.getUpdatedAt()).isEqualTo(NOW);
        Event fresh = eventRepository.findById("recover-fresh").orElseThrow();
        assertThat(fresh.getStatus()).isEqualTo(EventStatus.PROCESSING);
        assertThat(fresh.getUpdatedAt()).isEqualTo(NOW.minusSeconds(299));
        assertThat(eventRepository.findById("terminal-failed").orElseThrow().getStatus()).isEqualTo(EventStatus.FAILED);
        assertSchemaTypes("recover-retry", true);
        assertSchemaTypes("recover-exhausted", true);

        List<String> published = new ArrayList<>();
        new ReceivedEventRelay(eventRepository, recordingPublisher(published), CLOCK, RELAY).relayStaleEvents();
        assertThat(published).isEmpty();
        // Deliberately wait the extra relay window: recovery itself has no publisher dependency.
        Clock later = Clock.offset(CLOCK, Duration.ofSeconds(61));
        new ReceivedEventRelay(eventRepository, recordingPublisher(published), later, RELAY).relayStaleEvents();
        assertThat(published).containsExactly("recover-retry");
    }

    @Test
    void staleReceivedLookupUsesStatusUpdatedAtIndexWithoutHint() {
        List<Event> events = new ArrayList<>();
        for (int index = 0; index < 300; index++) {
            events.add(event("indexed-processed-" + index, EventStatus.PROCESSED, NOW, 1));
        }
        events.add(event("indexed-stale", EventStatus.RECEIVED, NOW.minusSeconds(61), 0));
        eventRepository.insert(events);
        Document filter = new Document("status", "RECEIVED")
                .append("updatedAt", new Document("$lt", Date.from(NOW.minusSeconds(60))));
        Document explain = mongoTemplate.getDb().runCommand(new Document("explain",
                new Document("find", "events").append("filter", filter)
                        .append("sort", new Document("updatedAt", 1))).append("verbosity", "queryPlanner"));

        // A lease returns the full event, so Mongo fetches its document after the indexed range scan.
        // This is an index-supported lookup, not a projection-only covered query.
        assertThat(explain.get("queryPlanner", Document.class).get("winningPlan", Document.class).toJson())
                .contains("IXSCAN", "status_1_updatedAt_1");
    }

    private void assertSchemaTypes(String eventId, boolean hasFailureReason) {
        Document raw = mongoTemplate.getCollection("events").find(new Document("_id", eventId)).first();
        assertThat(raw).isNotNull();
        assertThat(raw.get("attempts")).isInstanceOf(Integer.class);
        assertThat(raw.get("createdAt")).isInstanceOf(Date.class);
        assertThat(raw.get("updatedAt")).isInstanceOf(Date.class);
        if (hasFailureReason) {
            assertThat(raw.get("failureReason")).isInstanceOf(String.class);
        } else {
            assertThat(raw).doesNotContainKey("failureReason");
        }
    }

    private List<String> receiveEventIds(int expected) throws Exception {
        List<String> ids = new ArrayList<>();
        while (ids.size() < expected) {
            List<Message> messages = adminSqsClient().receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(mainQueueUrl()).maxNumberOfMessages(10).waitTimeSeconds(1).build()).messages();
            assertThat(messages).isNotEmpty();
            for (Message message : messages) {
                ids.add(objectMapper.readTree(message.body()).get("eventId").asText());
                adminSqsClient().deleteMessage(DeleteMessageRequest.builder().queueUrl(mainQueueUrl())
                        .receiptHandle(message.receiptHandle()).build());
            }
        }
        return ids;
    }

    private static void runAfter(CountDownLatch start, ReceivedEventRelay relay) {
        try {
            if (!start.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Relay start barrier timed out");
            }
            relay.relayStaleEvents();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failure);
        }
    }

    private Event event(String eventId, EventStatus status, Instant updatedAt, int attempts) {
        return new Event(eventId, "cust-1", EventType.PAYMENT, new BigDecimal("10.00"), "USD",
                NOW, status, NOW.minusSeconds(600), updatedAt,
                status == EventStatus.PROCESSED ? updatedAt : null, attempts, null, Map.of());
    }

    /** A publisher double that only records the event ids it is asked to publish. */
    private static SqsEventPublisher recordingPublisher(List<String> sink) {
        SqsEventPublisher publisher = Mockito.mock(SqsEventPublisher.class);
        Mockito.doAnswer(invocation -> {
            sink.add(invocation.getArgument(0));
            return null;
        }).when(publisher).publish(ArgumentMatchers.anyString());
        return publisher;
    }
}
