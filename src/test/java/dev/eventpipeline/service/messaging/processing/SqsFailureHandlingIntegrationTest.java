package dev.eventpipeline.service.messaging.processing;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.eventpipeline.service.config.EventCollectionInitializer;
import dev.eventpipeline.service.config.LocalStackTestcontainersConfig;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventType;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import dev.eventpipeline.service.repository.EventRepositoryImpl;
import dev.eventpipeline.service.service.processing.EventProcessor;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** A-D: real SQS redelivery, processor failures, and a transient Mongo claim outage. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SqsFailureHandlingIntegrationTest extends LocalStackTestcontainersConfig {

    @DynamicPropertySource
    static void failureProperties(DynamicPropertyRegistry registry) {
        registry.add("aws.sqs.consumer.enabled", () -> true);
        registry.add("aws.sqs.consumer.wait-time-seconds", () -> 1);
        registry.add("aws.sqs.consumer.visibility-timeout-seconds", () -> 1);
        registry.add("aws.sqs.consumer.worker-threads", () -> 1);
        registry.add("pipeline.processing.max-attempts", () -> MAX_RECEIVE_COUNT);
        registry.add("pipeline.processing.retry-backoff-base-seconds", () -> 1);
        registry.add("pipeline.processing.retry-backoff-cap-seconds", () -> 1);
        registry.add("pipeline.relay.enabled", () -> false);
        registry.add("pipeline.recovery.enabled", () -> false);
    }

    @Autowired private MongoTemplate mongoTemplate;
    @Autowired private ObjectMapper objectMapper;
    @MockitoSpyBean private EventRepository eventRepository;
    @MockitoSpyBean private EventProcessor eventProcessor;

    @BeforeEach
    void initializeValidator() throws Exception {
        mongoTemplate.getDb().drop();
        new EventCollectionInitializer().initializeEventCollection(mongoTemplate)
                .run(new DefaultApplicationArguments());
    }

    @Test
    void processorFailsOnceThenSucceeds_secondClaimCompletesAndDeletesMessage() throws Exception {
        String eventId = "retry-once";
        eventRepository.insert(received(eventId));
        doThrow(new IllegalStateException("temporary business failure")).doNothing()
                .when(eventProcessor).process(argThat(event -> event.getEventId().equals(eventId)));

        send(eventId);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Event stored = eventRepository.findById(eventId).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(EventStatus.PROCESSED);
            assertThat(stored.getAttempts()).isEqualTo(2);
            assertThat(stored.getFailureReason()).isNull();
        });
        await().atMost(Duration.ofSeconds(10)).until(() -> mainQueueMessageCount() == 0 && dlqMessageCount() == 0);
        verify(eventProcessor, times(2)).process(argThat(event -> event.getEventId().equals(eventId)));
        Document raw = raw(eventId);
        assertThat(raw).doesNotContainKey("failureReason");
        assertValidTypes(raw);
        assertThat(raw.get("processedAt")).isInstanceOf(java.util.Date.class);
    }

    @Test
    void processorAlwaysFails_exhaustsClaimsThenRedeliveryMovesUndeletedMessageToDlq() throws Exception {
        String eventId = "retry-exhausted";
        eventRepository.insert(received(eventId));
        doThrow(new IllegalStateException("business failure\r\n" + "x".repeat(300)))
                .when(eventProcessor).process(argThat(event -> event.getEventId().equals(eventId)));

        send(eventId);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Event stored = eventRepository.findById(eventId).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(EventStatus.FAILED);
            assertThat(stored.getAttempts()).isEqualTo(MAX_RECEIVE_COUNT);
            assertThat(stored.getFailureReason())
                    .startsWith(IllegalStateException.class.getName() + ": business failure")
                    .doesNotContain("\r", "\n", "private-metadata-value", "sensitive-customer");
            String message = stored.getFailureReason().substring((IllegalStateException.class.getName() + ": ").length());
            assertThat(message).hasSizeLessThanOrEqualTo(200);
        });
        // The final worker deliberately leaves the receipt alone. A later ReceiveMessage, after
        // visibility expires, drives the queue's redrive policy; see SqsRedriveIntegrationTest.
        await().atMost(Duration.ofSeconds(20)).until(() -> mainQueueMessageCount() == 0 && dlqMessageCount() == 1);
        verify(eventProcessor, times(MAX_RECEIVE_COUNT)).process(argThat(event -> event.getEventId().equals(eventId)));
        assertValidTypes(raw(eventId));
        assertThat(raw(eventId).get("failureReason")).isInstanceOf(String.class);
    }

    @Test
    void transientMongoFailure_doesNotConsumeAnAttemptOrLoseTheOnlyMessage() throws Exception {
        String eventId = "mongo-unavailable-once";
        eventRepository.insert(received(eventId));
        AtomicBoolean unavailable = new AtomicBoolean(true);
        AtomicInteger attemptsWhenMongoFailed = new AtomicInteger(-1);
        EventRepositoryImpl realTransitions = new EventRepositoryImpl(mongoTemplate);
        doAnswer(invocation -> {
            if (unavailable.compareAndSet(true, false)) {
                attemptsWhenMongoFailed.set(raw(eventId).getInteger("attempts"));
                throw new DataAccessResourceFailureException("simulated Mongo connection outage");
            }
            // Spring Data's repository interface is a proxy, so Mockito cannot invoke an abstract
            // interface method as a real method. Delegate subsequent calls to the production
            // Mongo fragment to retain its actual atomic claim and strict-validator behavior.
            return realTransitions.claimForProcessing(invocation.getArgument(0), invocation.getArgument(1));
        }).when(eventRepository).claimForProcessing(eq(eventId), any(Instant.class));

        send(eventId);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Event stored = eventRepository.findById(eventId).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(EventStatus.PROCESSED);
            assertThat(stored.getAttempts()).isEqualTo(1);
        });
        assertThat(attemptsWhenMongoFailed).hasValue(0);
        verify(eventRepository, times(2)).claimForProcessing(eq(eventId), any(Instant.class));
        verify(eventProcessor, times(1)).process(argThat(event -> event.getEventId().equals(eventId)));
        await().atMost(Duration.ofSeconds(10)).until(() -> mainQueueMessageCount() == 0 && dlqMessageCount() == 0);
    }

    private void send(String eventId) throws Exception {
        adminSqsClient().sendMessage(SendMessageRequest.builder().queueUrl(mainQueueUrl())
                .messageBody(objectMapper.writeValueAsString(Map.of("eventId", eventId))).build());
    }

    private Event received(String eventId) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        return new Event(eventId, "sensitive-customer", EventType.PAYMENT, new BigDecimal("10.00"), "USD",
                now, EventStatus.RECEIVED, now, now, null, 0, null,
                Map.of("private", "private-metadata-value"));
    }

    private Document raw(String eventId) {
        return mongoTemplate.getCollection("events").find(new Document("_id", eventId)).first();
    }

    private void assertValidTypes(Document raw) {
        assertThat(raw.get("attempts")).isInstanceOf(Integer.class);
        assertThat(raw.get("updatedAt")).isInstanceOf(java.util.Date.class);
        assertThat(raw.get("createdAt")).isInstanceOf(java.util.Date.class);
    }
}
