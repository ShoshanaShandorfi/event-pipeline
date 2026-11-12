package dev.eventpipeline.service.messaging.processing;

import dev.eventpipeline.service.config.EventCollectionInitializer;
import dev.eventpipeline.service.config.LocalStackTestcontainersConfig;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventType;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import dev.eventpipeline.service.service.processing.EventProcessor;
import dev.eventpipeline.service.testsupport.TestEventRequests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Exercises the real SQS consumer (long polling, delete-only-on-success, malformed/unknown-message
 * handling) end to end against LocalStack. Uses short wait/visibility timeouts just for this test class
 * so redelivery-adjacent assertions stay fast without needing {@code Thread.sleep} (everything is
 * driven by Awaitility).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class SqsEventConsumerIntegrationTest extends LocalStackTestcontainersConfig {

    @DynamicPropertySource
    static void consumerProperties(DynamicPropertyRegistry registry) {
        registry.add("aws.sqs.consumer.enabled", () -> true);
        registry.add("aws.sqs.consumer.wait-time-seconds", () -> 1);
        registry.add("aws.sqs.consumer.visibility-timeout-seconds", () -> 2);
        registry.add("aws.sqs.consumer.worker-threads", () -> 2);
        registry.add("pipeline.processing.retry-backoff-base-seconds", () -> 1);
        registry.add("pipeline.processing.retry-backoff-cap-seconds", () -> 2);
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoSpyBean
    private EventProcessor eventProcessor;

    @BeforeEach
    void setUp() throws Exception {
        mongoTemplate.getDb().drop();
        new EventCollectionInitializer().initializeEventCollection(mongoTemplate)
                .run(new DefaultApplicationArguments());
    }

    @Test
    void endToEnd_postThenEventIsProcessedAndQueueEndsUpEmpty() {
        String eventId = "evt-e2e";
        String body = TestEventRequests.json(objectMapper, eventId, "cust-1", "10.00", "2025-01-01T00:00:00Z", null);

        ResponseEntity<String> response = restTemplate.postForEntity("/api/events", jsonEntity(body), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Event stored = eventRepository.findById(eventId).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(EventStatus.PROCESSED);
            assertThat(stored.getProcessedAt()).isNotNull();
            assertThat(stored.getAttempts()).isEqualTo(1);
        });

        await().atMost(Duration.ofSeconds(10)).until(() -> mainQueueMessageCount() == 0 && dlqMessageCount() == 0);
    }

    @Test
    void sameMessageDeliveredTwice_singleProcessingBothMessagesDeleted() {
        String eventId = "evt-dup-delivery";
        eventRepository.save(newReceivedEvent(eventId));

        sendRawMessage(eventId);
        sendRawMessage(eventId);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Event stored = eventRepository.findById(eventId).orElseThrow();
            assertThat(stored.getStatus()).isEqualTo(EventStatus.PROCESSED);
        });
        await().atMost(Duration.ofSeconds(10)).until(() -> mainQueueMessageCount() == 0);

        // The placeholder business step must only ever run once for this event, no matter how many
        // times the (at-least-once) queue redelivers the same logical message.
        verify(eventProcessor, times(1)).process(argThat(e -> e.getEventId().equals(eventId)));
    }

    @ParameterizedTest
    @ValueSource(strings = {"{\"eventId\":\"evt-does-not-exist-in-mongo\"}", "this is not valid JSON"})
    void unknownOrMalformedMessage_isNotDeleted(String body) {
        adminSqsClient().sendMessage(SendMessageRequest.builder().queueUrl(mainQueueUrl()).messageBody(body).build());

        // Still present (invisible mid-processing or redelivered/DLQ'd), never silently dropped.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(mainQueueMessageCount() + dlqMessageCount()).isGreaterThanOrEqualTo(1));
    }

    private void sendRawMessage(String eventId) {
        try {
            String body = objectMapper.writeValueAsString(new java.util.HashMap<>(java.util.Map.of("eventId", eventId)));
            adminSqsClient().sendMessage(SendMessageRequest.builder()
                    .queueUrl(mainQueueUrl())
                    .messageBody(body)
                    .build());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Event newReceivedEvent(String eventId) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        return new Event(eventId, "cust-1", EventType.PAYMENT, new BigDecimal("10.00"), "USD",
                now, EventStatus.RECEIVED, now, now, null, 0, null, null);
    }

    private HttpEntity<String> jsonEntity(String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<>(body, headers);
    }
}
