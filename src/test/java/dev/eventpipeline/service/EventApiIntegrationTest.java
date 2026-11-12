package dev.eventpipeline.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.eventpipeline.service.api.exception.ProblemTypes;
import dev.eventpipeline.service.config.EventCollectionInitializer;
import dev.eventpipeline.service.testsupport.IngestionIntegrationTestSupport;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end tests for the event ingestion and query REST API, backed by a real MongoDB instance via
 * Testcontainers (no dependency on the local .env / docker-compose stack). Also starts a real SQS (via
 * LocalStack) since the ingestion service now publishes after every successful insert, but this is an
 * API-only test, so the SQS consumer is switched off via a property -- it does not need, and must not
 * start, a background poller.
 */
class EventApiIntegrationTest extends IngestionIntegrationTestSupport {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() throws Exception {
        mongoTemplate.getDb().drop();
        new EventCollectionInitializer().initializeEventCollection(mongoTemplate)
                .run(new DefaultApplicationArguments());
    }

    private String requestJson(String eventId, String customerId, String amount, String timestamp, Map<String, String> metadata) {
        try {
            var node = objectMapper.createObjectNode();
            node.put("eventId", eventId);
            node.put("customerId", customerId);
            node.put("type", "PAYMENT");
            node.put("amount", amount);
            node.put("currency", "USD");
            node.put("timestamp", timestamp);
            if (metadata != null) {
                var metadataNode = objectMapper.createObjectNode();
                metadata.forEach(metadataNode::put);
                node.set("metadata", metadataNode);
            }
            return objectMapper.writeValueAsString(node);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private org.springframework.http.HttpEntity<String> jsonEntity(String body) {
        var headers = new org.springframework.http.HttpHeaders();
        headers.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);
        return new org.springframework.http.HttpEntity<>(body, headers);
    }

    @Test
    void createThenGetEvent() {
        String body = requestJson("evt-create-get", "cust-1", "10.50", "2025-01-01T00:00:00Z", null);

        ResponseEntity<JsonNode> createResponse = restTemplate.exchange(
                "/api/events", org.springframework.http.HttpMethod.POST, jsonEntity(body), JsonNode.class);

        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(createResponse.getHeaders().getLocation().toString()).isEqualTo("/api/events/evt-create-get");
        assertThat(createResponse.getHeaders().get("Idempotent-Replayed")).isNull();
        assertThat(createResponse.getBody().get("eventId").asText()).isEqualTo("evt-create-get");
        assertThat(createResponse.getBody().get("status").asText()).isEqualTo("RECEIVED");

        ResponseEntity<JsonNode> getResponse = restTemplate.getForEntity("/api/events/evt-create-get", JsonNode.class);
        assertThat(getResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(getResponse.getBody().get("eventId").asText()).isEqualTo("evt-create-get");
        assertThat(getResponse.getBody().get("customerId").asText()).isEqualTo("cust-1");
        assertThat(getResponse.getBody().get("status").asText()).isEqualTo("RECEIVED");
        assertThat(getResponse.getBody().get("processedAt").isNull()).isTrue();
    }

    @Test
    void getEvent_missingReturns404() {
        ResponseEntity<JsonNode> response = restTemplate.getForEntity("/api/events/does-not-exist", JsonNode.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody().get("type").asText()).isEqualTo(ProblemTypes.EVENT_NOT_FOUND.toString());
    }

    @Test
    void unknownRoute_returns404NotServerError() {
        ResponseEntity<JsonNode> response = restTemplate.getForEntity("/api/no-such-route", JsonNode.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void unsupportedMethod_returns405WithAllowHeader() {
        ResponseEntity<JsonNode> response = restTemplate.exchange(
                "/api/events/evt-1", org.springframework.http.HttpMethod.DELETE, null, JsonNode.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
        assertThat(response.getHeaders().getAllow()).isNotEmpty();
    }

    @Test
    void duplicateWithSamePayloadButSubMillisecondTimestampAndTrailingZeroAmountStillReturns202() {
        // Regression test: a sub-millisecond timestamp and a decimal with trailing zeros must still be
        // recognized as the *same* payload after a real Mongo round-trip (Date truncates to millis,
        // Decimal128 preserves scale exactly), not misreported as a 409 conflict.
        String body = requestJson("evt-dup-precision", "cust-1", "12.340000", "2025-01-01T00:00:00.123456789Z", Map.of("k", "v"));

        ResponseEntity<JsonNode> first = restTemplate.exchange(
                "/api/events", org.springframework.http.HttpMethod.POST, jsonEntity(body), JsonNode.class);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(first.getBody().get("duplicate").asBoolean()).isFalse();

        ResponseEntity<JsonNode> second = restTemplate.exchange(
                "/api/events", org.springframework.http.HttpMethod.POST, jsonEntity(body), JsonNode.class);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(second.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(second.getBody().get("duplicate").asBoolean()).isTrue();

        assertThat(eventRepository.count()).isEqualTo(1);
    }

    @Test
    void duplicateWithDifferentPayloadReturns409() {
        String original = requestJson("evt-dup-diff", "cust-1", "20.00", "2025-01-01T00:00:00Z", null);
        String conflicting = requestJson("evt-dup-diff", "cust-1", "99.99", "2025-01-01T00:00:00Z", null);

        ResponseEntity<JsonNode> first = restTemplate.exchange(
                "/api/events", org.springframework.http.HttpMethod.POST, jsonEntity(original), JsonNode.class);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        ResponseEntity<JsonNode> second = restTemplate.exchange(
                "/api/events", org.springframework.http.HttpMethod.POST, jsonEntity(conflicting), JsonNode.class);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(second.getBody().get("type").asText()).isEqualTo(ProblemTypes.EVENT_PAYLOAD_CONFLICT.toString());

        assertThat(eventRepository.count()).isEqualTo(1);
    }

    @Test
    void concurrentDuplicatePostsResultInExactlyOneDocumentAndNoErrorResponses() throws Exception {
        int threadCount = 20;
        String body = requestJson("evt-concurrent", "cust-1", "5.00", "2025-01-01T00:00:00Z", null);

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch readyLatch = new CountDownLatch(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger();

        List<Future<ResponseEntity<JsonNode>>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            futures.add(pool.submit(() -> {
                readyLatch.countDown();
                startLatch.await();
                return restTemplate.exchange("/api/events", org.springframework.http.HttpMethod.POST, jsonEntity(body), JsonNode.class);
            }));
        }

        assertThat(readyLatch.await(10, TimeUnit.SECONDS)).isTrue();
        startLatch.countDown();

        for (Future<ResponseEntity<JsonNode>> future : futures) {
            ResponseEntity<JsonNode> response = future.get(30, TimeUnit.SECONDS);
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            successCount.incrementAndGet();
        }
        pool.shutdown();

        assertThat(successCount.get()).isEqualTo(threadCount);
        assertThat(eventRepository.count()).isEqualTo(1);

        // The consumer and jobs are disabled here. After every asynchronous sender has finished,
        // one persisted event must have produced exactly one notification despite 20 racing POSTs.
        awaitPublisherIdle();
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(mainQueueMessageCount()).isEqualTo(1);
            assertThat(dlqMessageCount()).isZero();
        });
        var messages = adminSqsClient().receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(mainQueueUrl()).maxNumberOfMessages(10).waitTimeSeconds(1).build()).messages();
        assertThat(messages).hasSize(1);
        assertThat(objectMapper.readTree(messages.get(0).body()).get("eventId").asText()).isEqualTo("evt-concurrent");
    }

    @Test
    void customerQueryFiltersPaginatesOrdersAndValidates() {
        Instant base = Instant.parse("2025-01-01T00:00:00Z");
        restTemplate.exchange("/api/events", org.springframework.http.HttpMethod.POST,
                jsonEntity(requestJson("q-e1", "cust-q", "10.00", base.plusSeconds(10).toString(), null)), JsonNode.class);
        restTemplate.exchange("/api/events", org.springframework.http.HttpMethod.POST,
                jsonEntity(requestJson("q-e2", "cust-q", "20.00", base.plusSeconds(20).toString(), null)), JsonNode.class);
        restTemplate.exchange("/api/events", org.springframework.http.HttpMethod.POST,
                jsonEntity(requestJson("q-e3", "cust-q", "30.00", base.plusSeconds(30).toString(), null)), JsonNode.class);
        restTemplate.exchange("/api/events", org.springframework.http.HttpMethod.POST,
                jsonEntity(requestJson("q-other-cust", "cust-other", "40.00", base.plusSeconds(40).toString(), null)), JsonNode.class);

        // Give the query fixture distinct receive times. Fast hosts can assign equal millisecond
        // createdAt values to sequential POSTs, for which the API deliberately has no tie-break sort.
        for (int index = 1; index <= 3; index++) {
            var event = eventRepository.findById("q-e" + index).orElseThrow();
            event.setCreatedAt(base.plusSeconds(index));
            eventRepository.save(event);
        }

        // All three cust-q events are RECEIVED; newest first ordering by server-side createdAt.
        ResponseEntity<JsonNode> page = restTemplate.getForEntity(
                "/api/customers/cust-q/events?status=RECEIVED&page=0&size=2", JsonNode.class);
        assertThat(page.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(page.getBody().get("content")).hasSize(2);
        assertThat(page.getBody().get("totalElements").asLong()).isEqualTo(3);
        assertThat(page.getBody().get("totalPages").asInt()).isEqualTo(2);
        assertThat(page.getBody().get("content").get(0).get("eventId").asText()).isEqualTo("q-e3");
        assertThat(page.getBody().get("content").get(1).get("eventId").asText()).isEqualTo("q-e2");

        ResponseEntity<JsonNode> secondPage = restTemplate.getForEntity(
                "/api/customers/cust-q/events?status=RECEIVED&page=1&size=2", JsonNode.class);
        assertThat(secondPage.getBody().get("content")).hasSize(1);
        assertThat(secondPage.getBody().get("content").get(0).get("eventId").asText()).isEqualTo("q-e1");

        ResponseEntity<JsonNode> otherCustomer = restTemplate.getForEntity(
                "/api/customers/cust-other/events", JsonNode.class);
        assertThat(otherCustomer.getBody().get("content")).hasSize(1);

        ResponseEntity<JsonNode> receiveTimeWindow = restTemplate.getForEntity(
                "/api/customers/cust-q/events?from=2025-01-01T00:00:02Z&to=2025-01-01T00:00:03Z"
                        + "&status=RECEIVED&page=0&size=2", JsonNode.class);
        assertThat(receiveTimeWindow.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(receiveTimeWindow.getBody().get("content")).hasSize(1);
        assertThat(receiveTimeWindow.getBody().get("content").get(0).get("eventId").asText()).isEqualTo("q-e2");
        assertThat(receiveTimeWindow.getBody().get("totalElements").asInt()).isEqualTo(1);

        // 400 cases
        ResponseEntity<JsonNode> invalidStatus = restTemplate.getForEntity("/api/customers/cust-q/events?status=BOGUS", JsonNode.class);
        assertThat(invalidStatus.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(invalidStatus.getBody().get("type").asText()).isEqualTo(ProblemTypes.VALIDATION_FAILED.toString());

        assertThat(restTemplate.getForEntity("/api/customers/cust-q/events?page=-1", JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(restTemplate.getForEntity("/api/customers/cust-q/events?size=0", JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(restTemplate.getForEntity("/api/customers/cust-q/events?size=101", JsonNode.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(restTemplate.getForEntity(
                        "/api/customers/cust-q/events?from=2025-01-01T01:00:00Z&to=2025-01-01T00:00:00Z", JsonNode.class)
                .getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
