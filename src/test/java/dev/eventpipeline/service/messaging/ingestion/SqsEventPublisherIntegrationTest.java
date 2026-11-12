package dev.eventpipeline.service.messaging.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.eventpipeline.service.config.EventCollectionInitializer;
import dev.eventpipeline.service.testsupport.IngestionIntegrationTestSupport;
import dev.eventpipeline.service.testsupport.TestEventRequests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies the publisher's happy-path and duplicate/conflict behaviour against a real SQS (LocalStack).
 * The consumer is switched off so messages accumulate in the queue for these assertions instead of
 * being immediately consumed.
 */
class SqsEventPublisherIntegrationTest extends IngestionIntegrationTestSupport {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() throws Exception {
        mongoTemplate.getDb().drop();
        new EventCollectionInitializer().initializeEventCollection(mongoTemplate)
                .run(new DefaultApplicationArguments());
    }

    @Test
    void publishesExactlyOneMessageWithExpectedBodyAfterSuccessfulPost() {
        String eventId = "evt-pub-one";
        String body = TestEventRequests.json(objectMapper, eventId, "cust-1", "10.00", "2025-01-01T00:00:00Z", null);

        ResponseEntity<String> response = restTemplate.postForEntity("/api/events", jsonEntity(body), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            ReceiveMessageResponse received = adminSqsClient().receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(mainQueueUrl())
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(0)
                    .messageAttributeNames("All")
                    .build());

            assertThat(received.messages()).hasSize(1);
            Message message = received.messages().get(0);
            JsonNode bodyNode = objectMapper.readTree(message.body());
            assertThat(bodyNode.get("eventId").asText()).isEqualTo(eventId);
            // Message body only ever contains the eventId -- Mongo is the source of truth for everything else.
            assertThat(bodyNode.fieldNames()).toIterable().containsExactly("eventId");
            assertThat(message.messageAttributes().get("schemaVersion").stringValue()).isEqualTo("1");
        });
    }

    @Test
    void duplicateAndConflictingPostsNeverPublishASecondMessage() {
        String eventId = "evt-pub-dup";
        String original = TestEventRequests.json(objectMapper, eventId, "cust-1", "10.00", "2025-01-01T00:00:00Z", null);
        String conflicting = TestEventRequests.json(objectMapper, eventId, "cust-1", "99.00", "2025-01-01T00:00:00Z", null);

        ResponseEntity<String> first = restTemplate.postForEntity("/api/events", jsonEntity(original), String.class);
        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        await().atMost(Duration.ofSeconds(5)).until(() -> mainQueueMessageCount() == 1);

        ResponseEntity<String> duplicate = restTemplate.postForEntity("/api/events", jsonEntity(original), String.class);
        assertThat(duplicate.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(duplicate.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");

        ResponseEntity<String> conflict = restTemplate.postForEntity("/api/events", jsonEntity(conflicting), String.class);
        assertThat(conflict.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        // The queue must stay at exactly one message throughout a short window: neither the duplicate
        // replay nor the 409 conflict is ever allowed to trigger a second publish.
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3))
                .until(() -> mainQueueMessageCount() == 1);
    }

    private HttpEntity<String> jsonEntity(String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<>(body, headers);
    }
}
