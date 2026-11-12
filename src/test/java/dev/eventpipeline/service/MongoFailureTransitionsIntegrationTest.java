package dev.eventpipeline.service;

import dev.eventpipeline.service.config.EventCollectionInitializer;
import dev.eventpipeline.service.config.MongoTestcontainersConfig;
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
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Fencing and every new state update run against the unchanged strict Mongo validator. */
@DataMongoTest
class MongoFailureTransitionsIntegrationTest extends MongoTestcontainersConfig {

    @Autowired private EventRepository eventRepository;
    @Autowired private MongoTemplate mongoTemplate;

    @BeforeEach
    void initializeValidator() throws Exception {
        mongoTemplate.getDb().drop();
        new EventCollectionInitializer().initializeEventCollection(mongoTemplate)
                .run(new DefaultApplicationArguments());
    }

    @Test
    void supersededWorkerCannotCompleteRetryOrFailTheNewerClaim() {
        Instant old = Instant.now().truncatedTo(ChronoUnit.MILLIS).minusSeconds(600);
        Instant now = old.plusSeconds(600);
        eventRepository.insert(event("fenced-worker", EventStatus.RECEIVED, 0, old));
        Event first = eventRepository.claimForProcessing("fenced-worker", old).orElseThrow();
        assertThat(first.getAttempts()).isEqualTo(1);

        assertThat(eventRepository.recoverStaleProcessing(now.minusSeconds(300), now, 3)).isPresent();
        Event second = eventRepository.claimForProcessing("fenced-worker", now).orElseThrow();
        assertThat(second.getAttempts()).isEqualTo(2);
        Document newerClaim = raw("fenced-worker");

        assertThat(eventRepository.markProcessed("fenced-worker", first.getAttempts(), now.plusSeconds(1))).isEmpty();
        assertThat(eventRepository.retryProcessing("fenced-worker", first.getAttempts(), "stale retry", now.plusSeconds(1))).isEmpty();
        assertThat(eventRepository.markFailed("fenced-worker", first.getAttempts(), "stale failure", now.plusSeconds(1))).isEmpty();
        assertThat(raw("fenced-worker")).isEqualTo(newerClaim);

        assertThat(eventRepository.markProcessed("fenced-worker", second.getAttempts(), now.plusSeconds(2)))
                .hasValueSatisfying(completed -> {
                    assertThat(completed.getStatus()).isEqualTo(EventStatus.PROCESSED);
                    assertThat(completed.getAttempts()).isEqualTo(2);
                });
        assertThat(raw("fenced-worker")).doesNotContainKey("failureReason");
    }

    private void assertTypes(String eventId, boolean hasFailureReason) {
        Document raw = raw(eventId);
        assertThat(raw.get("attempts")).isInstanceOf(Integer.class);
        assertThat(raw.get("createdAt")).isInstanceOf(java.util.Date.class);
        assertThat(raw.get("updatedAt")).isInstanceOf(java.util.Date.class);
        if (hasFailureReason) {
            assertThat(raw.get("failureReason")).isInstanceOf(String.class);
        } else {
            assertThat(raw).doesNotContainKey("failureReason");
        }
    }

    private Document raw(String eventId) {
        return mongoTemplate.getCollection("events").find(new Document("_id", eventId)).first();
    }

    private Event event(String eventId, EventStatus status, int attempts, Instant updatedAt) {
        return new Event(eventId, "cust-validator", EventType.PAYMENT, new BigDecimal("10.00"), "USD",
                updatedAt, status, updatedAt, updatedAt, null, attempts, null, null);
    }
}
